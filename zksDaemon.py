# -*- encoding: utf-8 -*-
'''
@File    :   zksDaemon.py
@Time    :   2021/09/15 15:48:29
@Author  :   hongchunhua
@Contact :   需要依赖python3、kazoo组件：pip3 install kazoo
@License :   (C)Copyright 2020-2025
'''

import re
from kazoo.client import KazooClient
from kazoo.protocol.states import (
    KazooState,
    EventType,
)
from kazoo.recipe.lock import (
    WriteLock,
    ReadLock,
)
from kazoo.exceptions import *
from kazoo.retry import *
import datetime
import time
import sys, getopt
import os
import socket
import threading

class zkLogger(object):
    def error(self, log):
        print("ERROR : ", log)
    def info(self, log):
        print("INFO : ", log)
    def WARN(self, log):
        print("WARN : ", log)
    def show(self, log):
        print(log)
    def show_r(self, log):
        print(log, end='', flush=True)

class zksRole():
    PARTICIPANT='participant'
    OBSERVER='observer'
    UNKNOWN='unknown'

def zksCmd(host, cmd='ruok', timeout=1):
    MAX_REV_BUFFER_LEN = 2048
    ret = ''
    cmds=['ruok','conf', 'envi', 'stat', 'cons']
    if cmd not in cmds:
        return ret
    try:
        conn = socket.socket()
        conn.settimeout(timeout)
        (host, port) = host.split(':')
        conn.connect((host, int(port)))
        conn.send(cmd.encode())
        rsp = conn.recv(MAX_REV_BUFFER_LEN)
        conn.close()
        ret = str(rsp, encoding = "utf-8")
    except Exception as e:
        print("cmd err: {}".format(e))
    return ret
    
class zksDaemon(object):
    """
        常量
    """
    #participan角色数量最大值
    PARTICIPANT_MAX_NUM = 7

    #加锁延迟常量
    DELAY_FOR_ELECTION = 10

    #内部异常保护重试间隔
    DELAY_FOR_RETRY_IF_ERROR= 10

    #集群比例格式：[集群总数下界，集群总数上界,参与者数]，如4,15,3表示：集群规模在4-15个节点之间时，则需要3个particpant
    ZKS_RULES=('1,1,1', '2,2,2', '3,3,3', '4,4,4', '5,100,5', '101,50000,7')

    def __init__(self, myhost, myid, timeout=4.0, check_delay=60*5,
                lockPath='/zks/lock', statusPath='/pos/participant_status',
                election_port=3888, transction_port=2888, logger=None):
        self.alive = False
        self.zkc = None
        self.host = myhost
        self.myid = myid
        self.lock_path=lockPath
        self.timeout = timeout
        self.participant_status_path = statusPath
        if type(logger) is zkLogger:
            self.log = logger
        else:
            self.log = zkLogger()
        self.notify = threading.Event()
        self.hosts = {myid:myhost}
        #session尝试重连时间间隔,单位秒
        self.max_delay_s = 1
        #session尝试重连最多次数
        self.max_retries = timeout//self.max_delay_s
        self.eport = election_port
        self.tport = transction_port
        #强制唤醒检查节点状态时间间隔
        self.force_check_status_delay_s = check_delay

    def __del__(self):
        if self.zkc:
            self.zkc.close()
            self.zkc = None

    def run(self):
        try:
            self._connect(self._zks_listener)
            self._init_zks()
            while(self.alive):
                try:
                    role = self._get_myrole()
                    self.log.info("Get role: %s" %(role))
                    is_retry = False
                    if role == zksRole.PARTICIPANT:
                        is_retry = self._do_participant()
                    elif role == zksRole.OBSERVER:
                        is_retry = self._do_observer()
                    else:
                        self.log.error("unkown role err: {}".format(role))
                    if is_retry:
                        continue
                    self._wait_wakeup(timeout=self.force_check_status_delay_s, role=role)
                except (RetryFailedError, SessionExpiredError) as e:
                    #session 失效，直接退出
                    self.log.error("reconnection fail, Session Expired err: {}".format(e))
                    raise e
                except Exception as e:
                    self.log.error("loop err: {}".format(e))
                    #异常时,避免频繁重试，设置间隔时间
                    time.sleep(self.DELAY_FOR_RETRY_IF_ERROR)
            self.log.WARN("run exit.")
        except Exception as e:
            self.log.error("run err: {}".format(e))
            self.alive = False
            raise e
        
    def stop(self):
        try:
            self.alive = False
            if not self.notify.is_set():
                self.notify.set()
            
            if self.zkc:
                self.zkc.close()
        except Exception as e:
            self.log.error("stop err: {}".format(e))
    
    def _do_participant(self):
        if self._is_participant_surplus() and not self._is_leader():
            self.log.WARN("participant overload, my node[%s] need switch to observer." %(self.myid))
            return self._degrade_observer()
        
        lock = self.zkc.WriteLock(self.lock_path, str(self.myid))
        if not lock.acquire(timeout=self.timeout):
            self.log.WARN("mynode[%s] can't get lock to be a substitute." %(self.myid))
            return False
        retry=False
        try:
            if self._is_remove():
                #改变自己节点角色只有本节点会执行，不存在并发问题，不用加写锁
                self.log.WARN("my node[%s] have removed, participant switch to observer." %(self.myid))
                self._switch_observer()
                retry=True
            else:
                my_status_path = os.path.join(self.participant_status_path, self.myid)
                my_status = self.zkc.exists(my_status_path)
                #避免重启，旧的session还未超时，导致其还占着状态值
                if my_status is not None:
                    (session_id, session_pwd) = self.zkc.client_id
                    if my_status.owner_session_id != session_id:
                        self.zkc.delete(my_status_path)
                        self.zkc.create(my_status_path, ephemeral=True, makepath=True)
                else:
                    #如果节点不存在，但集群还存在自己的配置，则可以重新注册自己的状态,注册本节点状态只有本节点再会执行，不需要写锁
                    self.zkc.create(my_status_path, ephemeral=True, makepath=True)
        except Exception as e:
            self.log.error("do participant err: {}".format(e))
            retry=True
        finally:
            lock.release()
            return retry
                
    def _do_observer(self):
        #避免加锁带来开销，这里获取不用分布式锁。如果正好处于临界区，超时或者其它observer节点将会重新替补。
        if self._is_remove():
            self.log.WARN("my node[%s] have removed, it add to cluster again." %(self.myid))
            self._switch_observer()
            return True
        
        if self._get_absent_participant_num() > 0:
            return self._elected_participant()
        else:
            self.log.info("mynode[%s] keep observer." %(self.myid))
            return False

    def _wait_wakeup(self, timeout, role):
        nodes = self._get_participant_status()
        while(self.alive):
            oldtime=datetime.datetime.now()
            @self.zkc.ChildrenWatch(self.participant_status_path, send_event=True)
            def __status_change(children, event):
                if event is None:
                    return None
                self.notify.set()
                return False
            self.notify.wait(timeout)
            nowtime=datetime.datetime.now()
            self.notify.clear()
            
            if (nowtime -oldtime).seconds >= timeout:
                break
            else:
                timeout -=((nowtime -oldtime).seconds)
            
            childs = self._get_participant_status()
            if len(nodes) <= len(childs):
                continue
            break
        self.log.info("zks [%s] wake up to update role, current role: %s" %(self.myid, role))
        time.sleep(self.DELAY_FOR_ELECTION * int(self.myid)%3)

    def _switch_observer(self):
        try:
            myserver = self._get_myserver()
            joins = 'server.%s=%s:%s:%s:observer;0.0.0.0:%s'%(myserver['id'], myserver['host'], myserver['tport'],myserver['eport'], myserver['cport'])
            self.log.info("switch observer reconfig: add " + joins)
            self.do_reconfig(joining=joins,leaving=None)
        except Exception as e:
            self.log.error("switch observer fail: {}".format(e))
        
        
    def _zks_listener(self, state):
        if state in KazooState.CONNECTED:
            self.log.info("Zookeeper connection established, state: %s" %(str(state)))
            self.alive = True
        elif state in KazooState.SUSPENDED:
            self.log.info("Zookeeper session suspended, state: %s" %(str(state)))
        else:
            self.log.error("Zookeeper connection lost: %s" %(str(state)))
            self.stop()


    def _connect(self, listener):
        self.log.info("Connecting to Zookeeper with host[%s], retry[%d,%d]" %(self.host, self.max_delay_s, self.max_retries))
        try:
            con_retry = KazooRetry(max_delay=self.max_delay_s, max_tries=self.max_retries)
            self.zkc = KazooClient(hosts=self.host, timeout=self.timeout, connection_retry=con_retry)
            self.zkc.add_listener(listener)
            self.zkc.start(self.timeout)
        except Exception as e:
            self.log.error('client connect server=%s, timeout=%s' %(self.host, str(self.timeout)))
            self.log.error("Cannot connect to Zookeeper: {0}".format(e))
            self.zkc = None
            raise e
    
    def _fourcmd(self, cmd):
        cmds = ('stat', 'ruok', 'conf')
        try:
            if cmd in cmds:
                return self.zkc.command(cmd.encode())
            else:
                self.log.error("invalid cmd[{}]".format(cmd))
                return None
        except Exception as e:
            self.log.error("Zookeeper cmd[{}] err: {}".format(cmd,e))
            return None
    
    def _init_zks(self):
        try:
            if self.zkc.exists(self.participant_status_path) is None:
                self.zkc.create(self.participant_status_path, makepath=True)
        except Exception as e:
            raise e

    def _get_myrole(self):
        try:
            rsp = self._fourcmd('stat')
            if rsp is None:
                return  zksRole.UNKNOWN
            if rsp.find('follower') > 0 or rsp.find('leader') > 0:
                return  zksRole.PARTICIPANT
            elif rsp.find('observer') > 0:
                return  zksRole.OBSERVER
            else:
                return  zksRole.UNKNOWN
        except Exception as e:
            self.log.error("get_role err: {}".format(e))
            return zksRole.UNKNOWN

    def _is_leader(self):
        try:
            rsp = self._fourcmd('stat')
            if rsp.find('leader') > 0:
                return  True
            else:
                return  False
        except Exception as e:
            self.log.error("check is leader err: {}".format(e))
            return False
        
    def _get_nodes(self):
        nodes={}
        try:
            """
            node={
                'id':"myid"
                'host':"每个host地址"
                'cport':"client端口"
                'role':"角色"
                'eport':"选举端口，投票专用，3888"
                'tport':"内部通讯端口，zks之间传输数据，2888"
            }
            """
            rsp = self._fourcmd('conf')
            for line in rsp.split():
                node={}
                if re.match(r'server\.(.*)=(.*:)?', line) is None:
                    continue
                (serverid, addr_ports) = line.split('=', 1)
                (discard, node['id']) = serverid.split('.')
                (host, listenter) = addr_ports.split(';')
                (node['host'], node['tport'], node['eport'], node['role']) = host.split(':')
                (discard, node['cport']) = listenter.split(':')
                nodes[node['id']] = node
                if node['id'] not in self.hosts:
                    self.hosts[node['id']] = node['host'] +':' +node['cport']
        except Exception as e:
            self.log.error("get nodes err: {}".format(e))
        finally:
            return nodes
    
    def _get_myserver(self):
        try:
            nodes = self._get_nodes()
            if self.myid in nodes:
                return nodes[self.myid]
            else:
                #若无法从集群里获取本节点配置信息，但又能连接到集群时，需要利用入参获取节点参数
                node ={}
                node['id'] = self.myid
                (node['host'], node['cport']) = self.host.split(':')
                node['tport'] = self.tport
                node['eport'] = self.eport
                node['role'] = zksRole.OBSERVER
                return node
        except Exception as e:
            self.log.error("get myserver err: {}".format(e))
            raise e

    def _get_participant_status(self):
        try:
            return self.zkc.get_children(self.participant_status_path)
        except Exception as e:
            self.log.error("get participant status err: {}".format(e))
            raise e
    
    def _get_alive_num(self):
        nodes = self._get_nodes()
        participant=0
        observers=0
        for node in nodes.values():
            if node['role'] == zksRole.PARTICIPANT:
                if self.is_zks_alive(node['host'] + ':' + node['cport']):
                    participant+=1
            else:
                if self.is_zks_alive(node['host'] + ':' + node['cport']):
                    observers+=1
        return(len(nodes), participant,observers)
    
    def _get_min_participant_num(self, total):
        expect_participant_num = self.PARTICIPANT_MAX_NUM
        for rule in self.ZKS_RULES:
            (start, end, num) = rule.split(',')
            if total  <= int(end) and total  >= int(start):
                expect_participant_num = int(num)
                break
        return expect_participant_num
    
    def _is_participant_surplus(self):
        (total, participant, observers) = self._get_alive_num()
        min_num = self._get_min_participant_num(total)
        if min_num < participant:
            return True
        else:
            return False

    def _get_absent_participant_num(self):
        (total, participant, observers) = self._get_alive_num()
        #集群默认最大participant数
        min_num = self._get_min_participant_num(total)
        self.log.info("myid[%s], total:%d, participant:%d, observer:%d, expact participant:%d" 
                    %(self.myid, total, participant, observers, min_num))
        if participant < min_num:
            return (min_num - participant)
        return 0

    def is_zks_alive(self, client='127.0.0.1:9639'):
        status = False
        try:
            rsp = zksCmd(client, 'ruok')
            if rsp == 'imok':
                status = True
        except Exception as e:
            self.log.error("socket err: {}".format(e))
        return status

    def _get_offline_participant(self):
        nodes = self._get_nodes()
        absent_participants={}
        for id, node in nodes.items():
            print(id, node)
            if node['role'] != zksRole.PARTICIPANT:
                continue
            if not self.is_zks_alive(node['host'] + ':' + node['cport']):
                absent_participants[id] = node
        return absent_participants
    
    def _is_remove(self):
        if self.myid in self._get_nodes():
            return False
        else:
            return True
    def do_reconfig(self, joining, leaving):
        retry = False
        try:
            max_wait_time = self.timeout
            #重试间隔时间，单位秒
            wait_interval = 0.5
            while not self.zkc.connected:
                self.log.WARN("session is suspended, wait ...")
                time.sleep(wait_interval)
                max_wait_time -= wait_interval
                if max_wait_time <= 0:
                    self.log.error(" session can't reconnected.")
                    raise ZookeeperError
            data, _ = self.zkc.reconfig(joining=joining, leaving=leaving, new_members=None)
            return retry
        except NewConfigNoQuorumError as e:
            self.log.error("NewConfigNoQuorumError: {}".format(e))
        except BadVersionError as e:
            self.log.error(" bad version: {}".format(e))
        except BadArgumentsError as e:
            self.log.error(" bad arguments: {}".format(e))
        except ZookeeperError as e:
            self.log.error(" zookeeper error: {}".format(e))
            retry = True
        except Exception as e:
            self.log.error(" unknown error: {}".format(e))
        finally:
            return retry
        
    def _elected_participant(self):
        #随机延迟，毫秒为单位
        delay = (self.DELAY_FOR_ELECTION * int(self.myid))*0.001
        time.sleep(delay)
        lock = self.zkc.WriteLock(self.lock_path, str(self.myid))
        self.log.info("myid[%s] lock path[%s]." %(self.myid, self.lock_path))
        if not lock.acquire(timeout=self.timeout):
            self.log.WARN("mynode[%s] can't get lock to be a substitute." %(self.myid))
            return False
        if self._get_absent_participant_num() == 0:
            self.log.WARN('no absent participant now.')
            lock.release()
            return False
        self.log.info("myid[%s] try to elected participant." %(self.myid))
        my_status_path = os.path.join(self.participant_status_path, self.myid)
        is_retry = False
        try:
            my_status = self.zkc.exists(my_status_path)
            if my_status is not None:
                (session_id, session_pwd) = self.zkc.client_id
                if my_status.owner_session_id != session_id:
                    self.zkc.delete(my_status_path)
                    self.zkc.create(my_status_path, ephemeral=True, makepath=True)
            else:
                self.zkc.create(my_status_path, ephemeral=True, makepath=True)
            offline_node = self._get_offline_participant()
            myserver = self._get_myserver()
            joins = 'server.%s=%s:%s:%s:participant;0.0.0.0:%s'%(myserver['id'], myserver['host'], myserver['tport'],myserver['eport'], myserver['cport'])
            if len(offline_node):
                (to_be_remove_id, nodes)= offline_node.popitem()
                self.log.info("reconfig: add " + joins+" remove " + to_be_remove_id)
                is_retry = self.do_reconfig(joining=joins, leaving=to_be_remove_id)
                if self.zkc.exists(os.path.join(self.participant_status_path, to_be_remove_id)):
                    self.zkc.delete(os.path.join(self.participant_status_path, to_be_remove_id))
            else:
                #移除节点后悔短暂的重连，需要做延迟保护
                wait_after_do_remove_s = 3
		        #没有可替换的participant时，需要先删除自身节点
                self.log.info("reconfig: remove " + self.myid)
                self.do_reconfig(joining=None, leaving=self.myid)
                is_retry = True
                #如果这里异常，则该节点会变成影子节点，为了保证原子性，需要立马重试。
                time.sleep(wait_after_do_remove_s)
                self.log.info("reconfig: add " + joins)
                is_retry = self.do_reconfig(joining=joins, leaving=None)
            self.log.info("myid[%s] elected participant success!" %(self.myid))
        except Exception as e:
            self.log.error("elected participant err: {}".format(e))
            if self._get_myrole != zksRole.PARTICIPANT and self.zkc.exists(my_status_path):
                self.zkc.delete(my_status_path)
            is_retry = True
        finally:
            lock.release()
            return is_retry

    def _degrade_observer(self):
        lock = self.zkc.WriteLock(self.lock_path, str(self.myid))
        self.log.info("myid[%s] lock path[%s]." %(self.myid, self.lock_path))
        if not lock.acquire(timeout=self.timeout):
            self.log.WARN("mynode[%s] can't get lock to be a substitute." %(self.myid))
            return False
        if not self._is_participant_surplus():
            self.log.WARN('participant can not degrade to observer now.')
            lock.release()
            return True
        self.log.info("myid[%s] try to degrade observer." %(self.myid))
        is_retry = False
        try:
            self._switch_observer()
            self.log.info("myid[%s] degrade observer success!" %(self.myid))
        except Exception as e:
            self.log.error("degrade participant err: {}".format(e))
            is_retry = True
        finally:
            lock.release()
            return is_retry

def daemon(myid, host):
    delay_s = 15
    timeout_s = 8
    hosts={myid:host}
    cur_host=host
    zks =None
    while(True):  
        try:
            zks = zksDaemon(myid=myid, myhost=cur_host, timeout=timeout_s)
            zks.run()
            zks.stop()
        except Exception as e:
            print("daemon err: {}".format(e))
        finally:
            if zks:
                for id,addr in zks.hosts.items():
                    if id in hosts:
                        continue
                    hosts[id] = addr
                zks =None
            print("cluster hosts:", hosts)
            print("zks daemon exit, sleep time %d, will retry." %(delay_s))
            time.sleep(delay_s)
   
def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], "h:i:", ["host=",'id='])
        myid=None
        myhost=None
        if len(opts) == 0 and len(args):
            raise ValueError("input invalid")
        for cmd, val in opts:
            if cmd in ('-h', '--host'):
                myhost = val
                continue
            elif cmd in ('-i', '--id'):
                myid = val
                continue
            else:
                raise ValueError("parameter invalid: %s,%s" %(cmd, val))
        if myid is None:
            raise ValueError("myid is None")
        if myhost is None:
            raise ValueError("my host is None")
        daemon(myid, myhost)
    except Exception as e:
        print("main: {}".format(e))
        print("cmd: -i [%s]  -h [%s]." %(myid, myhost))
        raise e

if __name__ == "__main__":
    main(sys.argv)
            
