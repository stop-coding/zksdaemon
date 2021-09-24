# -*- encoding: utf-8 -*-
'''
@File    :   ZksDaemon.py
@Time    :   2021/09/15 15:48:29
@Author  :   hongchunhua
@Contact :   需要依赖python3、kazoo组件：pip3 install kazoo
@License :   (C)Copyright 2020-2025
'''

import re
from kazoo.client import KazooClient
from kazoo.protocol.states import (
    KazooState,
)
from kazoo.exceptions import *
from kazoo.retry import KazooRetry
import time
import sys, getopt
import os
import socket
import threading
import random
import logging

class ZksLogger(logging.Logger):
    def __init__(self, name, level='INFO', file=None, encoding='utf-8'):
        super().__init__(name)
        self.setLevel(level=level)
        stdio = logging.StreamHandler()
        stdio.setLevel(level=level)
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s:%(funcName)s@%(lineno)d] %(message)s")
        stdio.setFormatter(formatter)
        self.addHandler(stdio)

#全局默认日志模块
G_DEFAULT_LOGGER = ZksLogger("zksdaemon")

class ZksRole():
    PARTICIPANT='participant'
    LEADER='leader'
    FOLLOWER='follower'
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
        pass
    return ret
    
class ZksDaemon(object):
    """
    ##说明：
        该类守护一个zookeeper server实例的动态配置
        确保守护的节点能及时加入集群，以及根据策略，
        转变自身角色
    ##使用方法
        直接调用：
        daemon = ZksDaemon(host="10.24.37.1:9639:9639", myid=1)
        daemon.run()
        # 这里会阻塞，直到session过期或者超时自动释放

        使用with:
        with ZksDaemon(host="10.24.37.1:9639:9639", myid=1):
            print("wait %d exit" %(l.id))
    ##注意：
        该类实例的生命周期是zookeeper client session有效为准,
        如果session无效，则该类实例也将无效释放资源，必须重建.
    """
    #participan角色数量最大值
    PARTICIPANT_MAX_NUM = 7

    #内部异常保护重试间隔,单位秒
    DELAY_FOR_RETRY_IF_ERROR= 15

    #内部重试异常保护，避免重试异常
    COUNT_FOR_RETRY_ON_LOOP= 10

    #zookeeper内部配置存储位置
    ZOOKEEPER_CONFIG_PATH = '/zookeeper/config'

    #集群比例格式：[集群总数下界，集群总数上界,参与者数]，如5,100,5表示：集群规模在5-100个节点之间时，则需要5个participant
    ZKS_RULES=('1,1,1', '2,2,2', '3,3,3', '4,4,4', '5,100,5', '101,50000,7')

    def __init__(self, myhost, myid, cid=0, timeout=8.0, check_delay=60*5,
                lockPath='/zookeeper/lock', statusPath='/zookeeper/status',
                election_port=3888, transction_port=2888, logger=None):
        """
        ##参数说明：
            myhost          需要守护的server，其客户端访问地址，如"10.24.37.1:9639".不可以是本地地址
            myid            需要守护的serverid，每个zks都有一个唯一id值
            cid             集群id，用于集群故障隔离，默认为0,可以不填
            timeout         session超时时间
            check_delay     自动检查节点角色状态是否同步的时间间隔，默认为5分钟
            lockPath        内部分布式锁的路径，默认不用改
            statusPath      participant状态存储路径，默认不用改
            election_port   选举端口，与默认不一样的话，得改
            transction_port 内部传输端口，与默认不一样的话，就得改
            logger          日志打印类，默认ZksLogger，自定义的话需要继承基类logging.Logger
        """
        self.alive = False
        self.zkc = None
        self.host = myhost
        self.myid = int(myid)
        self.cid = int(cid)
        self.lock_path=lockPath
        self.timeout = float(timeout)
        self.participant_status_path = statusPath
        if isinstance(logger, logging.Logger):
            self.log = logger
        else:
            self.log = G_DEFAULT_LOGGER
        self.notify = threading.Event()
        #session尝试重连时间间隔,单位秒
        self.max_delay_s = 1
        #session尝试重连最多次数
        self.max_retries = 2*int(timeout)//self.max_delay_s
        self.eport = election_port
        self.tport = transction_port
        #强制唤醒检查节点状态时间间隔
        self.force_check_status_delay_s = check_delay
        self._force_switch_observer =False
        self._session_id = 0
        self._configs =[]
        self._identifiers = []
        self._is_exit = True
        self._myidentifier = str(cid) + '_' + str(myid)
        self._myserver = {}
        self._mynodes = None
        #写互斥保护
        self._wrlock = threading.Lock()
        self._syncLock = threading.Lock()
    
    def run(self):
        try:
            retry_count = 0
            self._connect(self._zks_listener)
            self._init_zks()
            self._is_exit = False
            while(self.alive and self.zkc):
                try:
                    retry_count+=1
                    if retry_count > self.COUNT_FOR_RETRY_ON_LOOP:
                        #处理逻辑一定在有限次数内处理完毕并进入休眠，否则直接抛出异常
                        raise CancelledError('the counts of running have over max limit[%d].' %(self.COUNT_FOR_RETRY_ON_LOOP))
                    role = self._get_myrole()
                    self.log.info("My role:%s, identifier:%s, session:0x%x." %(role, self._myidentifier, self._session_id))
                    is_retry = False
                    nodes = self._get_nodes()
                    if role == ZksRole.LEADER or role == ZksRole.FOLLOWER:
                        is_retry = self._do_participant(role, nodes)
                    elif role == ZksRole.OBSERVER:
                        is_retry = self._do_observer(nodes)
                    else:
                        self.log.error("unkown role err: {}".format(role))
                    if is_retry:
                        continue
                    self.log.info("wait,then suspend time[%ds]." %(self.force_check_status_delay_s))
                    is_timeout = self._wait(self.force_check_status_delay_s)
                    if not is_timeout:
                        self.log.info("Zookeeper node[%s] wake up." %(self._myidentifier))
                    else:
                        self.log.info("Zookeeper node[%s] suspend timeout, check status automatically." %(self._myidentifier))
                        retry_count = 0    
                    #随机延迟一点时间，避免惊群效应。使用信号等待，可立马被唤醒
                    self._wait(0.001*random.randint(10, 1000))
                except (RetryFailedError, SessionExpiredError) as e:
                    #session 失效，直接退出, 确保session销毁
                    if self.alive:
                        self.log.error("reconnection fail, Session Expired err: {}".format(e))
                    raise e
                except (CancelledError) as e:
                    if self.alive:
                        self.log.error("Cancelled err: {}".format(e))
                    raise e
                except Exception as e:
                    self.log.error("loop err: {}".format(e))
                    #其它内部异常时,避免频繁重试，设置间隔时间
                    self._wait(self.DELAY_FOR_RETRY_IF_ERROR)
            self.log.warning("run exit.")
            self._wrlock.acquire(blocking=True)
            self._is_exit = True
            self._wrlock.release()
        except Exception as e:
            if self.alive:
                self.log.error("run err: {}".format(e))
            self.alive = False
            self._wrlock.acquire(blocking=True)
            self._is_exit = True
            self._wrlock.release()
            raise e
        
    def stop(self):
        max_wait_times = 120
        while max_wait_times > 0:
            self._wrlock.acquire(blocking=True)
            if self._is_exit:
                self._wrlock.release()
                break
            try:
                self.alive = False
                if self.notify:
                    self.notify.set()
                if self.zkc:
                    self.zkc.stop()
            except Exception as e:
                self.log.error("Stop daemon err: {}".format(e))
                self.alive = False
            self._wrlock.release()
            self.log.warning("Stop zks daemon ...")
            max_wait_times -=1
            time.sleep(0.5)
        self.log.warning("Stop zks daemon success.")
    
    def set_observer(self):
        self._wrlock.acquire(blocking=True)
        if not self.alive or self._is_exit:
            self._wrlock.release()
            return
        self._force_switch_observer = True
        self.notify.set()
        self._wrlock.release()

    def __enter__(self):
        self.run()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def __del__(self):
        self.stop()
    
    def _wait(self, timeout):
        is_timeout = True
        self.notify.clear()
        is_timeout = (self.notify.wait(timeout) != True)
        if not self.alive:
            raise CancelledError('wake up to exit')
        self.notify.clear()
        return is_timeout

    def _do_participant(self, role, nodes):
        if self._is_participant_surplus(nodes) and role != ZksRole.LEADER:
            self.log.warning("participant overload, mynode[%s] need switch to observer." %(self._myidentifier))
            return self._degrade_observer()
        if self._force_switch_observer:
            self.log.warning("participant[%s] force switch to observer." %(self._myidentifier))
            return self._degrade_observer(force=True)
        # 检查健康状态，避免反复加读锁    
        if self._is_participant_ok(nodes):
            self.log.info("mynode[%s] keep [%s]." %(self._myidentifier, role))
            return False
        lock = self.zkc.ReadLock(self.lock_path, str(self._myidentifier))
        if not lock.acquire(timeout=self.timeout):
            self.log.warning("mynode[%s] can't get lock to be a substitute." %(self._myidentifier))
            return False
        #重新获取一遍状态
        retry=False
        try:
            nodes = self._get_nodes(sync=True)
            my_status_path = os.path.join(self.participant_status_path, self._myidentifier)
            my_status = self.zkc.exists(my_status_path)
            if self._is_remove(nodes):
                #改变自己节点角色只有本节点会执行，不存在并发问题，不用加写锁
                retry=True
                self.log.warning("my node[%s] have removed, participant switch to observer." %(self._myidentifier))
                if my_status:
                    self.zkc.delete(my_status_path)
                self._switch_observer(self._myserver)
            else:
                if my_status:
                    (session_id, _) = self.zkc.client_id
                    if my_status.owner_session_id != session_id:
                        self.zkc.delete(my_status_path)
                        self.zkc.create(my_status_path, ephemeral=True, makepath=True)
                else:
                    #如果节点不存在，但集群还存在自己的配置，则可以重新注册自己的状态,注册本节点状态只有本节点再会执行，不需要写锁
                    self.zkc.create(my_status_path, ephemeral=True, makepath=True)
                self.log.info("my node[%s] register participant success." %(self._myidentifier))
        except (RetryFailedError, SessionExpiredError, CancelledError) as e:
            self.log.error("do participant session err: {}".format(e))
            raise e
        except Exception as e:
            self.log.error("do participant err: {}".format(e))
            retry=True
        finally:
            #如果解锁失败，会抛出session异常，必然会终止session，临时节点也会消失，所以不用担心死锁
            lock.release()
            return retry

    def _do_observer(self, nodes):
        #避免加锁带来开销，这里获取不用分布式锁。如果正好处于临界区，超时或者其它observer节点将会重新替补。
        if self._is_remove(nodes):
            self.log.warning("my node[%s] have removed, it add to cluster again." %(self._myidentifier))
            self._switch_observer(self._myserver)
            return True
        (is_election, _) = self._get_election_status(nodes)
        if is_election:
            return self._elected_participant()
        else:
            self.log.info("mynode[%s] keep observer." %(self._myidentifier))
            return False

    def _switch_observer(self, myserver):
        joins = 'server.%s=%s:%s:%s:observer;0.0.0.0:%s'%(myserver['id'], myserver['ip'], myserver['tport'],myserver['eport'], myserver['cport'])
        self.log.info("switch observer reconfig: add " + joins)
        self.do_reconfig(joining=joins,leaving=None)

    def _is_container(self, myid, nodes):
        for node in nodes:
            if self.myid == node['id']:
                print('no change')
                return True
        return False

    def _zks_listener(self, state):
        if state in KazooState.CONNECTED:
            (self._session_id, _) = self.zkc.client_id
            if not self.alive:
                self.log.info("state: %s, session id: 0x%x" %(str(state), self._session_id))
                self.alive = True
        elif state in KazooState.SUSPENDED:
            self.log.warning("Zookeeper session[0x%x] suspended, state: %s" %(self._session_id, str(state)))
        else:
            if self.alive:
                self.log.error("Zookeeper connection lost: %s" %(str(state)))
            self.alive = False
            self.notify.set()

    def _connect(self, listener):
        self.log.info("Connecting to Zookeeper with host[%s], retry[%d,%d]" %(self.host, self.max_delay_s, self.max_retries))
        try:
            con_retry = KazooRetry(max_delay=self.max_delay_s, max_tries=self.max_retries, ignore_expire=False)
            self.zkc = KazooClient(hosts=self.host, timeout=self.timeout, connection_retry=con_retry, logger=self.log)
            self.zkc.add_listener(listener)
            self.zkc.start(self.timeout*2)
        except Exception as e:
            self.log.error('client connect server=%s, timeout=%s.' %(self.host, str(self.timeout)))
            self.log.error("Cannot connect to Zookeeper: {0}".format(e))
            self.zkc = None
            self.alive = False
            raise e
    
    def _fourcmd(self, cmd):
        return self.zkc.command(cmd.encode())
    
    def _get_configs(self, config_str):
        configs = []
        try:
            for line in config_str.split():
                node={}
                if re.match(r'server\.(.*)=(.*:)?', line) is None:
                    continue
                (serverid, addr_ports) = line.split('=', 1)
                (_, node['id']) = serverid.split('.')
                (host, listenter) = addr_ports.split(';')
                (node['ip'], node['tport'], node['eport'], node['role']) = host.split(':')
                (_, node['cport']) = listenter.split(':')
                node['cid'] = 0
                node['id'] = int(node['id'])
                configs.append(node)
        except Exception as e:
            self.log.error("get configs error: {0}".format(e))
            raise e
        return configs

    def _get_identifiers(self, list_str):
        identifiers = {}
        try:
            for str in list_str:
                (cid, id) = str.split('_', 1)
                identifiers[int(id)] = int(cid)
        except Exception as e:
            self.log.error("get identifiers error: {0}".format(e))
            raise e
        return identifiers

    def _init_zks(self):
        try:
            #本节点信息填写
            self._myserver['id'] = self.myid
            self._myserver['cid'] = self.cid
            (self._myserver['ip'], self._myserver['cport']) = self.host.split(':')
            self._myserver['tport'] = self.tport
            self._myserver['eport'] = self.eport
            self._myserver['role'] = ZksRole.OBSERVER

            if self.zkc.exists(self.participant_status_path) is None:
                self.zkc.create(self.participant_status_path, makepath=True)
            
            if self.zkc.exists(self.lock_path) is None:
                self.zkc.create(self.lock_path, makepath=True)
            
            #改为watch 方式获取集群配置
            @self.zkc.DataWatch(self.ZOOKEEPER_CONFIG_PATH)
            def __data_changed(data, stat):
                if data is None:
                    return
                self._syncLock.acquire(timeout=self.timeout)
                self._configs = self._get_configs(data.decode('UTF-8'))
                self._syncLock.release()
                self.log.debug(self._configs)
                for node in self._configs:
                    if int(node['id']) == int(self.myid):
                        return
                #节点被移除，则立马唤醒自己
                self.notify.set()

            #改为watch 方式获取在线状态
            @self.zkc.ChildrenWatch(self.participant_status_path, send_event=True)
            def __status_change(children, event):
                self._syncLock.acquire(timeout=self.timeout)
                is_notify = (len(self._identifiers) > len(children))
                self._identifiers = self._get_identifiers(children)
                self._syncLock.release()
                if event is None:
                    return
                self.log.debug(self._identifiers)
                if is_notify:
                    self.notify.set()
                elif self.myid not in self._identifiers:
                    self.notify.set()
                else:
                    pass
        except Exception as e:
            self.log.error("Init daemon error: {0}".format(e))
            raise e
    
    def _is_participant_ok(self, nodes={}):
        if self._is_remove(nodes):
            return False
        my_status = self.zkc.exists(os.path.join(self.participant_status_path, self._myidentifier))
        #避免重启，旧的session还未超时，导致其还占着状态值
        if my_status:
            (session_id, _) = self.zkc.client_id
            if my_status.owner_session_id == session_id:
                return True
            else:
                return False
        else:
            return False

    def _get_myrole(self):
        try:
            rsp = self._fourcmd('stat')
            if rsp is None:
                return  ZksRole.UNKNOWN
            if rsp.find('follower') > 0:
                return  ZksRole.FOLLOWER
            elif rsp.find('leader') > 0:
                return  ZksRole.LEADER
            elif rsp.find('observer') > 0:
                return  ZksRole.OBSERVER
            else:
                return  ZksRole.UNKNOWN
        except Exception as e:
            self.log.error("get_role err: {}".format(e))
            raise e
 
    def _get_nodes(self, sync=False):
        self._syncLock.acquire(timeout=self.timeout)
        try:
            if sync:
                rsp,_ = self.zkc.get(self.ZOOKEEPER_CONFIG_PATH)
                self._configs = self._get_configs(rsp.decode('UTF-8'))
                self.log.debug("configs", self._configs)
                status = self.zkc.get_children(self.participant_status_path)
                self._identifiers = self._get_identifiers(status)
                self.log.debug("identifiers ", self._identifiers)
        except Exception as e:
            self.log.error("get myserver err: {}".format(e))
            self._syncLock.release()
            raise e
        self._syncLock.release()
        nodes = []
        for node in self._configs:
            if node['id'] in self._identifiers:
                node['cid'] = self._identifiers[node['id']]
            nodes.append(node)
        return nodes

    def _get_alive_num(self, nodes=[]):
        participant=0
        observers=0
        for node in nodes:
            if node['role'] == ZksRole.PARTICIPANT:
                if self.is_zks_alive(node['ip'] + ':' + node['cport']):
                    participant+=1
            else:
                if self.is_zks_alive(node['ip'] + ':' + node['cport']):
                    observers+=1
        return(len(nodes), participant, observers)
    
    def _classify_nodes(self, nodes=[]):
        total = []
        participant=[]
        observers=[]
        offline = []
        for node in nodes:
            total.append(node)
            if self.is_zks_alive(node['ip'] + ':' + node['cport']):
                if node['role'] == ZksRole.PARTICIPANT:
                    participant.append(node)
                else:
                    observers.append(node)
            else:
                offline.append(node)
        return(total, participant, observers, offline)
    
    def _get_min_participant_num(self, total):
        expect_participant_num = self.PARTICIPANT_MAX_NUM
        for rule in self.ZKS_RULES:
            (start, end, num) = rule.split(',')
            if total  <= int(end) and total  >= int(start):
                expect_participant_num = int(num)
                break
        return expect_participant_num
    
    def _is_participant_surplus(self, nodes=[]):
        (total, participant, observers) = self._get_alive_num(nodes)
        min_num = self._get_min_participant_num(total)
        if min_num < participant:
            return True
        else:
            return False
    
    '''
    param: nodes
    return: (is_election, be_replace_node)
    '''
    def _get_election_status(self, nodes=[]):
        (total, participant, observers, offline_nodes) = self._classify_nodes(nodes)
        min_num = self._get_min_participant_num(len(total))
        self.log.info("total:%d, participant:%d, observer:%d, expact participant:%d" 
                    %(len(total), len(participant), len(observers), min_num))
    
        absent_participants = None
        for node in offline_nodes:
            if node['role'] == ZksRole.PARTICIPANT:
                absent_participants = node
                self.log.warning('mynode[%s] will replace bad node[%s] to be new participant.' %(self.myid, node['id']))
                break
        if len(participant) < min_num:
            return (True, absent_participants)

        #检查，是否同一个故障区域内存在多个participant
        counts = {}
        for node in participant:
            if int(self.cid) == int(node['cid']) or int(node['cid']) == 0:
                continue
            if node['cid'] in counts:
                self.log.warning('mynode[%s] will replace node[%s] to be new participant.' %(self.myid, node['id']))
                return (True, node)
            else:
                counts[node['cid']] = 1
        return (False, None)
    
    def is_zks_alive(self, client):
        return (zksCmd(client, 'ruok') == "imok")

    def _is_remove(self, nodes):
        for node in nodes:
            if int(self.myid) == int(node['id']):
                return False
        return True
    
    def do_reconfig(self, joining, leaving):
        retry = False
        try:
            max_wait_time = self.timeout
            #重试间隔时间，单位秒
            wait_interval = 0.5
            while not self.zkc.connected:
                self.log.warning("session is suspended, wait ...")
                self._wait(wait_interval)
                max_wait_time -= wait_interval
                if max_wait_time <= 0:
                    self.log.error(" session can't reconnected.")
                    raise ZookeeperError
            self.zkc.reconfig(joining=joining, leaving=leaving, new_members=None)
        except NewConfigNoQuorumError as e:
            self.log.error("NewConfigNoQuorumError: {}".format(e))
        except BadVersionError as e:
            self.log.error(" bad version: {}".format(e))
        except BadArgumentsError as e:
            self.log.error(" bad arguments: {}".format(e))
        except ZookeeperError as e:
            retry = True
        except Exception as e:
            self.log.error(" unknown error: {}".format(e))
            raise e
        finally:
            return retry
    
    def _elected_participant(self):
        #随机延迟[10, 1000]，毫秒为单位
        self._wait(0.001*random.randint(10, 1000))
        lock = self.zkc.WriteLock(self.lock_path, str(self.myid))
        if not lock.acquire(timeout=self.timeout):
            self.log.warning("mynode[%d] can't get lock to be a substitute." %(self.myid))
            return False
        nodes = self._get_nodes(sync=True)
        (is_election, to_remove_node) = self._get_election_status(nodes)
        if not is_election:
            self.log.warning('no absent participant now.')
            lock.release()
            return False
        self.log.info("mynode[%d] try to elected participant." %(self.myid))
        my_status_path = os.path.join(self.participant_status_path, self._myidentifier)
        is_retry = False
        try:
            my_status = self.zkc.exists(my_status_path)
            if my_status is not None:
                (session_id, _) = self.zkc.client_id
                if my_status.owner_session_id != session_id:
                    self.zkc.delete(my_status_path)
                    self.zkc.create(my_status_path, ephemeral=True, makepath=True)
            else:
                self.zkc.create(my_status_path, ephemeral=True, makepath=True)
            myserver = self._myserver
            joins = 'server.%s=%s:%s:%s:participant;0.0.0.0:%s'%(myserver['id'], myserver['ip'], myserver['tport'],myserver['eport'], myserver['cport'])
            if to_remove_node:
                self.log.info("reconfig: add " + joins+" remove " + str(to_remove_node['id']))
                is_retry = self.do_reconfig(joining=joins, leaving=str(to_remove_node['id']))
            else:
                is_retry = True
                #移除节点后悔短暂的重连，需要做延迟保护
                wait_after_do_remove_s = 3
		        #没有可替换的participant时，需要先删除自身节点,在添加为participant
                self.log.info("reconfig: remove " + str(self.myid))
                self.do_reconfig(joining=None, leaving=str(self.myid))
                #如果这里异常，则该节点会变成影子节点，为了保证原子性，需要立马重试。
                time.sleep(wait_after_do_remove_s)
                self.log.info("reconfig: add " + joins)
                is_retry = self.do_reconfig(joining=joins, leaving=None)
            self.log.info("myid[%s] elected participant success!" %(self.myid))
        except (RetryFailedError, SessionExpiredError, CancelledError) as e:
            #session 失效，直接退出, 确保session销毁
            raise e
        except Exception as e:
            self.log.error("elected participant err: {}".format(e))
            if self._get_myrole != ZksRole.PARTICIPANT and self.zkc.exists(my_status_path):
                self.zkc.delete(my_status_path)
            is_retry = True
        finally:
            #如果解锁失败，会抛出session异常，必然会终止session，临时节点也会消失，所以不用担心死锁
            lock.release()
            return is_retry

    def _degrade_observer(self, force=False):
        lock = self.zkc.WriteLock(self.lock_path, str(self.myid))
        if not lock.acquire(timeout=self.timeout):
            self.log.warning("mynode[%s] can't get lock to be a substitute." %(self.myid))
            return False
        if not self._is_participant_surplus() and force == False:
            self.log.warning('participant do not need degrade to observer now.')
            lock.release()
            return True
        self.log.info("myid[%s] try to degrade observer." %(self.myid))
        is_retry = False
        try:
            if self.zkc.exists(os.path.join(self.participant_status_path, self.myid)):
                self.zkc.delete(os.path.join(self.participant_status_path, self.myid))
            self._switch_observer(self._myserver)
            self.log.info("myid[%s] degrade observer success!" %(self.myid))
        except (RetryFailedError, SessionExpiredError, CancelledError) as e:
            #session 失效，直接退出, 确保session销毁
            raise e
        except Exception as e:
            self.log.error("degrade participant err: {}".format(e))
            is_retry = True
        finally:
            #如果解锁失败，会抛出session异常，必然会终止session，临时节点也会消失，所以不用担心死锁
            lock.release()
            return is_retry

class ZksLoop(object):
    """
    ##说明：
        该类用于重建ZksDaemon实例，使其保持一直运行状态，直至退出
    ##使用方法
        直接调用：
        l = ZksLoop(ZksDaemon(myhost="10.24.37.1:9639:9639", myid=1, logger=G_DEFAULT_LOGGER), retry_delay=15)
        l.run()
        # 这里会阻塞，直到session过期或者超时自动释放

        使用with:
        with ZksLoop(ZksDaemon(myhost="10.24.37.1:9639:9639", myid=1), 15):
            print("wait %d exit" %(l.id))
    """
    def __init__(self, daemon, retry_delay=15, logger=None):
        """
        ##参数说明：
            daemon          ZksDaemon实例
            retry_delay     尝试重建ZksDaemon的时间间隔
            logger          日志打印，默认使用ZksDaemon实例的ZksLogger，自定义需使用或继承logging.Logger基类
        """
        if type(daemon) is ZksDaemon:
            self._daemon = daemon
        else:
            raise ValueError('daemon is not an instance of class ZksDaemon.')
        if isinstance(logger, logging.Logger):
            self.logger = logger
        elif type(self._daemon.log) is ZksLoop:
            self.logger = self.zks.log
        else:
            self.logger = G_DEFAULT_LOGGER
        self._is_runing = False
        self.retry_delay = retry_delay
        self._sig = threading.Event()
    
    def run(self):
        self.is_runing = True
        while(self.is_runing):
            try:
                with self._daemon:
                    self.logger.info("daemon finished.")
            except Exception as e:
                if self.is_runing:
                    self.logger.error("daemon error:{}.".format(e))
            if self.is_runing:
                self.logger.info("zks daemon exit, sleep time [%ds], then it will rebuild." %(self.retry_delay))
                self._sig.wait(self.retry_delay)
        else:
            self.logger.warning("zks loop exit.")

    def stop(self):
        self.is_runing = False
        self.logger.warning("Stop zks loop ...")
        if self._daemon:
            self._daemon.stop()
            self._sig.set()
        self.logger.warning("Stop zks loop success.")

    def __enter__(self):
        self.run()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

class ZksThread(threading.Thread):
    """
    ##说明：
        该类用于ZksLoop线程化执行,可以异步的操作ZksLoop
    ##使用方法
        mythread = ZksThread("zks-daemon", 
                            ZksLoop(daemon=ZksDaemon(myhost="10.24.37.11:9639", myid=1, logger=G_DEFAULT_LOGGER), retry_delay=10))
        mythread.start()
        #这里可以异步执行其它操作
        ...
        mythread.stop()
        #退出前回收线程
        mythread.join()
        # 这里会阻塞，直到session过期或者超时自动释放
    """
    def __init__(self, name, loop, logger=None):
        """
        ##参数说明：
            thread_name     线程名称
            loop            ZksLoop类的实例
            logger          日志打印，默认使用ZksLoop实例的ZksLogger，自定义需继承logging.Logger基类
        """
        super(ZksThread, self).__init__(name=name)
        self.name = name
        if type(loop) is ZksLoop:
            self.zks= loop
        else:
            raise ValueError("loop is not an instance of class ZksLoop.")
        if isinstance(logger, logging.Logger):
            self.logger = logger
        elif type(self.zks.logger) is ZksLoop:
            self.logger = self.zks.logger
        else:
            self.logger = G_DEFAULT_LOGGER
    def run(self): 
        self.logger.info("Starting thread: " + self.name)                  
        if self.zks is None:
            self.logger.error("ZksLoop is None")
            return
        self.zks.run()
    
    def stop(self):
        self.logger.info("Stopping thread: " + self.name)                    
        if self.zks is None:
            self.logger.error("ZksLoop is None")
            return
        self.zks.stop()

"""
这里是测试demo，使用命令测试：
 python3 zks_daemon.py -i 1 -h "172.22.2.3:9639"
"""
def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], "h:i:c:", ["host=",'id=', 'cid='])
        myid = 0
        myhost = None
        cid = 0
        if len(opts) == 0 and len(args):
            raise ValueError("input invalid")
        for cmd, val in opts:
            if cmd in ('-h', '--host'):
                myhost = val
                continue
            elif cmd in ('-i', '--id'):
                myid = val
                continue
            elif cid in ('-c', '--cid'):
                cid = val
                continue
            else:
                raise ValueError("parameter invalid: %s,%s" %(cmd, val))
        if myid is None:
            raise ValueError("myid is None")
        if myhost is None:
            raise ValueError("my host is None")
        '''
        mythread = ZksThread("zks-daemon", ZksLoop(ZksDaemon(myhost, myid, logger=G_DEFAULT_LOGGER), 10))
        mythread.start()
        time.sleep(15)
        mythread.stop()
        mythread.join()
        with ZksLoop(ZksDaemon(myhost=myhost, myid=myid), 10):
            print("wait exit ok.")
        '''
        with ZksLoop(ZksDaemon(myhost, myid, cid=cid, logger=G_DEFAULT_LOGGER), 10):
            print("wait exit ok.")
    except Exception as e:
        print("main: {}".format(e))
        print("cmd: -i [%s]  -h [%s]." %(myid, myhost))
        raise e

if __name__ == "__main__":
    main(sys.argv)
            
