#!/usr/bin/env bash

#set -ex

WORK_DIR=$1
NODE_NUM=$2
CLUSTER_NAME="zks"
NETWORK_ADDR='172.22.2.0'
CPORT='9639'
ZKS_DAEMON_IMAGE='zksdaemon:latest'
ZKS_IMAGE='zookeeper:3.6.3'

if [ -z $NODE_NUM ];then
    NODE_NUM=3
fi

if [ -z $WORK_DIR ];then
    WORK_DIR=`pwd`
    WORK_DIR=$WORK_DIR"/zksData"
    mkdir $WORK_DIR
    echo "cmd: $0 [data dir] [node numbers]"
    exit 1
fi

if [ ! -z $3 ];then
    ZKS_DAEMON_IMAGE=$3
fi

if [ ! -z $4 ];then
    ZKS_IMAGE=$4
fi

echo "### zookeeper cluster ###"
echo "WORK_DIR: $WORK_DIR"
echo "NODE_NUM: $NODE_NUM"
echo "CLUSTER_NAME: $CLUSTER_NAME"
echo "NETWORK_ADDR: $NETWORK_ADDR"
echo "CPORT: $CPORT"
echo "ZKS_DAEMON_IMAGE: $ZKS_DAEMON_IMAGE"
echo "ZKS_IMAGE: $ZKS_IMAGE"
echo "####################\n"

#建立集群网络
network_name=$CLUSTER_NAME'-net'
ret=`docker network ls |grep "$network_name"|awk '{print $2}'`
if [ "$ret" != "$network_name" ];then
    docker network create --subnet=$NETWORK_ADDR/24 $network_name
    if [ $? != 0 ];then
        echo "docker network create --subnet=$NETWORK_ADDR/24 $network_name fail..."
        exit 1
    fi
fi

function create_zks_cfg(){
    local myid=$1
    local myip=$2
    local zksDir=$3
    local cluster=$4

    if [ ! -d "$zksDir" ];then
        echo "path[$zksDir] invalid."
        return 1
    fi
    ret=`docker container ps |grep $CLUSTER_NAME-$myid`
    if [ ! -z "$ret" ];then
        echo "stop and rm container: $CLUSTER_NAME-$myid"
        docker container stop $CLUSTER_NAME-$myid
	    docker rm $CLUSTER_NAME-$myid
    fi
    rm -rf $zksDir/*
    mkdir $zksDir/{conf,data,datalog}
    echo "$myid">$zksDir/data/myid
    echo "autopurge.purgeInterval=12
initLimit=10
syncLimit=2
autopurge.snapRetainCount=40
skipACL=yes
forceSync=no
zookeeper.electionPortBindRetry=604800
4lw.commands.whitelist=*
globalOutstandingLimit=5000
[zookeeper]=
tickTime=2000
dataDir=/data
dataLogDir=/datalog
reconfigEnabled=true
preAllocSize=16384
standaloneEnabled=false" > $zksDir/conf/zoo.cfg
    #这里可以考虑少于等于3个节点时，都是participant。
    if [ -z $cluster ];then
        echo -e "server.$myid=$myip:2888:3888:participant;0.0.0.0:$CPORT" >> $zksDir/conf/zoo.cfg
    else
        echo -e "$cluster" >> $zksDir/conf/zoo.cfg
        echo -e "server.$myid=$myip:2888:3888:observer;0.0.0.0:$CPORT" >> $zksDir/conf/zoo.cfg
    fi

echo "export ZK_SERVER_HEAP=2048
export JMXLOCALONLY=true
export JMX_RMI_HOST=localhost
export JMXPORT=9630
export ZOO_ADMIN_SERVER_ENABLE=false
export ZOO_ADMIN_SERVER_HOST=localhost
export ZOO_ADMIN_SERVER_PORT=9631
export ZOO_LOG4J_PROP=INFO,CONSOLE
export ZOO_LOG_DIR=/var/
export ZOO_RANDOM_PATH=file:/dev/./urandom
export ZOO_ELEPORT_BIND_RETRY=604800
export JVMFLAGS=\"-Dfsync.warningthresholdms=5 $JVMFLAGS\"" > $zksDir/conf/java.env

echo 'zookeeper.root.logger=INFO, CONSOLE
zookeeper.console.threshold=INFO
zookeeper.log.dir=.
zookeeper.log.file=zookeeper.log
zookeeper.log.threshold=INFO
zookeeper.log.maxfilesize=128MB
zookeeper.log.maxbackupindex=8
zookeeper.tracelog.dir=${zookeeper.log.dir}
zookeeper.tracelog.file=zookeeper_trace.log
log4j.rootLogger=${zookeeper.root.logger}
log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=${zookeeper.console.threshold}
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.ROLLINGFILE=org.apache.log4j.RollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=${zookeeper.log.threshold}
log4j.appender.ROLLINGFILE.File=${zookeeper.log.dir}/${zookeeper.log.file}
log4j.appender.ROLLINGFILE.MaxFileSize=${zookeeper.log.maxfilesize}
log4j.appender.ROLLINGFILE.MaxBackupIndex=${zookeeper.log.maxbackupindex}
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L] - %m%n
log4j.appender.TRACEFILE=org.apache.log4j.FileAppender
log4j.appender.TRACEFILE.Threshold=TRACE
log4j.appender.TRACEFILE.File=${zookeeper.tracelog.dir}/${zookeeper.tracelog.file}
log4j.appender.TRACEFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.TRACEFILE.layout.ConversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L][%x] - %m%n
log4j.appender.SYSLOG=org.apache.log4j.net.SyslogAppender
log4j.appender.SYSLOG.syslogHost=localhost
log4j.appender.SYSLOG.layout=org.apache.log4j.PatternLayout
log4j.appender.SYSLOG.layout.conversionPattern=%d{ISO8601} [myid:%X{myid}] - %-5p [%t:%C{1}@%L][%x] - %m%n
log4j.appender.SYSLOG.Facility=LOCAL6
log4j.appender.SYSLOG.Threshold=debug
log4j.appender.SYSLOG.FacilityPrinting=true' > $zksDir/conf/log4j.properties

}

function create_zks(){
    local id=$1
	local ip=$2
	local path=$3
	cmd="docker run -dit --name=$CLUSTER_NAME-$id --network=$network_name --ip=$ip -v $path/conf:/conf -v $path/data:/data -v $path/datalog:/datalog $ZKS_IMAGE"
    docker container stop $CLUSTER_NAME-$id > /dev/null 2>&1
    docker rm $CLUSTER_NAME-$id > /dev/null 2>&1
	$cmd
	if [ $? != 0 ];then
		echo "run docker fail, [$cmd]"
		return 1
	else
		echo "success for [$cmd]"
	fi
    cnt=0
    while [[ $cnt -lt 100 ]]
    do
        sleep 3
        ret=`echo ruok|nc $ip $CPORT`
        if [ "$ret" == 'imok' ];then
            echo "$CLUSTER_NAME-$id have running."
            return 0
        fi
        echo "wait for $CLUSTER_NAME-$id ok ..."
        let cnt++

    done
    echo "error: wait for $CLUSTER_NAME-$id timeout."
    return 1
}

function add_zks_node(){
    local myid=$1
    local myip=$2
    local cluster=$3

    dataDir=$WORK_DIR/zks-$myid
    if [ ! -d "$dataDir" ];then
        mkdir $dataDir
    fi

    create_zks_cfg $myid $myip $dataDir $cluster
    if [ $? != 0 ];then
        echo "[create_zks_cfg $myid $myip $dataDir $cluster] fail..."
        return 1
    fi
    create_zks $myid $myip $dataDir
    if [ $? != 0 ];then
        echo "[create_zks $myid $myip $dataDir] fail..."
        return 1
    fi

    return 0
}

function add_zks_daemon(){
    local myid=$1
    local myhost=$2

    ret=`docker container ps -a|grep $CLUSTER_NAME-$myid-daemon`
    if [ ! -z "$ret" ];then
        echo "stop and rm container: $CLUSTER_NAME-$myid-daemon"
        docker container stop $CLUSTER_NAME-$myid-daemon
	    docker rm $CLUSTER_NAME-$myid-daemon
    fi
    docker container stop $CLUSTER_NAME-$myid-daemon > /dev/null 2>&1
    docker rm $CLUSTER_NAME-$myid-daemon> /dev/null 2>&1
    cmd="docker run -dit --name=$CLUSTER_NAME-$myid-daemon --network=host $ZKS_DAEMON_IMAGE $myid $myhost"
    $cmd
    if [ $? != 0 ];then
		echo "run daemon docker fail, [$cmd]"
		return 1
	else
		echo "success for [$cmd]"
	fi
    return 0
}

function clear_cluster()
{
    for cons in `docker container ps |grep $CLUSTER_NAME|awk '{print$1}'`
    do
        docker container stop $cons
        docker rm $cons
        echo "stop and rm container: $cons"
    done
}

#创建zookeeper集群
i=1
net_ip=${NETWORK_ADDR%.*}
g_myid=2
echo "Now create zookeeper cluster: $net_ip. [$g_myid - $NODE_NUM] "
server_info=''
clear_cluster
while [ $i -le $NODE_NUM ]
do
	let i++
    let ip_addr=g_myid
	node_ip=$net_ip.$ip_addr

	echo "zks Node message: {id:$g_myid} {ip:$node_ip}"

	if [ -z "$server_info" ];then
        echo "this is first node for cluster."
    fi

	add_zks_node $g_myid $node_ip $server_info
    if [ $? != 0 ];then
        echo "[add_zks_node $g_myid $node_ip $server_info] fail..."
        exit 1
    fi

    add_zks_daemon $g_myid $node_ip:$CPORT
    if [ $? != 0 ];then
        echo "[add_zks_daemon $g_myid $node_ip:$CPORT] fail..."
        exit 1
    fi

	server_info=`echo conf|nc $node_ip $CPORT|grep 'server\.'|grep "$CPORT"`
	echo "update cluster: $server_info"

	let g_myid++
	sleep 5
done
