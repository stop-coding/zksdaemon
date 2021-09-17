#!/usr/bin/env bash

#set -ex

WORK_DIR=`pwd`
NODE_NUM=3
CLUSTER_ID=2
ZKS_DAEMON_IMAGE='zksdaemon:latest'
ZKS_IMAGE='zookeeper:3.6.3'
IS_CLEAR=0

while getopts "p:n:d:z:i:hc" arg 
do
        case $arg in
            c)
                IS_CLEAR=1
                echo "Cleaning cluster...."
                ;;
            p)
                WORK_DIR=$OPTARG
                ;;
            n)
                NODE_NUM=$OPTARG
                ;;
            d)
                ZKS_DAEMON_IMAGE=$OPTARG
                ;;
            z)
                ZKS_IMAGE=$OPTARG
                ;;
            i)
                CLUSTER_ID=$OPTARG
                ;;
            h)
                echo "### help ####"
                echo "Usage:  $0 -p [path] -n [node numbers] -i [cluster id] -d [zks daemon docker image] -z [ zookeeper docker image]"
                echo "-h  :  help message"
                echo "-c  :  clear cluster."
                echo "###      ####"
                exit 0
                ;;
            ?) 
                echo "ERROR:  unkonw argument: $*"
                echo "Usage:  $0 -p [path] -n [node numbers] -i [cluster id] -d [zks daemon docker image] -z [ zookeeper docker image]"
                exit 1
                ;;
        esac
done

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
    echo "autopurge.purgeInterval=8
initLimit=10
syncLimit=2
autopurge.snapRetainCount=3
skipACL=yes
forceSync=no
zookeeper.electionPortBindRetry=604800
4lw.commands.whitelist=*
globalOutstandingLimit=5000
metricsProvider.className=org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider
metricsProvider.httpPort=7000
metricsProvider.exportJvmInfo=true
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
        echo -e "server.$myid=$myip:2888:3888:participant;0.0.0.0:$CPORT" >> $zksDir/conf/zoo.cfg
    fi

echo "export ZK_SERVER_HEAP=2048
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
    chmod -R a+rwx  $zksDir
}

function create_zks(){
    local id=$1
	local ip=$2
	local path=$3
	cmd="docker run -dit --name=$CLUSTER_NAME-$id --privileged=true --network=$network_name --ip=$ip -v $path/conf:/conf -v $path/data:/data -v $path/datalog:/datalog $ZKS_IMAGE"
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
    sleep 1
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
    cmd="docker run -dit --privileged=true --name=$CLUSTER_NAME-$myid-daemon --network=host $ZKS_DAEMON_IMAGE $myid $myhost"
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
    for cons in `docker container ps -a|grep $CLUSTER_NAME|awk '{print$1}'`
    do
        docker container stop $cons
        docker rm $cons
        echo "stop and rm container: $cons"
    done
}

CLUSTER="zks"
CLUSTER_NAME="$CLUSTER.$CLUSTER_ID"
NETWORK_ADDR="172.22.$CLUSTER_ID.0"
CPORT='9639'

echo "### create zookeeper cluster ###"

if [ ! -d $WORK_DIR ];then
    echo "WORK_DIR: $WORK_DIR invalid"
    exit 1
fi

WORK_DIR="$WORK_DIR/$CLUSTER_NAME"
if [ ! -e $WORK_DIR ];then
    mkdir $WORK_DIR
fi

echo "WORK_DIR: $WORK_DIR"
echo "NODE_NUM: $NODE_NUM"
echo "CLUSTER_NAME: $CLUSTER_NAME"
echo "NETWORK_ADDR: $NETWORK_ADDR"
echo "CPORT: $CPORT"
echo "ZKS_DAEMON_IMAGE: $ZKS_DAEMON_IMAGE"
echo "ZKS_IMAGE: $ZKS_IMAGE"
echo "####################"



#创建zookeeper集群
i=1
net_ip=${NETWORK_ADDR%.*}
g_myid=2
server_info=''

clear_cluster
if [ $IS_CLEAR != 0 ];then
    for cons in `docker container ps -a|grep $CLUSTER|awk '{print$1}'`
    do
        docker container stop $cons
        docker rm $cons
        echo "stop and rm container: $cons"
    done
    echo "Clear zookeeper cluster Finished."
    exit 0
fi

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

echo "Now create zookeeper cluster: $net_ip.[$g_myid - $NODE_NUM] "
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

	#server_info=`echo conf|nc $node_ip $CPORT|grep 'server\.'|grep "$CPORT"`
    server_info="$server_info""server.$g_myid=$node_ip:2888:3888:participant;0.0.0.0:$CPORT\n"
	echo -e "update cluster: $server_info"

	let g_myid++
	sleep 5
done
