# 简介

zookeeper daemon for servers

# 环境配置

### 1.安装docker
debian：

    yum install docker

### 2.获取镜像

获取zookeeper镜像，如3.6.3
获取python镜像，如python:latest

    docker pull zookeeper:3.6.3
    docker pull python:latest

### 3.打包本地镜像
进入项目目录，执行镜像本地打包
镜像打包需要安装kazoo，可能会因为下载地址为国外镜像源而失败。若失败重试即可

    docker build -t zksdaemon:1.0.1 -f Dockerfile . --no-cache

# 集群
### 1.zookeeper集群

集群使用容器组成，单机部署集群，会自动建立集群网络
默认建立3个节点集群；
默认镜像zookeeper:3.6.3, zksdaemon:latest

    ./cluster [工作目录] [节点数] [zks daemon image] [zks image]

    #例如当前目录下新建3个节点的集群，默认使用zookeeper:3.6.3,zksdaemon:latest镜像

    ./cluster ./

    #例如在/opt/zookeeper/路径下新建5个节点的集群，使用zookeeper:3.6.0, zksdaemon:latest 镜像

    ./cluster /opt/zookeeper/ 5 zookeeper:3.6.0 zksdaemon:latest

# 测试
 。。。