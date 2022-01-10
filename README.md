# 简介

该项目用于检测和调整zookeeper集群follower角色，实现集群follower角色动态稳定在一定的数量。
zookeeper 服务端角色有参与者哥观察者，对于大规模集群，参与者过多会影响集群性能，但太少可靠性又降低。
这里提出一种动态改变follwer角色的解决方案，但集群里follower节点故障时，可以自动地让观察者升级为参与者。
具体实现请看[WIKI](https://github.com/stop-coding/zksdaemon/wiki/zookeeper%E5%8A%A8%E6%80%81%E9%9B%86%E7%BE%A4%E8%AE%BE%E8%AE%A1)


# 环境配置

### 1.安装docker
debian：

    yum install docker

### 2.获取镜像

获取zookeeper镜像，如3.5.6
获取python镜像，如python:latest

    docker pull zookeeper:3.5.6
    docker pull python:latest

### 3.打包本地镜像
进入项目目录，执行镜像本地打包
镜像打包需要安装kazoo，可能会因为下载地址为国外镜像源而失败。若失败重试即可

    docker build -t zksdaemon:latest -f Dockerfile . --no-cache

# 集群
### 1.zookeeper集群

集群使用容器组成，单机部署集群，会自动建立集群网络
默认建立3个节点集群；
默认镜像zookeeper:3.6.3, zksdaemon:latest

    ./cluster -p [path] -n [node numbers] -i [cluster id] -d [zks daemon docker image] -z [ zookeeper docker image]
    ./cluster -p [工作目录] -n [节点数] -i [集群id] -d [zks daemon 镜像] -z [zookeeper 镜像]

    #例如当前目录下新建3个节点的集群，默认使用zookeeper:3.6.3,zksdaemon:latest镜像
    ./cluster

    #例如在/opt/zookeeper/路径下新建5个节点的集群，使用zookeeper:3.6.0, zksdaemon:latest 镜像

    ./cluster -p /opt/zookeeper/ -n 5 -z zookeeper:3.6.0 -d zksdaemon:latest

# 测试
 。。。
