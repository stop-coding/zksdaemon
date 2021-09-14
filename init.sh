#!/usr/bin/env bash

myid=$1
host=$2

set -ex

if [ -z $myid ];then
    echo "myid is empty"
    exit 1
fi
echo "myid:$myid"

if [ -z $host ];then
    echo "host is empty"
    exit 1
fi
echo "host:$host"

python3 /app/zksDaemon.py -i $myid -h $host
if [ $? != 0 ];then
    echo "$myid connect $host some error, exit now"
    exit 1
fi
echo "$myid connect $host finished, exit now."
