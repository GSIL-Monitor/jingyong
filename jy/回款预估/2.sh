#!/usr/bin/env bash
while true
do
    procnum=`ps -ef|grep "scheduler"|grep -v grep|wc -l`
    if [ $procnum -eq 0 ]
    then
        airflow scheduler
        echo `date +%Y-%m-%d` `date +%H:%M:%S`  "restart 服务" >> /home/jiankong/restart.log
    fi
    sleep 60
done