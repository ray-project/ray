#!/usr/bin/env bash

bin=`dirname ${0}`
bin=`cd ${bin}; pwd`
basedir=${bin}/..
RUN_DIR=${basedir}/run

# todo:: we should also record all process pid in run_dir
# kill worker with -15
pkill -9 plasma_store_server
pkill -9 redis-server
pkill -9 raylet
ps aux | grep DefaultWorker | grep -v grep | awk '{print $2}' | xargs kill -9

rm -rf /tmp/ray/sockets/*
rm -rf /tmp/ray/logs
