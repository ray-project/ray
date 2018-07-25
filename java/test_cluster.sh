#!/bin/bash

set -e

#############################
# build deploy file and deploy cluster

function sed_config_file()
{
    unamestr="$(uname)"
    if [[ "$unamestr" == "Linux" ]]; then
        if [[ "$1" == "raylet" ]];then
            sed -i 's/^use_raylet.*$/use_raylet = true/g' ray.config.ini
        else
            sed -i 's/^use_raylet.*$/use_raylet = false/g' ray.config.ini
        fi
    elif [[ "$unamestr" == "Darwin" ]]; then
        if [[ "$1" == "raylet" ]]; then
            sed -i '_' 's/^use_raylet.*$/use_raylet = true/g' ray.config.ini
        elif [[ "$1" == "non-raylet" ]]; then
            sed -i '_' 's/^use_raylet.*$/use_raylet = false/g' ray.config.ini
        fi
    fi
}

function run_test_cluster()
{
    pushd java
    sed_config_file "$1"
    sh cleanup.sh
    rm -rf local_deploy
    ./prepare.sh -t local_deploy
    pushd local_deploy
    local_ip=`ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
    #echo "use local_ip" $local_ip
    OVERWRITE="ray.java.start.redis_port=34222;ray.java.start.node_ip_address=$local_ip;ray.java.start.deploy=true;ray.java.start.run_mode=CLUSTER"
    echo OVERWRITE is $OVERWRITE
    ./run.sh start --head --overwrite=$OVERWRITE > cli.log 2>&1 &
    popd
    sleep 10

    # auto-pack zip for app example
    pushd example
    if [ ! -d "app1/" ];then
        mkdir app1
    fi
    cp -rf target/ray-example-1.0.jar app1/
    zip -r app1.zip app1
    popd

    # run with cluster mode
    pushd local_deploy
    export RAY_CONFIG=ray/ray.config.ini
    ARGS=" --package ../example/app1.zip --class org.ray.example.HelloWorld --args=test1,test2  --redis-address=$local_ip:34222"
    ../local_deploy/run.sh submit $ARGS
    popd

    # check: sleeping 5 second for waiting programs finished.
    sleep 5
        result=$(cat local_deploy/cli.log)
    [[ ${result} =~ "Started Ray head node" ]] || exit 1

    result=$(cat /tmp/org.ray.example.HelloWorld/0.out.txt)
    [[ ${result} =~ "hello,world!" ]] || exit 1
    popd
}

run_test_cluster non-raylet
run_test_cluster raylet
