#!/bin/bash

function usage() {
    echo " -t|--target-dir <dir> local target directory for prepare a Ray cluster deployment package"
    echo " [-s|--source-dir] <dir> local source directory to prepare a Ray cluster deployment package"
}

while [ $# -gt 0 ];do
    key=$1
    case $key in
        -h|--help)
            usage
            exit 0
            ;;
        -s|--source-dir)
            ray_dir=$2
            shift 2
            ;;
        -t|--target-dir)
            t_dir=$2
            shift 2
            ;;
        *)
            echo "ERROR: unknown option $key"
            echo
            usage
            exit -1
            ;;
    esac
done

realpath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

if [ -z $ray_dir ];then
    scripts_path=`realpath $0`
    ray_dir=`dirname $scripts_path`
    ray_dir=`dirname $ray_dir`
fi

# echo "ray_dir = $ray_dir"

declare -a nativeBinaries=(
    "./src/common/thirdparty/redis/src/redis-server"
    "./src/plasma/plasma_store"
    "./src/plasma/plasma_manager"
    "./src/local_scheduler/local_scheduler"
    "./src/global_scheduler/global_scheduler"
    "./src/ray/raylet/raylet"
    "./src/ray/raylet/raylet_monitor"
)

declare -a nativeLibraries=(
    "./src/common/redis_module/libray_redis_module.so"
    "./src/local_scheduler/liblocal_scheduler_library_java.*"
    "./src/plasma/libplasma_java.*"
    "./src/ray/raylet/*lib.a"
)

declare -a javaBinaries=(
    "api"
    "common"
    "worker"
    "test"
)

function prepare_source()
{
    if [ -z $t_dir ];then
        echo "--target-dir not specified"
        usage
        exit -1
    fi

    # prepare native components under /ray/native/bin
    mkdir -p $t_dir"/ray/native/bin/"
    for i in "${!nativeBinaries[@]}"
    do
        cp $ray_dir/build/${nativeBinaries[$i]} $t_dir/ray/native/bin/
    done

    # prepare native libraries under /ray/native/lib
    mkdir -p $t_dir"/ray/native/lib/"
    for i in "${!nativeLibraries[@]}"
    do
        cp $ray_dir/build/${nativeLibraries[$i]} $t_dir/ray/native/lib/
    done

    # prepare java components under /ray/java/lib
    mkdir -p $t_dir"/ray/java/lib/"
    unzip -q $ray_dir/java/cli/target/ray-cli-ear.zip -d $ray_dir/java
    cp $ray_dir/java/ray-cli/lib/* $t_dir/ray/java/lib/
    rm -rf $ray_dir/java/ray-cli

    cp -rf $ray_dir/java/ray.config.ini $t_dir/ray/

    # prepare java apps directory
    mkdir -p $t_dir"/ray/java/apps/"

    # prepare run.sh
    cp $ray_dir/java/run.sh $t_dir/
}

prepare_source
