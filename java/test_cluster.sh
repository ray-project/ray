#!/bin/bash

# test ray java in cluster mode.
# 1. start ray head, it will starts redis-server, object-store and
#    raylet locally, then raylet should start workers (default 2 workers).
# 2. start a driver, make sure the jars are pre-deployed on each nodes
#    todo:: auto-deploy jars

basedir=`dirname ${0}; pwd`
cd ${basedir}

assembly_file_name=ray-0.1-SNAPSHOT

sh bin/clean.sh
rm -rf ${assembly_file_name}

# decompress the tar.gz file
tar -xzf assembly/target/${assembly_file_name}-bin.tar.gz
pushd ${assembly_file_name}

# start the head node
./bin/run.sh start --head --config=assembly.conf

# start the driver
./bin/driver.sh exercise-02 ray-tutorial-0.1-SNAPSHOT.jar org.ray.exercise.Exercise02

# check the log
result=$(cat "./logs/exercise-02.log")
if [[ ${result} =~ "hello,world!" ]]; then
    echo "failed cluster test"
    exit 1
else
    echo "success cluster test"
fi

popd

sleep 1

sh bin/clean.sh
rm -rf ${assembly_file_name}
