#!/usr/bin/env bash

ray_version="" 
commit=""
ray_branch=""
workload=""

usage() {
    echo "Start one microbenchmark trial."
}

for i in "$@"
do
echo $i
case $i in
    --ray-version=*)
    ray_version="${i#*=}"

    ;;
    --commit=*)
    commit="${i#*=}"
    ;;
    --ray-branch=*)
    ray_branch="${i#*=}"
    ;;
    --workload=*)
    workload="${i#*=}"
    ;;
    --help)
    usage
    exit
    ;;
    *)
    echo "unknown arg, $i"
    exit 1
    ;;
esac
done

if [[ $ray_version == "" || $commit == "" || $ray_branch == "" ]]
then
    echo "Provide --ray-version, --commit, and --ray-branch"
    exit 1
fi

echo "version: $ray_version"
echo "commit: $commit"
echo "branch: $ray_branch"
echo "workload: $workload"

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp36-cp36m-manylinux1_x86_64.whl"

pip install -U pip
source activate tensorflow_p36 && pip install -q -U $wheel Click
source activate tensorflow_p36 && pip install -q ray[all] gym[atari]
source activate tensorflow_p36 && python "workloads/$workload.py"

