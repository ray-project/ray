#!/usr/bin/env bash

ray_version="" 
commit=""
ray_branch=""
workload=""

for i in "$@"
do
echo "$i"
case "$i" in
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

echo "version: $ray_version"
echo "commit: $commit"
echo "branch: $ray_branch"
echo "workload: $workload"

python "workloads/$workload.py"
