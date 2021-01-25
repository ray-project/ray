#!/usr/bin/env bash

nodes=""
ray_version="" 
commit=""
ray_branch=""

for i in "$@"
do
echo "$i"
case "$i" in
    --nodes=*)
    nodes="${i#*=}"
    ;;
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

if [[ $nodes == "" || $ray_version == "" || $commit == "" || $ray_branch == "" ]]
then
    echo "Provide --nodes --ray-version, --commit, and --ray-branch"
    exit 1
fi

echo "nodes: $nodes"
echo "version: $ray_version"
echo "commit: $commit"
echo "branch: $ray_branch"
echo "workload: ignored"

# wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp37-cp37m-manylinux2014_x86_64.whl"
# pip install -U "$wheel"

if ! python "wait_cluster.py" "$nodes" 600; then
  echo "Cluster did not come up in time. Aborting test."
  exit 1
fi

python "workloads/$workload.py"
