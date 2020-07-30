#!/usr/bin/env bash

ray_version="" 
commit=""
ray_branch=""

usage() {
    echo "Start one microbenchmark trial."
}

for i in "$@"
do
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
    --help)
    usage
    exit
    ;;
    *)
    echo "unknown arg, $2"
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

rm ray-$ray_version-cp36-cp36m-manylinux1_x86_64.whl || true
wget https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp36-cp36m-manylinux1_x86_64.whl
      
pip uninstall -y -q ray
pip install -U ray-$ray_version-cp36-cp36m-manylinux1_x86_64.whl

OMP_NUM_THREADS=64 ray microbenchmark
