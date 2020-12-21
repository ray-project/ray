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

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp37-cp37m-manylinux2014_x86_64.whl"

conda uninstall -y terminado || true
pip install -U pip
pip install terminado
pip install -U "$wheel"
pip install "ray[rllib]"
pip install -U ipdb
# There have been some recent problems with torch 1.5 and torchvision 0.6
# not recognizing GPUs.
# So, we force install torch 1.4 and torchvision 0.5. 
# https://github.com/pytorch/pytorch/issues/37212#issuecomment-623198624.
pip install torch==1.4.0 torchvision==0.5.0
echo set-window-option -g mouse on > ~/.tmux.conf
echo 'termcapinfo xterm* ti@:te@' > ~/.screenrc

python "workloads/$workload.py"
