#!/usr/bin/env bash

ray_version=""
commit=""
ray_branch=""
workload=""

usage() {
    echo "Run a horovod test."
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
pip install -q -U ipdb torch torchvision
HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_MXNET=1 HOROVOD_WITH_PYTORCH=1  pip install -U git+https://github.com/horovod/horovod.git

echo set-window-option -g mouse on > ~/.tmux.conf
echo 'termcapinfo xterm* ti@:te@' > ~/.screenrc

python "workloads/$workload.py"
