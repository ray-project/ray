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

echo set-window-option -g mouse on > ~/.tmux.conf
echo 'termcapinfo xterm* ti@:te@' > ~/.screenrc
# Serve load testing tool
rm -rf wrk || true && git clone https://github.com/wg/wrk.git wrk && cd wrk && make -j && sudo cp wrk /usr/local/bin
pip install -U pip
unset RAY_ADDRESS
source activate tensorflow_p36 
conda remove -y --force wrapt || true
pip install --upgrade pip
pip install -U tensorflow==1.14
pip install -q -U "$wheel" Click
pip install -q "ray[all]" "gym[atari]"
ray stop
sleep 5
python "$HOME/workloads/$workload.py"

