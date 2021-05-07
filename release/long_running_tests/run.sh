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

# Serve load testing tool
cur_dir=$(pwd)
cd /tmp && rm -rf wrk && git clone https://github.com/wg/wrk.git wrk && cd wrk && make -j && sudo cp wrk /usr/local/bin
cd "$cur_dir" || exit

python "./workloads/$workload.py"
