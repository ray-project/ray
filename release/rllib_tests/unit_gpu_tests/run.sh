#!/usr/bin/env bash

ray_version="" 
commit=""
ray_branch=""

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
echo "workload: ignored"

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp36-cp36m-manylinux1_x86_64.whl"

conda uninstall -y terminado
source activate tensorflow_p36 && pip install -U pip
source activate tensorflow_p36 && pip install -U "$wheel"

# Run all test cases, but with a forced num_gpus=1.
# TODO: (sven) chose correct dir and run over all RLlib tests and example scripts!
source activate tensorflow_p36 && export RAY_FORCE_NUM_GPUS=1 && cd ~ && python -m pytest test_attention_net_learning.py
