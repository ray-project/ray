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

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp36-cp36m-manylinux1_x86_64.whl"

# Install Anaconda.
wget --quiet https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh || true
bash Anaconda3-5.0.1-Linux-x86_64.sh -b -p "$HOME/anaconda3" || true
# shellcheck disable=SC2016
echo 'export PATH="$HOME/anaconda3/bin:$PATH"' >> ~/.bashrc

conda uninstall -y terminado
source activate tensorflow_p36 && pip install -U pip
source activate tensorflow_p36 && pip install -U "$wheel"

pip install -U pip 
conda uninstall -y terminado || true
pip install terminado
pip install boto3==1.4.8 cython==0.29.0
python "workloads/$workload.py"
