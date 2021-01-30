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

echo "version: $ray_version"
echo "commit: $commit"
echo "branch: $ray_branch"
echo "workload: ignored"

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp37-cp37m-manylinux2014_x86_64.whl"
wheel="https://ray-wheels.s3-us-west-2.amazonaws.com/releases/1.2.0/b4d87b8fc5ff03b868adba35b37da32b7be9f71f/ray-1.2.0-cp37-cp37m-manylinux2014_x86_64.whl"

conda uninstall -y terminado
pip install -U pip
pip install -U "$wheel"
pip install "ray[rllib]" "ray"
pip install terminado
pip install torch==1.6 torchvision
pip install boto3==1.4.8 cython==0.29.0

ray stop

# Run tf learning tests.
rllib train -f compact-regression-tests-tf.yaml

# Run torch learning tests.
rllib train -f compact-regression-tests-torch.yaml
