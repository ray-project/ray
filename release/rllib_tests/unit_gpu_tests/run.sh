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

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp37-cp37m-manylinux2014_x86_64.whl"

conda uninstall -y terminado
pip install -U pip
pip install -U "$wheel"
pip install -U pytest
pip install terminado
pip install torch>=1.6 torchvision
pip install -U tensorflow-gpu

if [ -z "$commit" ]; then
  cob="origin/$ray_branch"
else
  cob="$commit"
fi

git clone https://github.com/ray-project/ray.git ray
pushd ray || true
git checkout "$cob"

bash ./ci/travis/install-bazel.sh
BAZEL_PATH=$HOME/bin/bazel

# Run all test cases, but with a forced num_gpus=1.
# TODO: (sven) chose correct dir and run over all RLlib tests and example scripts!
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config="ci $(./scripts/bazel_export_options)" --build_tests_only --test_tag_filters=examples_A,examples_B --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 rllib/...
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config="ci $(./scripts/bazel_export_options)" --build_tests_only --test_tag_filters=examples_C,examples_D --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 rllib/...
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config="ci $(./scripts/bazel_export_options)" --build_tests_only --test_tag_filters=examples_E,examples_F,examples_G,examples_H,examples_I,examples_J,examples_K,examples_L,examples_M,examples_N,examples_O,examples_P --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1  rllib/...
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config="ci $(./scripts/bazel_export_options)" --build_tests_only --test_tag_filters=examples_Q,examples_R,examples_S,examples_T,examples_U,examples_V,examples_W,examples_X,examples_Y,examples_Z --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 rllib/...
popd || true
