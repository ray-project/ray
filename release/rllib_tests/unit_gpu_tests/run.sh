#!/usr/bin/env bash
# shellcheck disable=SC2046
cob="$1"


conda uninstall -y terminado
pip install -U pip
pip install -U pytest
pip install terminado
pip install torch>=1.6 torchvision
pip install -U tensorflow-gpu

git clone https://github.com/ray-project/ray.git ray
pushd ray || true
git checkout "$cob"

bash ./ci/travis/install-bazel.sh
BAZEL_PATH=$HOME/bin/bazel

# Run all test cases, but with a forced num_gpus=1.
# TODO: (sven) chose correct dir and run over all RLlib tests and example scripts!
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config=ci $(./scripts/bazel_export_options) --build_tests_only --test_tag_filters=examples_A,examples_B --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 rllib/...
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config=ci $(./scripts/bazel_export_options) --build_tests_only --test_tag_filters=examples_C,examples_D --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 rllib/...
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config=ci $(./scripts/bazel_export_options) --build_tests_only --test_tag_filters=examples_E,examples_F,examples_G,examples_H,examples_I,examples_J,examples_K,examples_L,examples_M,examples_N,examples_O,examples_P --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1  rllib/...
export RLLIB_NUM_GPUS=1 && $BAZEL_PATH test --config=ci $(./scripts/bazel_export_options) --build_tests_only --test_tag_filters=examples_Q,examples_R,examples_S,examples_T,examples_U,examples_V,examples_W,examples_X,examples_Y,examples_Z --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 rllib/...
popd || true
