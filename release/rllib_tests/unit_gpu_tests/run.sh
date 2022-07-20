#!/usr/bin/env bash
# shellcheck disable=SC2046
cob="$1"

git clone https://github.com/ray-project/ray.git ray
pushd ray || true
git checkout "$cob"

#bash ./ci/env/install-bazel.sh
#BAZEL_PATH=$HOME/bin/bazel

#ray stop

SUCCESS=1

# Run all test cases, but with a forced num_gpus=1 (--test_env=RLLIB_NUM_GPUS=1).
#if ! $BAZEL_PATH test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=examples_A,examples_B --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 --test_env=RLLIB_NUM_GPUS=1 rllib/... ; then SUCCESS=0; fi
#if ! $BAZEL_PATH test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=examples_C,examples_D --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 --test_env=RLLIB_NUM_GPUS=1 rllib/... ; then SUCCESS=0; fi
#if ! $BAZEL_PATH test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=examples_E,examples_F,examples_G,examples_H,examples_I,examples_J,examples_K,examples_L,examples_M,examples_N,examples_O,examples_P --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 --test_env=RLLIB_NUM_GPUS=1  rllib/... ; then SUCCESS=0; fi
#if ! $BAZEL_PATH test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=examples_Q,examples_R,examples_S,examples_T,examples_U,examples_V,examples_W,examples_X,examples_Y,examples_Z --test_env=RAY_USE_MULTIPROCESSING_CPU_COUNT=1 --test_env=RLLIB_NUM_GPUS=1 rllib/... ; then SUCCESS=; fi

# Run all test cases, but with a forced num_gpus=1.
export RLLIB_NUM_GPUS=1

if python rllib/examples/attention_net.py --as-test --stop-reward=20 --num-cpus=0; then SUCCESS=0; fi
if python rllib/examples/attention_net.py --framework=torch --as-test --stop-reward=20 --num-cpus=0; then SUCCESS=0; fi

popd || true

echo "{'passed': $SUCCESS}" > "${TEST_OUTPUT_JSON:-/tmp/release_test_out.json}"
exit $((1 - SUCCESS))
