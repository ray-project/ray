#!/bin/bash
# shellcheck disable=SC2046

# Shard bazel tests and then run bazel test
# Passes arguments through

set -x

test_tag_filters=""
optional_args=()
targets=()
for arg in "$@"; do
    shift
    if [[ "${arg:0:19}" == "--test_tag_filters=" ]]
    then
        test_tag_filters="${arg:19}"
    elif [[ "${arg:0:1}" == "-" ]]
    then
        optional_args+=("$arg")
    else
        targets+=("$arg")
    fi
done

SHARD=$(python ./ci/run/bazel_sharding/bazel_sharding.py --exclude_manual --index "${BUILDKITE_PARALLEL_JOB}" --count "${BUILDKITE_PARALLEL_JOB_COUNT}" --tag_filters="$test_tag_filters" "${targets[@]}")
echo "$SHARD"
echo "$SHARD" | xargs bazel test --test_tag_filters="$test_tag_filters" "${optional_args[@]}"
