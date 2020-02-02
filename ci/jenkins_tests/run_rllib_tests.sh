# -------------------------------------------------------------------
# -------------------------------------------------------------------
# NOTE: THIS FILE IS BEING DEPRECATED. PLEASE DO NOT ADD MORE TESTS
# TO THIS FILE. INSTEAD, ADD NEW RLLIB TEST CASES TO /ray/rllib/BUILD
# -------------------------------------------------------------------
# -------------------------------------------------------------------

# Not moved to BAZEL yet due to *.sh.
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/tests/test_rollout.sh
