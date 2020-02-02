# -------------------------------------------------------------------
# -------------------------------------------------------------------
# NOTE: THIS FILE IS BEING DEPRECATED. PLEASE DO NOT ADD MORE TESTS
# TO THIS FILE. INSTEAD, ADD NEW RLLIB TEST CASES TO /ray/rllib/BUILD
# -------------------------------------------------------------------
# -------------------------------------------------------------------
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_eval.py --custom-eval

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_catalog.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_optimizers.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_filters.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_evaluators.py


# Not moved to BAZEL yet due to *.runfile problem (declaring external input files and making them available
# at runtime for py_test).
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/train.py \
    --env CartPole-v0 \
    --run MARWIL \
    --stop '{"training_iteration": 1}' \
    --config '{"input": "/ray/rllib/tests/data/cartpole_small", "learning_starts": 0, "input_evaluation": ["wis", "is"], "shuffle_buffer_size": 10}'

# Not moved to BAZEL yet due to *.runfile problem (declaring external input files and making them available
# at runtime for py_test).
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 1}' \
    --config '{"input": "/ray/rllib/tests/data/cartpole_small", "learning_starts": 0, "input_evaluation": ["wis", "is"], "soft_q": true}'

# Not moved to BAZEL yet due to *.sh.
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/tests/test_rollout.sh
