docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_catalog.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_optimizers.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_filters.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_evaluators.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_eager_support.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_local.py
    
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_reproducibility.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_dependency.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_io.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_checkpoint_restore.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_rollout_worker.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_nested_spaces.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_external_env.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_external_multi_agent_env.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_keras_model.py --run=A2C --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_keras_model.py --run=PPO --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_keras_model.py --run=DQN --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/parametric_action_cartpole.py --run=PG --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/parametric_action_cartpole.py --run=PPO --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/parametric_action_cartpole.py --run=DQN --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_lstm.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/batch_norm_model.py --num-iters=1 --run=PPO

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/batch_norm_model.py --num-iters=1 --run=PG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/batch_norm_model.py --num-iters=1 --run=DQN

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/batch_norm_model.py --num-iters=1 --run=DDPG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_multi_agent_env.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_supported_spaces.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/tests/test_rollout.sh

# Run all single-agent regression tests (3x retry each)
for yaml in $(ls $ROOT_DIR/../../rllib/tuned_examples/regression_tests); do
    docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/ci/suppress_output python /ray/rllib/tests/run_regression_tests.py \
            /ray/rllib/tuned_examples/regression_tests/$yaml
done

# Try a couple times since it's stochastic
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/ci/suppress_output python /ray/rllib/tests/multiagent_pendulum.py || \
    docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/ci/suppress_output python /ray/rllib/tests/multiagent_pendulum.py || \
    docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/ci/suppress_output python /ray/rllib/tests/multiagent_pendulum.py


docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/multiagent_cartpole.py --num-iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/multiagent_two_trainers.py --num-iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_avail_actions_qmix.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/cartpole_lstm.py --run=PPO --stop=200

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/cartpole_lstm.py --run=IMPALA --stop=100

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/cartpole_lstm.py --stop=200 --use-prev-action-reward

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_loss.py --iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/rollout_worker_custom_workflow.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/eager_execution.py --iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_tf_policy.py --iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_torch_policy.py --iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/rollout_worker_custom_workflow.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_metrics_and_callbacks.py --num-iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/contrib/random_agent/random_agent.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/contrib/alpha_zero/examples/train_cartpole.py --training-iteration=1

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/centralized_critic.py --stop=2000

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/centralized_critic_2.py --stop=2000

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/twostep_game.py --stop=2000 --run=contrib/MADDPG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/twostep_game.py --stop=2000 --run=PG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/twostep_game.py --stop=2000 --run=QMIX

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/twostep_game.py --stop=2000 --run=APEX_QMIX

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/autoregressive_action_dist.py --stop=150

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/agents/impala/vtrace_test.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_ignore_worker_failure.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_keras_rnn_model.py --run=PPO --stop=50 --env=RepeatAfterMeEnv

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/examples/custom_keras_rnn_model.py --run=PPO --stop=50 --env=RepeatInitialEnv

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output python /ray/rllib/tests/test_env_with_subprocess.py
