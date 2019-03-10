docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env PongDeterministic-v0 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pong-ram-v4 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env PongDeterministic-v0 \
    --run A2C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "lr": 1e-4, "sgd_minibatch_size": 64, "train_batch_size": 2000, "num_workers": 1, "model": {"free_log_std": true}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"simple_optimizer": false, "num_sgd_iter": 2, "model": {"use_lstm": true}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"simple_optimizer": true, "num_sgd_iter": 2, "model": {"use_lstm": true}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"num_gpus": 0.1}' \
    --ray-num-gpus 1

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "lr": 1e-4, "sgd_minibatch_size": 64, "train_batch_size": 2000, "num_workers": 1, "use_gae": false, "batch_mode": "complete_episodes"}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"remote_worker_envs": true, "num_envs_per_worker": 2, "num_workers": 1, "train_batch_size": 100, "sgd_minibatch_size": 50}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"async_remote_worker_envs": true, "num_envs_per_worker": 2, "num_workers": 1, "train_batch_size": 100, "sgd_minibatch_size": 50}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pendulum-v0 \
    --run APPO \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "num_gpus": 0}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pendulum-v0 \
    --run ES \
    --stop '{"training_iteration": 1}' \
    --config '{"stepsize": 0.01, "episodes_per_batch": 20, "train_batch_size": 100, "num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pong-v0 \
    --run ES \
    --stop '{"training_iteration": 1}' \
    --config '{"stepsize": 0.01, "episodes_per_batch": 20, "train_batch_size": 100, "num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run A3C \
    --stop '{"training_iteration": 1}' \

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 1}' \
    --config '{"lr": 1e-3, "schedule_max_timesteps": 100000, "exploration_fraction": 0.1, "exploration_final_eps": 0.02, "dueling": false, "hiddens": [], "model": {"fcnet_hiddens": [64], "fcnet_activation": "relu"}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run APEX \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "timesteps_per_iteration": 1000, "num_gpus": 0, "min_iter_time_s": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env FrozenLake-v0 \
    --run DQN \
    --stop '{"training_iteration": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env FrozenLake-v0 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"num_sgd_iter": 10, "sgd_minibatch_size": 64, "train_batch_size": 1000, "num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env PongDeterministic-v4 \
    --run DQN \
    --stop '{"training_iteration": 1}' \
    --config '{"lr": 1e-4, "schedule_max_timesteps": 2000000, "buffer_size": 10000, "exploration_fraction": 0.1, "exploration_final_eps": 0.01, "sample_batch_size": 4, "learning_starts": 10000, "target_network_update_freq": 1000, "gamma": 0.99, "prioritized_replay": true}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env MontezumaRevenge-v0 \
    --run PPO \
    --stop '{"training_iteration": 1}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "lr": 1e-4, "sgd_minibatch_size": 64, "train_batch_size": 2000, "num_workers": 1, "model": {"dim": 40, "conv_filters": [[16, [8, 8], 4], [32, [4, 4], 2], [512, [5, 5], 1]]}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "model": {"use_lstm": true}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 1}' \
    --config '{"sample_batch_size": 500, "num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 1}' \
    --config '{"sample_batch_size": 500, "use_pytorch": true}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 1}' \
    --config '{"sample_batch_size": 500, "num_workers": 1, "model": {"use_lstm": true, "max_seq_len": 100}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 1}' \
    --config '{"sample_batch_size": 500, "num_workers": 1, "num_envs_per_worker": 10}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pong-v0 \
    --run PG \
    --stop '{"training_iteration": 1}' \
    --config '{"sample_batch_size": 500, "num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env FrozenLake-v0 \
    --run PG \
    --stop '{"training_iteration": 1}' \
    --config '{"sample_batch_size": 500, "num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pendulum-v0 \
    --run DDPG \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{"training_iteration": 1}' \
    --config '{"num_gpus": 0, "num_workers": 2, "min_iter_time_s": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{"training_iteration": 1}' \
    --config '{"num_gpus": 0, "num_workers": 2, "min_iter_time_s": 1, "model": {"use_lstm": true}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{"training_iteration": 1}' \
    --config '{"num_gpus": 0, "num_workers": 2, "min_iter_time_s": 1, "num_data_loader_buffers": 2, "replay_buffer_num_slots": 100, "replay_proportion": 1.0}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{"training_iteration": 1}' \
    --config '{"num_gpus": 0, "num_workers": 2, "min_iter_time_s": 1, "num_data_loader_buffers": 2, "replay_buffer_num_slots": 100, "replay_proportion": 1.0, "model": {"use_lstm": true}}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env MountainCarContinuous-v0 \
    --run DDPG \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env MountainCarContinuous-v0 \
    --run DDPG \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pendulum-v0 \
    --run APEX_DDPG \
    --ray-num-cpus 8 \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "optimizer": {"num_replay_buffer_shards": 1}, "learning_starts": 100, "min_iter_time_s": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env Pendulum-v0 \
    --run APEX_DDPG \
    --ray-num-cpus 8 \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "optimizer": {"num_replay_buffer_shards": 1}, "learning_starts": 100, "min_iter_time_s": 1, "batch_mode": "complete_episodes", "parameter_noise": true}'

# TODO(ericl): reenable the test after fix the arrow serialization error.
# https://github.com/ray-project/ray/pull/4127#issuecomment-468903577
#docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
#    /ray/python/ray/rllib/tests/run_silent.sh train.py \
#    --env CartPole-v0 \
#    --run MARWIL \
#    --stop '{"training_iteration": 1}' \
#    --config '{"input": "/ray/python/ray/rllib/tests/data/cartpole_small", "learning_starts": 0, "input_evaluation": ["wis", "is"], "shuffle_buffer_size": 10}'

#docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
#    /ray/python/ray/rllib/tests/run_silent.sh train.py \
#    --env CartPole-v0 \
#    --run DQN \
#    --stop '{"training_iteration": 1}' \
#    --config '{"input": "/ray/python/ray/rllib/tests/data/cartpole_small", "learning_starts": 0, "input_evaluation": ["wis", "is"], "soft_q": true}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_local.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_io.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_checkpoint_restore.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_policy_evaluator.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_nested_spaces.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_external_env.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/parametric_action_cartpole.py --run=PG --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/parametric_action_cartpole.py --run=PPO --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/parametric_action_cartpole.py --run=DQN --stop=50

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_lstm.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/batch_norm_model.py --num-iters=1 --run=PPO

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/batch_norm_model.py --num-iters=1 --run=PG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/batch_norm_model.py --num-iters=1 --run=DQN

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/batch_norm_model.py --num-iters=1 --run=DDPG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_multi_agent_env.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_supported_spaces.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_env_with_subprocess.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_rollout.sh

# Run all single-agent regression tests (3x retry each)
for yaml in $(ls $ROOT_DIR/../../python/ray/rllib/tuned_examples/regression_tests); do
    docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/python/ray/rllib/tests/run_silent.sh tests/run_regression_tests.py \
            /ray/python/ray/rllib/tuned_examples/regression_tests/$yaml
done

# Try a couple times since it's stochastic
docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/python/ray/rllib/tests/run_silent.sh tests/multiagent_pendulum.py || \
    docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/python/ray/rllib/tests/run_silent.sh tests/multiagent_pendulum.py || \
    docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
        /ray/python/ray/rllib/tests/run_silent.sh tests/multiagent_pendulum.py


docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/multiagent_cartpole.py --num-iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/multiagent_two_trainers.py --num-iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh tests/test_avail_actions_qmix.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/cartpole_lstm.py --run=PPO --stop=200

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/cartpole_lstm.py --run=IMPALA --stop=100

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/cartpole_lstm.py --stop=200 --use-prev-action-reward

# TODO(ericl): reenable the test after fix the arrow serialization error.
# https://github.com/ray-project/ray/pull/4127#issuecomment-468903577
#docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
#    /ray/python/ray/rllib/tests/run_silent.sh examples/custom_loss.py --iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/custom_metrics_and_callbacks.py --num-iters=2

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh contrib/random_agent/random_agent.py

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/twostep_game.py --stop=2000 --run=PG

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/twostep_game.py --stop=2000 --run=QMIX

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh examples/twostep_game.py --stop=2000 --run=APEX_QMIX

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "use_pytorch": true, "sample_async": false, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "use_pytorch": true, "sample_async": false}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/python/ray/rllib/tests/run_silent.sh train.py \
    --env PongDeterministic-v4 \
    --run IMPALA \
    --stop='{"timesteps_total": 40000}' \
    --ray-object-store-memory=500000000 \
    --config '{"num_workers": 1, "num_gpus": 0, "num_envs_per_worker": 64, "sample_batch_size": 50, "train_batch_size": 50, "learner_queue_size": 1}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/rllib/agents/impala/vtrace_test.py
