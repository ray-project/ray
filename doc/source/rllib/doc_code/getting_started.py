# flake8: noqa

# __rllib-first-config-begin__
from ray.rllib.algorithms.ppo import PPOConfig
from ray.tune.logger import pretty_print


algo = (
    PPOConfig()
    .env_runners(num_env_runners=1)
    .resources(num_gpus=0)
    .environment(env="CartPole-v1")
    .build()
)

for i in range(10):
    result = algo.train()
    print(pretty_print(result))

    if i % 5 == 0:
        checkpoint_dir = algo.save().checkpoint.path
        print(f"Checkpoint saved in directory {checkpoint_dir}")
# __rllib-first-config-end__

import ray

ray.shutdown()

if False:
    # __rllib-tune-config-begin__
    import ray
    from ray import train, tune

    ray.init()

    config = PPOConfig().training(lr=tune.grid_search([0.01, 0.001, 0.0001]))

    tuner = tune.Tuner(
        "PPO",
        run_config=train.RunConfig(
            stop={"env_runners/episode_return_mean": 150},
        ),
        param_space=config,
    )

    tuner.fit()
    # __rllib-tune-config-end__

    # __rllib-tuner-begin__
    # ``Tuner.fit()`` allows setting a custom log directory (other than ``~/ray-results``)
    tuner = ray.tune.Tuner(
        "PPO",
        param_space=config,
        run_config=train.RunConfig(
            stop={"env_runners/episode_return_mean": 150},
            checkpoint_config=train.CheckpointConfig(checkpoint_at_end=True),
        ),
    )

    results = tuner.fit()

    # Get the best result based on a particular metric.
    best_result = results.get_best_result(
        metric="env_runners/episode_return_mean", mode="max"
    )

    # Get the best checkpoint corresponding to the best result.
    best_checkpoint = best_result.checkpoint
    # __rllib-tuner-end__


# __rllib-compute-action-begin__
# Note: `gymnasium` (not `gym`) will be **the** API supported by RLlib from Ray 2.3 on.
try:
    import gymnasium as gym

    gymnasium = True
except Exception:
    import gym

    gymnasium = False

from ray.rllib.algorithms.ppo import PPOConfig

env_name = "CartPole-v1"
env = gym.make(env_name)
algo = PPOConfig().environment(env_name).build()

episode_reward = 0
terminated = truncated = False

if gymnasium:
    obs, info = env.reset()
else:
    obs = env.reset()

while not terminated and not truncated:
    action = algo.compute_single_action(obs)
    if gymnasium:
        obs, reward, terminated, truncated, info = env.step(action)
    else:
        obs, reward, terminated, info = env.step(action)
    episode_reward += reward
# __rllib-compute-action-end__

# __rllib-get-state-begin__
from ray.rllib.algorithms.dqn import DQNConfig

algo = DQNConfig().environment(env="CartPole-v1").build()

# Get weights of the default local policy
algo.get_policy().get_weights()

# Same as above
algo.env_runner.policy_map["default_policy"].get_weights()

# Get list of weights of each worker, including remote replicas
algo.env_runner_group.foreach_worker(
    lambda env_runner: env_runner.get_policy().get_weights()
)

# Same as above, but with index.
algo.env_runner_group.foreach_worker_with_id(
    lambda _id, worker: worker.get_policy().get_weights()
)
# __rllib-get-state-end__
