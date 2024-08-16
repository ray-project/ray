# flake8: noqa

# __rllib-first-config-begin__
from pprint import pprint

from ray.rllib.algorithms.ppo import PPOConfig

config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("CartPole-v1")
    .env_runners(num_env_runners=1)
)

algo = config.build()

for i in range(10):
    result = algo.train()
    result.pop("config")
    pprint(result)

    if i % 5 == 0:
        checkpoint_dir = algo.save_to_path()
        print(f"Checkpoint saved in directory {checkpoint_dir}")
# __rllib-first-config-end__

algo.stop()

if False:
    # __rllib-tune-config-begin__
    from ray import train, tune

    config = (
        PPOConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment("CartPole-v1")
        .training(
            lr=tune.grid_search([0.01, 0.001, 0.0001]),
        )
    )

    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=train.RunConfig(
            stop={"env_runners/episode_return_mean": 150.0},
        ),
    )

    tuner.fit()
    # __rllib-tune-config-end__


# __rllib-tuner-begin__
from ray import train, tune

# Tuner.fit() allows setting a custom log directory (other than ~/ray-results).
tuner = tune.Tuner(
    "PPO",
    param_space=config,
    run_config=train.RunConfig(
        stop={"num_env_steps_sampled_lifetime": 20000},
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
import pathlib
import gymnasium as gym
import numpy as np
import torch
from ray.rllib.core.rl_module import RLModule

env = gym.make("CartPole-v1")

# Create only the neural network (RLModule) from our checkpoint.
rl_module = RLModule.from_checkpoint(
    pathlib.Path(best_checkpoint.path) / "learner_group" / "learner" / "rl_module"
)["default_policy"]

episode_return = 0
terminated = truncated = False

obs, info = env.reset()

while not terminated and not truncated:
    # Compute the next action from a batch (B=1) of observations.
    torch_obs_batch = torch.from_numpy(np.array([obs]))
    action_logits = rl_module.forward_inference({"obs": torch_obs_batch})[
        "action_dist_inputs"
    ]
    # The default RLModule used here produces action logits (from which
    # we'll have to sample an action or use the max-likelihood one).
    action = torch.argmax(action_logits[0]).numpy()
    obs, reward, terminated, truncated, info = env.step(action)
    episode_return += reward

print(f"Reached episode return of {episode_return}.")
# __rllib-compute-action-end__


del rl_module


# __rllib-get-state-begin__
from ray.rllib.algorithms.ppo import PPOConfig

algo = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("CartPole-v1")
    .env_runners(num_env_runners=2)
).build()

# Get weights of the algo's RLModule.
algo.get_module().get_state()

# Same as above
algo.env_runner.module.get_state()

# Get list of weights of each EnvRunner, including remote replicas.
algo.env_runner_group.foreach_worker(lambda env_runner: env_runner.module.get_state())

# Same as above, but with index.
algo.env_runner_group.foreach_worker_with_id(
    lambda _id, env_runner: env_runner.module.get_state()
)
# __rllib-get-state-end__

algo.stop()
