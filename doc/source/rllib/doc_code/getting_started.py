# flake8: noqa

if False:
    # __rllib-tune-config-begin__
    from ray import train, tune

    config = (
        PPOConfig()
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




del rl_module


# __rllib-get-state-begin__
from ray.rllib.algorithms.ppo import PPOConfig

algo = (
    PPOConfig()
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
