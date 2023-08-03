from ray import air, tune
from ray.rllib.algorithms.ppo import PPO

tuner = tune.Tuner(
    PPO,
    run_config=air.RunConfig(
        stop={"episode_len_mean": 20},
    ),
    param_space={"env": "CartPole-v1", "framework": "torch", "log_level": "INFO"},
)
tuner.fit()
