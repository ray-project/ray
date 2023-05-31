from rllib_a3c.a3c import A3C, A3CConfig

import ray
from ray import air, tune

if __name__ == "__main__":
    ray.init()

    config = (
        A3CConfig()
        .rollouts(num_rollout_workers=1)
        .framework("torch")
        .environment("CartPole-v1")
        .training(
            gamma=0.95,
        )
    )

    num_iterations = 100

    tuner = tune.Tuner(
        A3C,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"episode_reward_mean": 150, "timesteps_total": 200000},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
