import numpy as np
from redq import REDQ

import ray
from ray import air, tune
from ray.rllib.algorithms.sac import SAC

if __name__ == "__main__":
    # Debugging the target entropy of discrete SAC / REDQ
    max_concurrent_trials, num_samples, num_gpus = 1, 1, 1
    ray.init(num_gpus=num_gpus, local_mode=True)
    stop = {"timesteps_total": 50000}
    params = {
        "num_gpus": num_gpus / float(max_concurrent_trials),
        "env": "CartPole-v0",
        "gamma": 0.99,
        "tau": 0.005,
        "train_batch_size": 32,
        "target_network_update_freq": 1,
        "num_steps_sampled_before_learning_starts": 500,
        "q_fcn_aggregator": "min",
        "optimization": {
            "actor_learning_rate": 0.005,
            "critic_learning_rate": 0.005,
            "entropy_learning_rate": 0.0005,
        },
        # These params start off randomly drawn from a set.
        "seed": tune.choice([42, 43, 44, 45, 46, 47, 48, 49, 50]),
    }

    for idx, algo in enumerate([REDQ, REDQ, SAC]):
        if idx == 1:
            params.update(
                {"target_entropy": -0.98 * np.array(np.log(1 / 2.0), dtype=np.float32)}
            )
        else:
            params.update({"target_entropy": "auto"})
        if idx == 2:
            del params["q_fcn_aggregator"]
        tuner = tune.Tuner(
            algo,
            tune_config=tune.TuneConfig(
                metric="episode_reward_mean",
                mode="max",
                scheduler=None,
                num_samples=num_samples,
                max_concurrent_trials=max_concurrent_trials,
            ),
            param_space=params,
            run_config=air.RunConfig(stop=stop),
        )
        results = tuner.fit()
