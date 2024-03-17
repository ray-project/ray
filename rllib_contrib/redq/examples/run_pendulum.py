import pprint

from redq import REDQ

import ray
from ray import air, tune

if __name__ == "__main__":

    max_concurrent_trials, num_samples, num_gpus = 1, 1, 1
    ray.init(num_gpus=num_gpus)
    stop = {"timesteps_total": 30000}
    params = {
        "num_gpus": num_gpus / float(max_concurrent_trials),
        "env": "Pendulum-v1",
        "framework": "torch",
        "q_model_config": {"fcnet_activation": "relu", "fcnet_hiddens": [256, 256]},
        "policy_model_config": {
            "fcnet_activation": "relu",
            "fcnet_hiddens": [256, 256],
        },
        "tau": 0.005,
        "target_entropy": "auto",
        "n_step": 1,
        "rollout_fragment_length": 1,
        "train_batch_size": 256,
        "ensemble_size": 4,
        "q_fcn_aggregator": "min",
        "target_network_update_freq": 1,
        "min_sample_timesteps_per_iteration": 1000,
        "replay_buffer_config": {"type": "MultiAgentPrioritizedReplayBuffer"},
        "num_steps_sampled_before_learning_starts": 256,
        "optimization": {
            "actor_learning_rate": 0.0003,
            "critic_learning_rate": 0.0003,
            "entropy_learning_rate": 0.0001,
        },
        "metrics_num_episodes_for_smoothing": 5,
        # These params start off randomly drawn from a set.
        "seed": tune.choice([42, 43, 44, 45, 46, 47, 48, 49, 50]),
    }

    tuner = tune.Tuner(
        REDQ,
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

    best_result = results.get_best_result()

    print("\nBest performing trial's final reported metrics:\n")

    metrics_to_print = [
        "episode_reward_mean",
        "episode_reward_max",
        "episode_reward_min",
        "episode_len_mean",
    ]
    pprint.pprint(
        {k: v for k, v in best_result.metrics.items() if k in metrics_to_print}
    )
