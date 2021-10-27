#!/usr/bin/env python3
""" Benchmarking TF against PyTorch on an example task using Ray Tune.
"""

import logging
from pprint import pformat

import ray
from ray import tune

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("tune_framework")


def run():
    config = {
        "env": "PongNoFrameskip-v4",
        "framework": tune.grid_search(["tf", "torch"]),
        "num_gpus": 1,
        "rollout_fragment_length": 50,
        "train_batch_size": 750,
        "num_workers": 50,
        "num_envs_per_worker": 5,
        "clip_rewards": True,
        "num_sgd_iter": 2,
        "vf_loss_coeff": 1.0,
        "clip_param": 0.3,
        "grad_clip": 10,
        "vtrace": True,
        "use_kl_loss": False,
    }
    logger.info("Configuration: \n %s", pformat(config))

    # Run the experiment.
    # TODO(jungong) : maybe add checkpointing.
    return tune.run(
        "APPO",
        config=config,
        stop={
            "episode_reward_mean": 18.0,
            "timesteps_total": 5000000,
            "time_total_s": 3000,
        },
        verbose=1,
        num_samples=2)


if __name__ == "__main__":
    ray.init()

    logger.info(run())

    ray.shutdown()
