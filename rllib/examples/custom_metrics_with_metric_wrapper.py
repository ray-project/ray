"""Example of using RLlib's debug callbacks with
Metric implementations and MetricWrapper

Here we use callbacks to track the average CartPole pole angle magnitude as a
custom metric.
"""

import argparse
from typing import Dict

import numpy as np

import ray
from ray import tune

from rllib.metrics.metric import Metric
from rllib.metrics.metric_wrapper import MetricsWrapper


class PoleAngle(Metric):
    @staticmethod
    def on_episode_start(info: Dict):
        episode = info["episode"]
        episode.user_data["pole_angles"] = []

    @staticmethod
    def on_episode_step(info: Dict):
        episode = info["episode"]
        pole_angle = abs(episode.last_observation_for()[2])
        episode.user_data["pole_angles"].append(pole_angle)

    @staticmethod
    def on_episode_end(info: Dict):
        episode = info["episode"]
        pole_angle = np.mean(episode.user_data["pole_angles"])
        episode.custom_metrics["pole_angle"] = pole_angle


class NumBatches(Metric):
    @staticmethod
    def on_postprocess_traj(info: Dict):
        episode = info["episode"]
        if "num_batches" not in episode.custom_metrics:
            episode.custom_metrics["num_batches"] = 0
        episode.custom_metrics["num_batches"] += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-iters", type=int, default=2000)
    args = parser.parse_args()

    metric_wrapper = MetricsWrapper([PoleAngle(), NumBatches()])

    ray.init(local_mode=True)
    trials = tune.run(
        "PG",
        stop={
            "training_iteration": args.num_iters,
        },
        config={
            "env": "CartPole-v0",
            "callbacks": metric_wrapper.to_dict(),
        },
        return_trials=True)

    # verify custom metrics for integration tests
    custom_metrics = trials[0].last_result["custom_metrics"]
    print(custom_metrics)
    assert "pole_angle_mean" in custom_metrics
    assert "pole_angle_min" in custom_metrics
    assert "pole_angle_max" in custom_metrics
    assert "num_batches_mean" in custom_metrics
    assert "callback_ok" in trials[0].last_result
