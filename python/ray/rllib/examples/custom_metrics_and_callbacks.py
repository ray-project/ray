"""Example of using RLlib's debug callbacks.

Here we use callbacks to track the average CartPole pole angle magnitude as a
custom metric.
"""

import argparse
import numpy as np

import ray
from ray import tune


def on_episode_start(info):
    episode = info["episode"]
    print("episode {} started".format(episode.episode_id))
    episode.pole_angles = []


def on_episode_step(info):
    episode = info["episode"]
    pole_angle = abs(episode.last_observation_for()[2])
    episode.pole_angles.append(pole_angle)


def on_episode_end(info):
    episode = info["episode"]
    mean_pole_angle = np.mean(episode.pole_angles)
    print("episode {} ended with length {} and pole angles {}".format(
        episode.episode_id, episode.length, mean_pole_angle))
    episode.custom_metrics["mean_pole_angle"] = mean_pole_angle


def on_sample_end(info):
    print("returned sample batch of size {}".format(info["samples"].count))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-iters", type=int, default=2000)
    args = parser.parse_args()

    ray.init()
    tune.run_experiments({
        "test": {
            "env": "CartPole-v0",
            "run": "PG",
            "stop": {
                "training_iteration": args.num_iters,
            },
            "config": {
                "callbacks": {
                    "on_episode_start": tune.function(on_episode_start),
                    "on_episode_step": tune.function(on_episode_step),
                    "on_episode_end": tune.function(on_episode_end),
                    "on_sample_end": tune.function(on_sample_end),
                },
            },
        }
    })
