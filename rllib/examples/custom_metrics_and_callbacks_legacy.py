"""Deprecated API; see custom_metrics_and_callbacks.py instead."""

import argparse
import numpy as np
import os

import ray
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray import tune


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stop-iters", type=int, default=2000)
    args = parser.parse_args()

    ray.init()

    class MyCallbacks(DefaultCallbacks):
        def on_episode_start(self, *, worker, base_env, policies, episode, **kwargs):
            print("episode {} started".format(episode.episode_id))
            episode.user_data["pole_angles"] = []
            episode.hist_data["pole_angles"] = []

        def on_episode_step(
            self, *, worker, base_env, policies, episode=None, **kwargs
        ):
            pole_angle = abs(episode.last_observation_for()[2])
            raw_angle = abs(episode.last_raw_obs_for()[2])
            assert pole_angle == raw_angle
            episode.user_data["pole_angles"].append(pole_angle)

        def on_episode_end(self, *, worker, base_env, policies, episode, **kwargs):
            pole_angle = np.mean(episode.user_data["pole_angles"])
            print(
                "episode {} ended with length {} and pole angles {}".format(
                    episode.episode_id, episode.length, pole_angle
                )
            )
            episode.custom_metrics["pole_angle"] = pole_angle
            episode.hist_data["pole_angles"] = episode.user_data["pole_angles"]

        def on_sample_end(self, *, worker, samples, **kwargs):
            print("returned sample batch of size {}".format(samples.count))

        def on_postprocess_trajectory(
            self,
            *,
            worker,
            episode,
            agent_id,
            policy_id,
            policies,
            postprocessed_batch,
            original_batches,
            **kwargs
        ):
            print("postprocessed {} steps".format(postprocessed_batch.count))
            if "num_batches" not in episode.custom_metrics:
                episode.custom_metrics["num_batches"] = 0
            episode.custom_metrics["num_batches"] += 1

        def on_train_result(self, *, trainer, result, **kwargs):
            print(
                "trainer.train() result: {} -> {} episodes".format(
                    trainer, result["episodes_this_iter"]
                )
            )
            # you can mutate the result dict to add new fields to return
            result["callback_ok"] = True

    trials = tune.run(
        "PG",
        stop={
            "training_iteration": args.stop_iters,
        },
        config={
            "env": "CartPole-v0",
            "callbacks": MyCallbacks,
            "framework": "tf",
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        },
    ).trials

    # verify custom metrics for integration tests
    custom_metrics = trials[0].last_result["custom_metrics"]
    print(custom_metrics)
    assert "pole_angle_mean" in custom_metrics
    assert "pole_angle_min" in custom_metrics
    assert "pole_angle_max" in custom_metrics
    assert "num_batches_mean" in custom_metrics
    assert "callback_ok" in trials[0].last_result
