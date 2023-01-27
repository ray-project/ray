from pathlib import Path
import os
import unittest
from typing import Dict

import gymnasium as gym
import numpy as np

import ray
from ray.rllib import SampleBatch
from ray.rllib.algorithms.dt import DTConfig
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_train_results

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def _assert_input_dict_equals(d1: Dict[str, np.ndarray], d2: Dict[str, np.ndarray]):
    for key in d1.keys():
        assert key in d2.keys()

    for key in d2.keys():
        assert key in d1.keys()

    for key in d1.keys():
        assert isinstance(d1[key], np.ndarray)
        assert isinstance(d2[key], np.ndarray)
        assert d1[key].shape == d2[key].shape
        assert np.allclose(d1[key], d2[key])


class TestDT(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dt_compilation(self):
        """Test whether a DT algorithm can be built with all supported frameworks."""

        rllib_dir = Path(__file__).parent.parent.parent.parent
        data_file = os.path.join(rllib_dir, "tests/data/pendulum/large.json")

        input_config = {
            "paths": data_file,
            "format": "json",
        }

        config = (
            DTConfig()
            .environment(
                env="Pendulum-v1",
                clip_actions=True,
                normalize_actions=True,
            )
            .framework("torch")
            .offline_data(
                input_="dataset",
                input_config=input_config,
                actions_in_input_normalized=True,
            )
            .training(
                train_batch_size=200,
                replay_buffer_config={
                    "capacity": 8,
                },
                model={
                    "max_seq_len": 4,
                },
                num_layers=1,
                num_heads=1,
                embed_dim=64,
                horizon=200,
            )
            .evaluation(
                target_return=-120,
                evaluation_interval=2,
                evaluation_num_workers=0,
                evaluation_duration=10,
                evaluation_duration_unit="episodes",
                evaluation_parallel_to_training=False,
                evaluation_config=DTConfig.overrides(input_="sampler", explore=False),
            )
            .rollouts(
                num_rollout_workers=0,
            )
            .reporting(
                min_train_timesteps_per_iteration=10,
            )
            .experimental(
                _disable_preprocessor_api=True,
            )
        )

        num_iterations = 4

        for _ in ["torch"]:
            algo = config.build()
            # check if 4 iterations raises any errors
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
                if (i + 1) % 2 == 0:
                    # evaluation happens every 2 iterations
                    eval_results = results["evaluation"]
                    print(
                        f"iter={algo.iteration} "
                        f"R={eval_results['episode_reward_mean']}"
                    )

            # do example inference rollout
            env = gym.make("Pendulum-v1")

            obs, _ = env.reset()
            input_dict = algo.get_initial_input_dict(obs)

            for _ in range(200):
                action, _, extra = algo.compute_single_action(input_dict=input_dict)
                obs, reward, terminated, truncated, _ = env.step(action)
                if terminated or truncated:
                    break
                else:
                    input_dict = algo.get_next_input_dict(
                        input_dict,
                        action,
                        reward,
                        obs,
                        extra,
                    )

            env.close()
            algo.stop()

    def test_inference_methods(self):
        """Test inference methods."""

        config = (
            DTConfig()
            .environment(
                env="Pendulum-v1",
                clip_actions=True,
                normalize_actions=True,
            )
            .framework("torch")
            .training(
                train_batch_size=200,
                replay_buffer_config={
                    "capacity": 8,
                },
                model={
                    "max_seq_len": 3,
                },
                num_layers=1,
                num_heads=1,
                embed_dim=64,
                horizon=200,
            )
            .evaluation(
                target_return=-120,
            )
            .rollouts(
                num_rollout_workers=0,
            )
            .experimental(_disable_preprocessor_api=True)
        )
        algo = config.build()

        # Do a controlled fake rollout for 2 steps and check input_dict
        # first input_dict
        obs = np.array([0.0, 1.0, 2.0])

        input_dict = algo.get_initial_input_dict(obs)
        target = SampleBatch(
            {
                SampleBatch.OBS: np.array(
                    [
                        [0.0, 0.0, 0.0],
                        [0.0, 0.0, 0.0],
                        [0.0, 1.0, 2.0],
                    ],
                    dtype=np.float32,
                ),
                SampleBatch.ACTIONS: np.array([[0.0], [0.0]], dtype=np.float32),
                SampleBatch.RETURNS_TO_GO: np.array([0.0, 0.0], dtype=np.float32),
                SampleBatch.REWARDS: np.zeros((), dtype=np.float32),
                SampleBatch.T: np.array([-1, -1], dtype=np.int32),
            }
        )
        _assert_input_dict_equals(input_dict, target)

        # forward pass with first input_dict
        action, _, extra = algo.compute_single_action(input_dict=input_dict)
        assert action.shape == (1,)
        assert SampleBatch.RETURNS_TO_GO in extra
        assert np.isclose(extra[SampleBatch.RETURNS_TO_GO], -120.0)

        # second input_dict
        action = np.array([0.5])
        obs = np.array([3.0, 4.0, 5.0])
        reward = -10.0

        input_dict = algo.get_next_input_dict(
            input_dict,
            action,
            reward,
            obs,
            extra,
        )
        target = SampleBatch(
            {
                SampleBatch.OBS: np.array(
                    [
                        [0.0, 0.0, 0.0],
                        [0.0, 1.0, 2.0],
                        [3.0, 4.0, 5.0],
                    ],
                    dtype=np.float32,
                ),
                SampleBatch.ACTIONS: np.array([[0.0], [0.5]], dtype=np.float32),
                SampleBatch.RETURNS_TO_GO: np.array([0.0, -120.0], dtype=np.float32),
                SampleBatch.REWARDS: np.asarray(-10.0),
                SampleBatch.T: np.array([-1, 0], dtype=np.int32),
            }
        )
        _assert_input_dict_equals(input_dict, target)

        # forward pass with second input_dict
        action, _, extra = algo.compute_single_action(input_dict=input_dict)
        assert action.shape == (1,)
        assert SampleBatch.RETURNS_TO_GO in extra
        assert np.isclose(extra[SampleBatch.RETURNS_TO_GO], -110.0)

        # third input_dict
        action = np.array([-0.2])
        obs = np.array([6.0, 7.0, 8.0])
        reward = -20.0

        input_dict = algo.get_next_input_dict(
            input_dict,
            action,
            reward,
            obs,
            extra,
        )
        target = SampleBatch(
            {
                SampleBatch.OBS: np.array(
                    [
                        [0.0, 1.0, 2.0],
                        [3.0, 4.0, 5.0],
                        [6.0, 7.0, 8.0],
                    ],
                    dtype=np.float32,
                ),
                SampleBatch.ACTIONS: np.array([[0.5], [-0.2]], dtype=np.float32),
                SampleBatch.RETURNS_TO_GO: np.array([-120, -110.0], dtype=np.float32),
                SampleBatch.REWARDS: np.asarray(-20.0),
                SampleBatch.T: np.array([0, 1], dtype=np.int32),
            }
        )
        _assert_input_dict_equals(input_dict, target)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
