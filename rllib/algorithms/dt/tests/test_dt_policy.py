import unittest
from typing import Dict

import gym
import numpy as np

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.algorithms.dt.dt_torch_policy import DTTorchPolicy

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


def _default_config():
    """Base config to use."""
    return {
        "model": {
            "max_seq_len": 4,
        },
        "embed_dim": 32,
        "num_layers": 2,
        "horizon": 10,
        "num_heads": 2,
        "embed_pdrop": 0.1,
        "resid_pdrop": 0.1,
        "attn_pdrop": 0.1,
        "framework": "torch",
        "lr": 1e-3,
        "lr_schedule": None,
        "optimizer": {
            "weight_decay": 1e-4,
            "betas": [0.9, 0.99],
        },
        "target_return": 200.0,
        "loss_coef_actions": 1.0,
        "loss_coef_obs": 0,
        "loss_coef_returns_to_go": 0,
        "num_gpus": 0,
        "_fake_gpus": None,
        "_enable_rl_module_api": False,
    }


def _assert_input_dict_equals(d1: Dict[str, np.ndarray], d2: Dict[str, np.ndarray]):
    for key in d1.keys():
        assert key in d2.keys()

    for key in d2.keys():
        assert key in d1.keys()

    for key in d1.keys():
        assert isinstance(d1[key], np.ndarray), "input_dict should only be numpy array."
        assert isinstance(d2[key], np.ndarray), "input_dict should only be numpy array."
        assert d1[key].shape == d2[key].shape, "input_dict are of different shape."
        assert np.allclose(d1[key], d2[key]), "input_dict values are not equal."


class TestDTPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_torch_postprocess_trajectory(self):
        """Test postprocess_trajectory"""
        config = _default_config()

        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(4,))
        action_space = gym.spaces.Box(-1.0, 1.0, shape=(3,))

        # Create policy
        policy = DTTorchPolicy(observation_space, action_space, config)

        # Generate input_dict with some data
        sample_batch = SampleBatch(
            {
                SampleBatch.REWARDS: np.array([1.0, 2.0, 1.0, 1.0]),
                SampleBatch.EPS_ID: np.array([0, 0, 0, 0]),
            }
        )

        # Do postprocess trajectory to calculate rtg
        sample_batch = policy.postprocess_trajectory(sample_batch)

        # Assert that dones is correctly set
        assert SampleBatch.DONES in sample_batch, "`dones` isn't part of the batch."
        assert np.allclose(
            sample_batch[SampleBatch.DONES],
            np.array([False, False, False, True]),
        ), "`dones` isn't set correctly."

    def test_torch_input_dict(self):
        """Test inference input_dict methods

        This is a minimal version the test in test_dt.py.
        The shapes of the input_dict might be confusing but it makes sense in
        context of what the function is supposed to do.
        Check action_distribution_fn for an explanation.
        """
        config = _default_config()

        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(3,))
        action_spaces = [
            gym.spaces.Box(-1.0, 1.0, shape=(1,)),
            gym.spaces.Discrete(4),
        ]

        for action_space in action_spaces:
            # Create policy
            policy = DTTorchPolicy(observation_space, action_space, config)

            # initial obs and input_dict
            obs = np.array([0.0, 1.0, 2.0])
            input_dict = policy.get_initial_input_dict(obs)

            # Check input_dict matches what it should be
            target_input_dict = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [0.0, 0.0, 0.0],
                            [0.0, 0.0, 0.0],
                            [0.0, 0.0, 0.0],
                            [0.0, 1.0, 2.0],
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[0.0], [0.0], [0.0]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([0, 0, 0], dtype=np.int32)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [0.0, 0.0, 0.0], dtype=np.float32
                    ),
                    SampleBatch.REWARDS: np.zeros((), dtype=np.float32),
                    SampleBatch.T: np.array([-1, -1, -1], dtype=np.int32),
                }
            )
            _assert_input_dict_equals(input_dict, target_input_dict)

            # Get next input_dict
            input_dict = policy.get_next_input_dict(
                input_dict,
                action=(
                    np.asarray([1.0], dtype=np.float32)
                    if isinstance(action_space, gym.spaces.Box)
                    else np.asarray(1, dtype=np.int32)
                ),
                reward=1.0,
                next_obs=np.array([3.0, 4.0, 5.0]),
                extra={
                    SampleBatch.RETURNS_TO_GO: config["target_return"],
                },
            )

            # Check input_dict matches what it should be
            target_input_dict = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [0.0, 0.0, 0.0],
                            [0.0, 0.0, 0.0],
                            [0.0, 1.0, 2.0],
                            [3.0, 4.0, 5.0],
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[0.0], [0.0], [1.0]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([0, 0, 1], dtype=np.int32)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [0.0, 0.0, config["target_return"]], dtype=np.float32
                    ),
                    SampleBatch.REWARDS: np.asarray(1.0, dtype=np.float32),
                    SampleBatch.T: np.array([-1, -1, 0], dtype=np.int32),
                }
            )
            _assert_input_dict_equals(input_dict, target_input_dict)

    def test_torch_action(self):
        """Test policy's action_distribution_fn and extra_action_out methods by
        calling compute_actions_from_input_dict which works those two methods
        in conjunction.
        """
        config = _default_config()

        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(3,))
        action_spaces = [
            gym.spaces.Box(-1.0, 1.0, shape=(1,)),
            gym.spaces.Discrete(4),
        ]

        for action_space in action_spaces:
            # Create policy
            policy = DTTorchPolicy(observation_space, action_space, config)

            # input_dict for initial observation
            input_dict = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [
                                [0.0, 0.0, 0.0],
                                [0.0, 0.0, 0.0],
                                [0.0, 0.0, 0.0],
                                [0.0, 1.0, 2.0],
                            ]
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[[0.0], [0.0], [0.0]]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([[0, 0, 0]], dtype=np.int32)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [[0.0, 0.0, 0.0]], dtype=np.float32
                    ),
                    SampleBatch.REWARDS: np.array([0.0], dtype=np.float32),
                    SampleBatch.T: np.array([[-1, -1, -1]], dtype=np.int32),
                }
            )

            # Run compute_actions_from_input_dict
            actions, _, extras = policy.compute_actions_from_input_dict(
                input_dict,
                explore=False,
                timestep=None,
            )

            # Check actions
            assert actions.shape == (
                1,
                *action_space.shape,
            ), "actions has incorrect shape."

            # Check extras
            assert (
                SampleBatch.RETURNS_TO_GO in extras
            ), "extras should contain returns_to_go."
            assert extras[SampleBatch.RETURNS_TO_GO].shape == (
                1,
            ), "extras['returns_to_go'] has incorrect shape."
            assert np.isclose(
                extras[SampleBatch.RETURNS_TO_GO],
                np.asarray([config["target_return"]], dtype=np.float32),
            ), "extras['returns_to_go'] should contain target_return."

            # input_dict for non-initial observation
            input_dict = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [
                                [0.0, 0.0, 0.0],
                                [0.0, 0.0, 0.0],
                                [0.0, 1.0, 2.0],
                                [3.0, 4.0, 5.0],
                            ]
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[[0.0], [0.0], [1.0]]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([[0, 0, 1]], dtype=np.int32)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [[0.0, 0.0, config["target_return"]]], dtype=np.float32
                    ),
                    SampleBatch.REWARDS: np.array([10.0], dtype=np.float32),
                    SampleBatch.T: np.array([[-1, -1, 0]], dtype=np.int32),
                }
            )

            # Run compute_actions_from_input_dict
            actions, _, extras = policy.compute_actions_from_input_dict(
                input_dict,
                explore=False,
                timestep=None,
            )

            # Check actions
            assert actions.shape == (
                1,
                *action_space.shape,
            ), "actions has incorrect shape."

            # Check extras
            assert (
                SampleBatch.RETURNS_TO_GO in extras
            ), "extras should contain returns_to_go."
            assert extras[SampleBatch.RETURNS_TO_GO].shape == (
                1,
            ), "extras['returns_to_go'] has incorrect shape."
            assert np.isclose(
                extras[SampleBatch.RETURNS_TO_GO],
                np.asarray([config["target_return"] - 10.0], dtype=np.float32),
            ), "extras['returns_to_go'] should contain target_return."

    def test_loss(self):
        """Test loss function."""
        config = _default_config()
        config["embed_pdrop"] = 0
        config["resid_pdrop"] = 0
        config["attn_pdrop"] = 0

        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(3,))
        action_spaces = [
            gym.spaces.Box(-1.0, 1.0, shape=(1,)),
            gym.spaces.Discrete(4),
        ]

        for action_space in action_spaces:
            # Create policy
            policy = DTTorchPolicy(observation_space, action_space, config)

            # Run loss functions on batches with different items in the mask to make
            # sure the masks are working and making the loss the same.
            batch1 = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [
                                [0.0, 0.0, 0.0],
                                [0.0, 0.0, 0.0],
                                [0.0, 1.0, 2.0],
                                [3.0, 4.0, 5.0],
                            ]
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[[0.0], [0.0], [1.0], [0.5]]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([[0, 0, 1, 3]], dtype=np.int64)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [[[0.0], [0.0], [100.0], [90.0], [80.0]]], dtype=np.float32
                    ),
                    SampleBatch.T: np.array([[0, 0, 0, 1]], dtype=np.int32),
                    SampleBatch.ATTENTION_MASKS: np.array(
                        [[0.0, 0.0, 1.0, 1.0]], dtype=np.float32
                    ),
                }
            )

            batch2 = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [
                                [1.0, 1.0, -1.0],
                                [1.0, 10.0, 12.0],
                                [0.0, 1.0, 2.0],
                                [3.0, 4.0, 5.0],
                            ]
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[[1.0], [-0.5], [1.0], [0.5]]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([[2, 1, 1, 3]], dtype=np.int64)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [[[200.0], [-10.0], [100.0], [90.0], [80.0]]], dtype=np.float32
                    ),
                    SampleBatch.T: np.array([[9, 3, 0, 1]], dtype=np.int32),
                    SampleBatch.ATTENTION_MASKS: np.array(
                        [[0.0, 0.0, 1.0, 1.0]], dtype=np.float32
                    ),
                }
            )

            loss1 = policy.loss(policy.model, policy.dist_class, batch1)
            loss2 = policy.loss(policy.model, policy.dist_class, batch2)

            loss1 = loss1.detach().cpu().item()
            loss2 = loss2.detach().cpu().item()

            assert np.isclose(loss1, loss2), "Masks are not working for losses."

            # Run loss on a widely different batch and make sure the loss is different.
            batch3 = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [
                                [1.0, 1.0, -20.0],
                                [0.1, 10.0, 12.0],
                                [1.4, 12.0, -9.0],
                                [6.0, 40.0, -2.0],
                            ]
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[[2.0], [-1.5], [0.2], [0.1]]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([[1, 3, 0, 2]], dtype=np.int64)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [[[90.0], [80.0], [70.0], [60.0], [50.0]]], dtype=np.float32
                    ),
                    SampleBatch.T: np.array([[3, 4, 5, 6]], dtype=np.int32),
                    SampleBatch.ATTENTION_MASKS: np.array(
                        [[1.0, 1.0, 1.0, 1.0]], dtype=np.float32
                    ),
                }
            )

            loss3 = policy.loss(policy.model, policy.dist_class, batch3)
            loss3 = loss3.detach().cpu().item()

            assert not np.isclose(
                loss1, loss3
            ), "Widely different inputs are giving the same loss value."

    def test_loss_coef(self):
        """Test the loss_coef_{key} config options."""

        config = _default_config()
        config["embed_pdrop"] = 0
        config["resid_pdrop"] = 0
        config["attn_pdrop"] = 0
        # set initial action coef to 0
        config["loss_coef_actions"] = 0

        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(3,))
        action_spaces = [
            gym.spaces.Box(-1.0, 1.0, shape=(1,)),
            gym.spaces.Discrete(4),
        ]

        for action_space in action_spaces:
            batch = SampleBatch(
                {
                    SampleBatch.OBS: np.array(
                        [
                            [
                                [0.0, 0.0, 0.0],
                                [0.0, 0.0, 0.0],
                                [0.0, 1.0, 2.0],
                                [3.0, 4.0, 5.0],
                            ]
                        ],
                        dtype=np.float32,
                    ),
                    SampleBatch.ACTIONS: (
                        np.array([[[0.0], [0.0], [1.0], [0.5]]], dtype=np.float32)
                        if isinstance(action_space, gym.spaces.Box)
                        else np.array([[0, 0, 1, 3]], dtype=np.int64)
                    ),
                    SampleBatch.RETURNS_TO_GO: np.array(
                        [[[0.0], [0.0], [100.0], [90.0], [80.0]]], dtype=np.float32
                    ),
                    SampleBatch.T: np.array([[0, 0, 0, 1]], dtype=np.int32),
                    SampleBatch.ATTENTION_MASKS: np.array(
                        [[0.0, 0.0, 1.0, 1.0]], dtype=np.float32
                    ),
                }
            )

            keys = [SampleBatch.ACTIONS, SampleBatch.OBS, SampleBatch.RETURNS_TO_GO]
            for key in keys:
                # create policy and run loss with different coefs
                # create policy 1 with coef = 1
                config1 = config.copy()
                config1[f"loss_coef_{key}"] = 1.0
                policy1 = DTTorchPolicy(observation_space, action_space, config1)

                loss1 = policy1.loss(policy1.model, policy1.dist_class, batch)
                loss1 = loss1.detach().cpu().item()

                # create policy 2 with coef = 10
                config2 = config.copy()
                config2[f"loss_coef_{key}"] = 10.0
                policy2 = DTTorchPolicy(observation_space, action_space, config2)
                # Copy the weights over so they output the same loss without scaling.
                policy2.set_weights(policy1.get_weights())

                loss2 = policy2.loss(policy2.model, policy2.dist_class, batch)
                loss2 = loss2.detach().cpu().item()

                # Compare loss, should be factor of 10 difference.
                self.assertAlmostEqual(
                    loss2 / loss1,
                    10.0,
                    places=3,
                    msg="the two losses should be different to a factor of 10.",
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
