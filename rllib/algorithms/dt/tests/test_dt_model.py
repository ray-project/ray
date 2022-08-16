import unittest

import gym
import numpy as np

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from rllib.algorithms.dt.dt_torch_model import DTTorchModel

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def _assert_outputs_equal(outputs):
    for i in range(1, len(outputs)):
        for key in outputs[0].keys():
            assert np.allclose(
                outputs[0][key], outputs[i][key]
            ), "outputs are different but they shouldn't be."


def _assert_outputs_not_equal(outputs):
    for i in range(1, len(outputs)):
        for key in outputs[0].keys():
            assert not np.allclose(
                outputs[0][key], outputs[i][key]
            ), "some outputs are the same but they shouldn't be."


def _generate_input_dict(B, T, obs_space, action_space):
    """Generate input_dict that has completely fake values."""
    # generate deterministic inputs
    # obs
    obs = np.arange(B * T * obs_space.shape[0], dtype=np.float32).reshape(
        (B, T, obs_space.shape[0])
    )
    # actions
    if isinstance(action_space, gym.spaces.Box):
        act = np.arange(B * T * action_space.shape[0], dtype=np.float32).reshape(
            (B, T, action_space.shape[0])
        )
    else:
        act = np.mod(np.arange(B * T, dtype=np.int32).reshape((B, T)), action_space.n)
    # returns to go
    rtg = np.arange(B * (T + 1), dtype=np.float32).reshape((B, T + 1, 1))
    # timesteps
    timesteps = np.stack([np.arange(T, dtype=np.int32) for _ in range(B)], axis=0)
    # attention mask
    mask = np.ones((B, T), dtype=np.float32)

    input_dict = SampleBatch(
        {
            SampleBatch.OBS: obs,
            SampleBatch.ACTIONS: act,
            SampleBatch.RETURNS_TO_GO: rtg,
            SampleBatch.T: timesteps,
            SampleBatch.ATTENTION_MASKS: mask,
        }
    )
    input_dict = convert_to_torch_tensor(input_dict)
    return input_dict


class TestDTModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_torch_model_init(self):
        """Test models are initialized properly"""
        model_config = {
            "embed_dim": 32,
            "num_layers": 2,
            "max_seq_len": 4,
            "max_ep_len": 10,
            "num_heads": 2,
            "embed_pdrop": 0.1,
            "resid_pdrop": 0.1,
            "attn_pdrop": 0.1,
            "use_obs_output": False,
            "use_return_output": False,
        }

        num_outputs = 2
        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(num_outputs,))

        action_dim = 5
        action_spaces = [
            gym.spaces.Box(-1.0, 1.0, shape=(action_dim,)),
            gym.spaces.Discrete(action_dim),
        ]

        B, T = 3, 4

        for action_space in action_spaces:
            # Generate input dict.
            input_dict = _generate_input_dict(B, T, observation_space, action_space)

            # Do random initialization a few times and make sure outputs are different
            outputs = []
            for _ in range(10):
                model = DTTorchModel(
                    observation_space,
                    action_space,
                    num_outputs,
                    model_config,
                    "model",
                )
                # so dropout is not in effect
                model.eval()
                model_out, _ = model(input_dict)
                output = model.get_prediction(model_out, input_dict)
                outputs.append(convert_to_numpy(output))
            _assert_outputs_not_equal(outputs)

            # Initialize once and make sure dropout is working
            model = DTTorchModel(
                observation_space,
                action_space,
                num_outputs,
                model_config,
                "model",
            )

            # Dropout should make outputs different in training mode
            model.train()
            outputs = []
            for _ in range(10):
                model_out, _ = model(input_dict)
                output = model.get_prediction(model_out, input_dict)
                outputs.append(convert_to_numpy(output))
            _assert_outputs_not_equal(outputs)

            # Dropout should make outputs the same in eval mode
            model.eval()
            outputs = []
            for _ in range(10):
                model_out, _ = model(input_dict)
                output = model.get_prediction(model_out, input_dict)
                outputs.append(convert_to_numpy(output))
            _assert_outputs_equal(outputs)

    def test_torch_model_prediction_target(self):
        """Test the get_prediction and get_targets function."""
        model_config = {
            "embed_dim": 16,
            "num_layers": 3,
            "max_seq_len": 3,
            "max_ep_len": 9,
            "num_heads": 1,
            "embed_pdrop": 0.2,
            "resid_pdrop": 0.2,
            "attn_pdrop": 0.2,
            "use_obs_output": True,
            "use_return_output": True,
        }

        num_outputs = 5
        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(num_outputs,))

        action_dim = 2
        action_spaces = [
            gym.spaces.Box(-1.0, 1.0, shape=(action_dim,)),
            gym.spaces.Discrete(action_dim),
        ]

        B, T = 2, 3

        for action_space in action_spaces:
            # Generate input dict.
            input_dict = _generate_input_dict(B, T, observation_space, action_space)

            # Make model and forward pass.
            model = DTTorchModel(
                observation_space,
                action_space,
                num_outputs,
                model_config,
                "model",
            )
            model_out, _ = model(input_dict)
            preds = model.get_prediction(model_out, input_dict)
            target = model.get_targets(model_out, input_dict)

            preds = convert_to_numpy(preds)
            target = convert_to_numpy(target)

            # Test the content and shape of output and target
            if isinstance(action_space, gym.spaces.Box):
                # test preds shape
                self.assertEqual(preds[SampleBatch.ACTIONS].shape, (B, T, action_dim))
                # test target shape and content
                self.assertEqual(target[SampleBatch.ACTIONS].shape, (B, T, action_dim))
                assert np.allclose(
                    target[SampleBatch.ACTIONS],
                    input_dict[SampleBatch.ACTIONS],
                )
            else:
                # test preds shape
                self.assertEqual(preds[SampleBatch.ACTIONS].shape, (B, T, action_dim))
                # test target shape and content
                self.assertEqual(target[SampleBatch.ACTIONS].shape, (B, T))
                assert np.allclose(
                    target[SampleBatch.ACTIONS],
                    input_dict[SampleBatch.ACTIONS],
                )

            # test preds shape
            self.assertEqual(preds[SampleBatch.OBS].shape, (B, T, num_outputs))
            # test target shape and content
            self.assertEqual(target[SampleBatch.OBS].shape, (B, T, num_outputs))
            assert np.allclose(
                target[SampleBatch.OBS],
                input_dict[SampleBatch.OBS],
            )

            # test preds shape
            self.assertEqual(preds[SampleBatch.RETURNS_TO_GO].shape, (B, T, 1))
            # test target shape and content
            self.assertEqual(target[SampleBatch.RETURNS_TO_GO].shape, (B, T, 1))
            assert np.allclose(
                target[SampleBatch.RETURNS_TO_GO],
                input_dict[SampleBatch.RETURNS_TO_GO][:, 1:, :],
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
