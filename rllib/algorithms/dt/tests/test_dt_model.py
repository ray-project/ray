import unittest
from typing import Union, List

import gym
import numpy as np

import ray
from ray.rllib.algorithms.dt.segmentation_buffer import (
    SegmentationBuffer,
    MultiAgentSegmentationBuffer,
)
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    MultiAgentBatch,
    concat_samples,
    DEFAULT_POLICY_ID,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import PolicyID
from rllib.algorithms.dt.dt_torch_model import DTTorchModel

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def _assert_outputs_equal(outputs):
    for i in range(1, len(outputs)):
        for key in outputs[0].keys():
            assert np.allclose(outputs[0][key], outputs[i][key])


def _assert_outputs_not_equal(outputs):
    for i in range(1, len(outputs)):
        for key in outputs[0].keys():
            assert not np.allclose(outputs[0][key], outputs[i][key])


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
        action_space = gym.spaces.Box(-1.0, 1.0, shape=(1,))
        observation_space = gym.spaces.Box(-1.0, 1.0, shape=(2,))
        num_outputs = int(np.prod(observation_space.shape))

        B, T = 3, 4

        input_dict = SampleBatch(
            {
                SampleBatch.OBS: np.arange(B * T * 2, dtype=np.float32).reshape(
                    (B, T, 2)
                ),
                SampleBatch.ACTIONS: np.arange(B * T, dtype=np.float32).reshape(
                    (B, T, 1)
                ),
                SampleBatch.RETURNS_TO_GO: np.arange(3 * 4, dtype=np.float32).reshape(
                    (B, T, 1)
                ),
                SampleBatch.T: np.stack(
                    [np.arange(T, dtype=np.int32) for _ in range(B)], axis=0
                ),
                SampleBatch.ATTENTION_MASKS: np.ones((B, T), dtype=np.float32),
            }
        )
        input_dict = convert_to_torch_tensor(input_dict)

        # do random initialization a few times and make sure outputs are different
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
            output = model.get_prediction(model(input_dict)[0], input_dict)
            outputs.append(convert_to_numpy(output))
        _assert_outputs_not_equal(outputs)

        # initialize once and make sure dropout is working
        model = DTTorchModel(
            observation_space,
            action_space,
            num_outputs,
            model_config,
            "model",
        )

        # dropout should make outputs different in training mode
        model.train()
        outputs = []
        for _ in range(10):
            output = model.get_prediction(model(input_dict)[0], input_dict)
            outputs.append(convert_to_numpy(output))
        _assert_outputs_not_equal(outputs)

        # dropout should make outputs the same in eval mode
        model.eval()
        outputs = []
        for _ in range(10):
            output = model.get_prediction(model(input_dict)[0], input_dict)
            outputs.append(convert_to_numpy(output))
        _assert_outputs_equal(outputs)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
