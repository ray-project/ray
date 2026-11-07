"""CPU optimizer state loaded through NumPy must belong to the Learner.

Regression for Ray's CPU torch.from_numpy -> Optimizer.load_state_dict path:
in-place Adam updates must not mutate checkpoint sender or shared Ray object
store data. The test uses real RLlib Learner weights/Adam state and gradients.
"""

import copy

import gymnasium as gym
import numpy as np
import torch

from ray.rllib.core import COMPONENT_OPTIMIZER
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig
from ray.rllib.utils.test_utils import get_cartpole_dataset_reader


def _numeric_optimizer_arrays(state):
    arrays = {}
    for optimizer_name, entry in state[COMPONENT_OPTIMIZER].items():
        for pidx, buffers in entry["state"]["state"].items():
            for key, value in buffers.items():
                if isinstance(value, np.ndarray):
                    arrays[(str(optimizer_name), str(pidx), str(key))] = value
    return arrays


def test_torch_cpu_restored_optimizer_buffers_own_storage():
    env = gym.make("CartPole-v1")
    try:
        config = BaseTestingAlgorithmConfig()
        learner = config.build_learner(env=env)
        reader = get_cartpole_dataset_reader(batch_size=32)
        batch = reader.next().as_multi_agent()
        learner.update(batch=learner._convert_batch_type(copy.deepcopy(batch)))

        checkpoint = copy.deepcopy(learner.get_state())
        source_arrays = _numeric_optimizer_arrays(checkpoint)
        assert len(source_arrays) >= 8, len(source_arrays)
        source_bytes = {
            key: array.tobytes() for key, array in source_arrays.items()
        }

        restored = config.build_learner(env=env)
        restored.set_state(checkpoint)
        for optimizer_name, optimizer in restored._named_optimizers.items():
            loaded = optimizer.state_dict()["state"]
            for param_idx, buffers in loaded.items():
                for key, tensor in buffers.items():
                    lookup = (str(optimizer_name), str(param_idx), str(key))
                    if lookup not in source_arrays:
                        continue
                    assert isinstance(tensor, torch.Tensor), lookup
                    source_ptr = source_arrays[lookup].__array_interface__["data"][0]
                    assert tensor.data_ptr() != source_ptr, (
                        "optimizer buffer aliases input checkpoint",
                        lookup,
                    )
                    np.testing.assert_array_equal(
                        tensor.detach().cpu().numpy(), source_arrays[lookup]
                    )

        restored.update(
            batch=restored._convert_batch_type(copy.deepcopy(batch))
        )
        assert all(
            source_arrays[key].tobytes() == previous
            for key, previous in source_bytes.items()
        ), "in-place Adam step mutated checkpoint sender's numpy buffers"
    finally:
        env.close()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
