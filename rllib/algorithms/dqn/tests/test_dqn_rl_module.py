import dataclasses

import numpy as np
import pytest
import tree
from gymnasium.spaces import Box, Dict, Discrete

from ray.rllib.algorithms.dqn.dqn_catalog import DQNCatalog
from ray.rllib.algorithms.dqn.torch.default_dqn_torch_rl_module import (
    DefaultDQNTorchRLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, nn = try_import_torch()


# Custom encoder, config and catalog to test Dict observation spaces.
# RLlib does not build encoders for Dict observation spaces out of the box so we define our own.
class DictFlattenEncoder(nn.Module):
    def __init__(self, obs_space, output_dim=64):
        super().__init__()
        total_dim = sum(
            int(np.prod(space.shape)) for space in obs_space.spaces.values()
        )
        self.net = nn.Sequential(
            nn.Linear(total_dim, output_dim),
            nn.ReLU(),
        )

    def forward(self, inputs):
        obs = inputs[Columns.OBS]
        flat_obs = torch.cat(
            [obs[k].reshape(obs[k].shape[0], -1) for k in sorted(obs.keys())],
            dim=-1,
        )
        return {ENCODER_OUT: self.net(flat_obs)}


class DictEncoderConfig:
    def __init__(self, obs_space, output_dim=64):
        self.obs_space = obs_space
        self.output_dims = (output_dim,)

    def build(self, framework):
        return DictFlattenEncoder(self.obs_space, output_dim=self.output_dims[0])


class DictObsDQNCatalog(DQNCatalog):
    @classmethod
    def _get_encoder_config(
        cls, observation_space, model_config_dict, action_space=None
    ):
        return DictEncoderConfig(observation_space, output_dim=64)


# Observation space definitions.
OBS_SPACES = {
    "box": Box(low=-1.0, high=1.0, shape=(8,), dtype=np.float32),
    "image": Box(low=0, high=255, shape=(64, 64, 3), dtype=np.uint8),
    "dict": Dict(
        {
            "sensors": Box(low=-1.0, high=1.0, shape=(4,), dtype=np.float32),
            "position": Box(low=-10.0, high=10.0, shape=(3,), dtype=np.float32),
            "mode": Discrete(4),
        }
    ),
}


def _get_dqn_module(observation_space, action_space, **config_overrides):
    model_config = dataclasses.asdict(DefaultModelConfig())
    model_config.update(
        {
            "double_q": True,
            "dueling": True,
            "epsilon": [(0, 1.0), (10000, 0.05)],
            "num_atoms": 1,
            "v_min": -10.0,
            "v_max": 10.0,
        }
    )
    model_config.update(config_overrides)

    # Use custom catalog for Dict observation spaces.
    catalog_class = (
        DictObsDQNCatalog if isinstance(observation_space, Dict) else DQNCatalog
    )

    module = DefaultDQNTorchRLModule(
        observation_space=observation_space,
        action_space=action_space,
        model_config=model_config,
        catalog_class=catalog_class,
        inference_only=False,
    )

    # Create target networks (normally done by the learner).
    module.make_target_networks()
    return module


class TestDQNRLModule:
    @pytest.mark.parametrize("obs_space_name", ["box", "image", "dict"])
    @pytest.mark.parametrize("forward_method", ["train", "exploration", "inference"])
    @pytest.mark.parametrize("double_q", [True, False])
    @pytest.mark.parametrize("dueling", [True, False])
    def test_forward(self, obs_space_name, forward_method, double_q, dueling):
        """Test forward methods with different obs spaces and config settings."""
        obs_space = OBS_SPACES[obs_space_name]
        action_space = Discrete(4)

        module = _get_dqn_module(
            obs_space, action_space, double_q=double_q, dueling=dueling
        )

        if (
            forward_method == "train"
        ):  # forward train needs batching, exploration and inference don't
            module.train()
            # Create a batch first
            batch_size = 4
            obs_list = [obs_space.sample() for _ in range(batch_size)]
            next_obs_list = [obs_space.sample() for _ in range(batch_size)]

            obs_batch = tree.map_structure(
                lambda *x: np.stack(x, axis=0, dtype=np.float32), *obs_list
            )
            next_obs_batch = tree.map_structure(
                lambda *x: np.stack(x, axis=0, dtype=np.float32), *next_obs_list
            )

            batch = {
                Columns.OBS: convert_to_torch_tensor(obs_batch),
                Columns.NEXT_OBS: convert_to_torch_tensor(next_obs_batch),
                Columns.ACTIONS: convert_to_torch_tensor(
                    np.array([0] * batch_size, dtype=np.int64)
                ),
                Columns.REWARDS: convert_to_torch_tensor(
                    np.array([1.0] * batch_size, dtype=np.float32)
                ),
                Columns.TERMINATEDS: convert_to_torch_tensor(
                    np.array([False] * batch_size, dtype=np.bool_)
                ),
                Columns.TRUNCATEDS: convert_to_torch_tensor(
                    np.array([False] * batch_size, dtype=np.bool_)
                ),
            }

            # Forward pass and check outputs
            output = module.forward_train(batch)

            assert "qf_preds" in output
            assert output["qf_preds"].shape == (4, action_space.n)

            if double_q:
                assert "qf_next_preds" in output
                assert output["qf_next_preds"].shape == (4, action_space.n)
            else:
                assert "qf_next_preds" not in output
        else:
            module.eval()
            # Create a single observation batch
            obs = obs_space.sample()
            if isinstance(obs_space, Dict):
                obs_tensor = tree.map_structure(
                    lambda x: convert_to_torch_tensor(x.astype(np.float32)[None]),
                    obs,
                )
            else:
                obs_tensor = convert_to_torch_tensor(obs.astype(np.float32)[None])
            batch = {Columns.OBS: obs_tensor}

            # Forward pass and check outputs
            if forward_method == "exploration":
                output = module.forward_exploration(batch, t=0)
            else:
                output = module.forward_inference(batch)

            assert Columns.ACTIONS in output
            assert output[Columns.ACTIONS].shape == (1,)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
