"""Tests for LayerNorm in encoder models with different model_config variations.

Verifies that fcnet_use_layernorm and output_layer_use_layernorm are correctly
applied to encoder networks, including LayerNorm after the last (output) layer.
"""

import dataclasses

import gymnasium as gym
import numpy as np
import pytest

from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
    DefaultPPOTorchRLModule,
)
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import MLPEncoderConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


def count_layernorm_modules(module):
    """Count LayerNorm layers in a module and its submodules."""
    return sum(1 for m in module.modules() if isinstance(m, nn.LayerNorm))


def get_layernorm_positions(sequential_net):
    """Get indices and normalized shapes of LayerNorm layers in a Sequential net."""
    result = []
    for i, layer in enumerate(sequential_net):
        if isinstance(layer, nn.LayerNorm):
            result.append((i, tuple(layer.normalized_shape)))
    return result


def get_full_model_config(overrides):
    """Merge overrides with DefaultModelConfig for Catalog compatibility."""
    return dataclasses.asdict(DefaultModelConfig()) | overrides


# ---------------------------------------------------------------------------
# MLPEncoderConfig parametrized tests
# ---------------------------------------------------------------------------

MLP_ENCODER_CONFIGS = [
    pytest.param(
        {
            "hidden_layer_use_layernorm": True,
            "output_layer_use_layernorm": False,
            "hidden_layer_dims": [16, 16],
            "output_layer_dim": 16,
            "expected_count": 2,
            "expected_positions": [(16,), (16,)],
        },
        id="hidden_only_layernorm",
    ),
    pytest.param(
        {
            "hidden_layer_use_layernorm": False,
            "output_layer_use_layernorm": True,
            "hidden_layer_dims": [16],
            "output_layer_dim": 16,
            "expected_count": 1,
            "expected_positions": [(16,)],
        },
        id="output_only_layernorm",
    ),
    pytest.param(
        {
            "hidden_layer_use_layernorm": True,
            "output_layer_use_layernorm": True,
            "hidden_layer_dims": [16],
            "output_layer_dim": 16,
            "expected_count": 2,
            "expected_positions": [(16,), (16,)],
        },
        id="both_hidden_and_output_layernorm",
    ),
    pytest.param(
        {
            "hidden_layer_use_layernorm": False,
            "output_layer_use_layernorm": False,
            "hidden_layer_dims": [16, 16],
            "output_layer_dim": 16,
            "expected_count": 0,
            "expected_positions": [],
        },
        id="no_layernorm",
    ),
]


@pytest.mark.parametrize("config", MLP_ENCODER_CONFIGS)
def test_mlp_encoder_config_layernorm(config):
    """MLPEncoderConfig LayerNorm combinations via parametrize."""
    encoder_config = MLPEncoderConfig(
        input_dims=[10],
        hidden_layer_dims=config["hidden_layer_dims"],
        hidden_layer_activation="relu",
        hidden_layer_use_layernorm=config["hidden_layer_use_layernorm"],
        output_layer_use_layernorm=config["output_layer_use_layernorm"],
        output_layer_dim=config["output_layer_dim"],
        output_layer_activation="relu",
    )
    encoder = encoder_config.build(framework="torch")
    layernorm_count = count_layernorm_modules(encoder.net)
    assert layernorm_count == config["expected_count"]

    positions = get_layernorm_positions(encoder.net.mlp)
    assert len(positions) == len(config["expected_positions"])
    for (_, shape), expected_shape in zip(positions, config["expected_positions"]):
        assert shape == expected_shape


def test_mlp_encoder_forward_pass():
    """Verify encoder produces correct output shape with LayerNorm enabled."""
    config = MLPEncoderConfig(
        input_dims=[10],
        hidden_layer_dims=[32, 32],
        hidden_layer_activation="relu",
        hidden_layer_use_layernorm=True,
        output_layer_use_layernorm=True,
        output_layer_dim=64,
        output_layer_activation="relu",
    )
    encoder = config.build(framework="torch")
    batch = torch.randn(8, 10)
    out = encoder({ENCODER_OUT: None, "obs": batch})
    assert out[ENCODER_OUT].shape == (8, 64)


# ---------------------------------------------------------------------------
# Catalog parametrized tests
# ---------------------------------------------------------------------------

CATALOG_CONFIGS = [
    pytest.param(
        {"fcnet_use_layernorm": True, "expected_count": 2},
        id="fcnet_use_layernorm_true",
    ),
    pytest.param(
        {"fcnet_use_layernorm": False, "expected_count": 0},
        id="fcnet_use_layernorm_false",
    ),
]


@pytest.mark.parametrize("config", CATALOG_CONFIGS)
def test_catalog_encoder_layernorm(config):
    """Catalog passes fcnet_use_layernorm to encoder config."""
    model_config_dict = get_full_model_config(
        {
            "fcnet_hiddens": [16, 16],
            "fcnet_activation": "relu",
            "fcnet_use_layernorm": config["fcnet_use_layernorm"],
        }
    )
    catalog = Catalog(
        observation_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
        action_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
        model_config_dict=model_config_dict,
    )
    encoder_config = catalog._get_encoder_config(
        observation_space=catalog.observation_space,
        model_config_dict=catalog._model_config_dict,
        action_space=catalog.action_space,
    )
    assert encoder_config.hidden_layer_use_layernorm == config["fcnet_use_layernorm"]
    assert encoder_config.output_layer_use_layernorm == config["fcnet_use_layernorm"]

    encoder = encoder_config.build(framework="torch")
    layernorm_count = count_layernorm_modules(encoder.net)
    assert layernorm_count == config["expected_count"]


# ---------------------------------------------------------------------------
# RLModule model_config parametrized tests
# ---------------------------------------------------------------------------

RL_MODULE_CONFIGS = [
    pytest.param(
        {
            "fcnet_hiddens": [16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        },
        4,
        id="fcnet_use_layernorm_true",
    ),
    pytest.param(
        {
            "fcnet_hiddens": [16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": False,
            "fcnet_activation": "relu",
        },
        0,
        id="fcnet_use_layernorm_false",
    ),
    pytest.param(
        {
            "fcnet_hiddens": [16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        },
        2,
        id="single_hidden_layer_with_layernorm",
    ),
    pytest.param(
        {
            "fcnet_hiddens": [16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": True,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        },
        2,
        id="shared_encoder_layernorm",
    ),
    pytest.param(
        {
            "fcnet_hiddens": [16, 16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        },
        6,
        id="three_hidden_layers_with_layernorm",
    ),
]


@pytest.mark.parametrize("model_config,expected_count", RL_MODULE_CONFIGS)
def test_rl_module_encoder_layernorm(model_config, expected_count):
    """RLModuleSpec with different model_config variations for LayerNorm."""
    model = RLModuleSpec(
        module_class=DefaultPPOTorchRLModule,
        observation_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
        action_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
        model_config=model_config,
    ).build()
    count = count_layernorm_modules(model.encoder)
    assert count == expected_count


def test_rl_module_forward_pass_with_layernorm():
    """Verify forward pass works with fcnet_use_layernorm=True."""
    model_config = {
        "fcnet_hiddens": [32, 32],
        "head_fcnet_hiddens": [16],
        "vf_share_layers": False,
        "fcnet_use_layernorm": True,
        "fcnet_activation": "relu",
    }
    model = RLModuleSpec(
        module_class=DefaultPPOTorchRLModule,
        observation_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
        action_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
        model_config=model_config,
    ).build()
    batch = {"obs": torch.randn(4, 10)}
    out = model.forward_inference(batch)
    assert "action_dist_inputs" in out
    assert out["action_dist_inputs"].shape[0] == 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
