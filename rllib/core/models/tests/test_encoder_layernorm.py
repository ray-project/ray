"""Tests for LayerNorm in encoder models with different model_config variations.

Verifies that fcnet_use_layernorm and output_layer_use_layernorm are correctly
applied to encoder networks, including LayerNorm after the last (output) layer.
"""

import dataclasses
import unittest

import gymnasium as gym
import numpy as np

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


class TestEncoderLayerNormConfig(unittest.TestCase):
    """Tests for LayerNorm in MLPEncoderConfig with various config combinations."""

    def test_hidden_only_layernorm(self):
        """With hidden_layer_use_layernorm=True, output_layer_use_layernorm=False."""
        config = MLPEncoderConfig(
            input_dims=[10],
            hidden_layer_dims=[16, 16],
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=True,
            output_layer_use_layernorm=False,
            output_layer_dim=16,
            output_layer_activation="relu",
        )
        encoder = config.build(framework="torch")
        layernorm_count = count_layernorm_modules(encoder.net)
        self.assertEqual(
            layernorm_count, 2, "Expected 2 LayerNorms after hidden layers"
        )
        # No LayerNorm after output layer
        positions = get_layernorm_positions(encoder.net.mlp)
        self.assertEqual(len(positions), 2)
        self.assertEqual(positions[0][1], (16,))
        self.assertEqual(positions[1][1], (16,))

    def test_output_only_layernorm(self):
        """With hidden_layer_use_layernorm=False, output_layer_use_layernorm=True."""
        config = MLPEncoderConfig(
            input_dims=[10],
            hidden_layer_dims=[16],
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=False,
            output_layer_use_layernorm=True,
            output_layer_dim=16,
            output_layer_activation="relu",
        )
        encoder = config.build(framework="torch")
        layernorm_count = count_layernorm_modules(encoder.net)
        self.assertEqual(layernorm_count, 1, "Expected 1 LayerNorm after output layer")
        positions = get_layernorm_positions(encoder.net.mlp)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0][1], (16,))

    def test_both_hidden_and_output_layernorm(self):
        """With both hidden and output LayerNorm enabled (fcnet_use_layernorm=True)."""
        config = MLPEncoderConfig(
            input_dims=[10],
            hidden_layer_dims=[16],
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=True,
            output_layer_use_layernorm=True,
            output_layer_dim=16,
            output_layer_activation="relu",
        )
        encoder = config.build(framework="torch")
        layernorm_count = count_layernorm_modules(encoder.net)
        self.assertEqual(layernorm_count, 2, "Expected 2 LayerNorms (hidden + output)")
        positions = get_layernorm_positions(encoder.net.mlp)
        self.assertEqual(len(positions), 2)
        self.assertEqual(positions[0][1], (16,))
        self.assertEqual(positions[1][1], (16,))

    def test_no_layernorm(self):
        """With both LayerNorm options disabled."""
        config = MLPEncoderConfig(
            input_dims=[10],
            hidden_layer_dims=[16, 16],
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=False,
            output_layer_use_layernorm=False,
            output_layer_dim=16,
            output_layer_activation="relu",
        )
        encoder = config.build(framework="torch")
        layernorm_count = count_layernorm_modules(encoder.net)
        self.assertEqual(layernorm_count, 0, "Expected no LayerNorms")

    def test_encoder_forward_pass(self):
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
        self.assertEqual(out[ENCODER_OUT].shape, (8, 64))


class TestCatalogEncoderLayerNorm(unittest.TestCase):
    """Tests that Catalog passes fcnet_use_layernorm to encoder output layer."""

    def _get_full_model_config(self, overrides):
        """Merge overrides with DefaultModelConfig for Catalog compatibility."""
        return dataclasses.asdict(DefaultModelConfig()) | overrides

    def test_catalog_fcnet_use_layernorm_true(self):
        """Catalog with fcnet_use_layernorm=True should add LayerNorm to output layer."""
        model_config_dict = self._get_full_model_config(
            {
                "fcnet_hiddens": [16, 16],
                "fcnet_activation": "relu",
                "fcnet_use_layernorm": True,
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
        self.assertTrue(encoder_config.hidden_layer_use_layernorm)
        self.assertTrue(encoder_config.output_layer_use_layernorm)

        encoder = encoder_config.build(framework="torch")
        layernorm_count = count_layernorm_modules(encoder.net)
        # fcnet_hiddens=[16,16] -> hidden_layer_dims=[16], output_layer_dim=16
        # 1 hidden + 1 output layer = 2 LayerNorms
        self.assertEqual(layernorm_count, 2)

    def test_catalog_fcnet_use_layernorm_false(self):
        """Catalog with fcnet_use_layernorm=False should have no LayerNorms."""
        model_config_dict = self._get_full_model_config(
            {
                "fcnet_hiddens": [16, 16],
                "fcnet_activation": "relu",
                "fcnet_use_layernorm": False,
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
        self.assertFalse(encoder_config.hidden_layer_use_layernorm)
        self.assertFalse(encoder_config.output_layer_use_layernorm)

        encoder = encoder_config.build(framework="torch")
        layernorm_count = count_layernorm_modules(encoder.net)
        self.assertEqual(layernorm_count, 0)


class TestRLModuleModelConfigVariations(unittest.TestCase):
    """Tests RLModuleSpec with different model_config variations for LayerNorm."""

    def _build_and_count_layernorms(self, model_config):
        """Build RLModule and return LayerNorm count in encoder."""
        model = RLModuleSpec(
            module_class=DefaultPPOTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
            action_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
            model_config=model_config,
        ).build()
        return count_layernorm_modules(model.encoder)

    def test_fcnet_use_layernorm_true(self):
        """fcnet_use_layernorm=True: LayerNorm after hidden and output layers."""
        model_config = {
            "fcnet_hiddens": [16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        }
        count = self._build_and_count_layernorms(model_config)
        # fcnet_hiddens=[16,16] -> 1 hidden + 1 output = 2 LayerNorms per encoder
        # Actor + critic encoders = 4 LayerNorms total
        self.assertEqual(count, 4, "Expected 4 LayerNorms (2 per encoder)")

    def test_fcnet_use_layernorm_false(self):
        """fcnet_use_layernorm=False: no LayerNorm in encoder."""
        model_config = {
            "fcnet_hiddens": [16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": False,
            "fcnet_activation": "relu",
        }
        count = self._build_and_count_layernorms(model_config)
        self.assertEqual(count, 0, "Expected no LayerNorms in encoder")

    def test_single_hidden_layer_with_layernorm(self):
        """Single hidden layer + output layer, both with LayerNorm."""
        model_config = {
            "fcnet_hiddens": [16],  # hidden_layer_dims=[], output_layer_dim=16
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        }
        count = self._build_and_count_layernorms(model_config)
        # With fcnet_hiddens=[16]: hidden_layer_dims=[], output_layer_dim=16
        # So only output layer gets LayerNorm = 1 per encoder
        self.assertEqual(count, 2, "Expected 2 LayerNorms (1 per encoder)")

    def test_shared_encoder_layernorm(self):
        """vf_share_layers=True: single encoder with LayerNorm."""
        model_config = {
            "fcnet_hiddens": [16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": True,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        }
        model = RLModuleSpec(
            module_class=DefaultPPOTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
            action_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
            model_config=model_config,
        ).build()
        count = count_layernorm_modules(model.encoder)
        # Single shared encoder: 1 hidden + 1 output = 2 LayerNorms
        self.assertEqual(count, 2)

    def test_forward_pass_with_layernorm(self):
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
        self.assertIn("action_dist_inputs", out)
        self.assertEqual(out["action_dist_inputs"].shape[0], 4)

    def test_three_hidden_layers_with_layernorm(self):
        """fcnet_hiddens=[16,16,16]: 2 hidden + 1 output = 3 LayerNorms per encoder."""
        model_config = {
            "fcnet_hiddens": [16, 16, 16],
            "head_fcnet_hiddens": [16],
            "vf_share_layers": False,
            "fcnet_use_layernorm": True,
            "fcnet_activation": "relu",
        }
        count = self._build_and_count_layernorms(model_config)
        # 2 hidden + 1 output = 3 per encoder, 2 encoders = 6
        self.assertEqual(count, 6)

    def test_different_activations_with_layernorm(self):
        """LayerNorm works with different activation functions."""
        for activation in ["relu", "tanh", "swish"]:
            model_config = {
                "fcnet_hiddens": [16, 16],
                "head_fcnet_hiddens": [16],
                "vf_share_layers": False,
                "fcnet_use_layernorm": True,
                "fcnet_activation": activation,
            }
            model = RLModuleSpec(
                module_class=DefaultPPOTorchRLModule,
                observation_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
                action_space=gym.spaces.Box(0, 1, (10,), dtype=np.float32),
                model_config=model_config,
            ).build()
            batch = {"obs": torch.randn(2, 10)}
            out = model.forward_inference(batch)
            self.assertIn("action_dist_inputs", out)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
