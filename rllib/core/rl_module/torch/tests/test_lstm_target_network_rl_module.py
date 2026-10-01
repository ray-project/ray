import unittest

import gymnasium as gym
import numpy as np
import torch
import tree

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import TARGET_NETWORK_ACTION_DIST_INPUTS
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModuleWithTargetNetwork,
)


class TestLSTMContainingRLModuleWithTargetNetwork(unittest.TestCase):
    """Test cases for LSTMContainingRLModuleWithTargetNetwork."""

    def setUp(self):
        """Set up test fixtures."""
        self.obs_dim = 25
        self.action_dim = 4
        self.batch_size = 10
        self.seq_len = 5
        self.lstm_cell_size = 32

        self.module = LSTMContainingRLModuleWithTargetNetwork(
            observation_space=gym.spaces.Box(-1.0, 1.0, (self.obs_dim,), np.float32),
            action_space=gym.spaces.Discrete(self.action_dim),
            model_config={"lstm_cell_size": self.lstm_cell_size},
        )

    def _create_input_batch(self):
        """Helper method to create a dummy input batch."""
        obs = torch.from_numpy(
            np.random.random_sample(
                size=(self.batch_size, self.seq_len, self.obs_dim)
            ).astype(np.float32)
        )
        state_in = self.module.get_initial_state()
        # Repeat state_in across batch.
        state_in = tree.map_structure(
            lambda s: torch.from_numpy(s).unsqueeze(0).repeat(self.batch_size, 1),
            state_in,
        )
        return {
            Columns.OBS: obs,
            Columns.STATE_IN: state_in,
        }

    def test_make_target_networks(self):
        """Test that target networks are created correctly."""
        # Initially, target networks should not exist
        self.assertFalse(hasattr(self.module, "_old_lstm"))
        self.assertFalse(hasattr(self.module, "_old_fc_net"))
        self.assertFalse(hasattr(self.module, "_old_pi_head"))

        # Create target networks
        self.module.make_target_networks()

        # After creation, target networks should exist
        self.assertTrue(hasattr(self.module, "_old_lstm"))
        self.assertTrue(hasattr(self.module, "_old_fc_net"))
        self.assertTrue(hasattr(self.module, "_old_pi_head"))

        # Target networks should be torch modules
        self.assertIsInstance(self.module._old_lstm, torch.nn.Module)
        self.assertIsInstance(self.module._old_fc_net, torch.nn.Module)
        self.assertIsInstance(self.module._old_pi_head, torch.nn.Module)

        # Target networks should be different objects from main networks
        self.assertIsNot(self.module._lstm, self.module._old_lstm)
        self.assertIsNot(self.module._fc_net, self.module._old_fc_net)
        self.assertIsNot(self.module._pi_head, self.module._old_pi_head)

    def test_get_target_network_pairs(self):
        """Test that get_target_network_pairs returns correct pairs."""
        # Create target networks first
        self.module.make_target_networks()

        # Get target network pairs
        pairs = self.module.get_target_network_pairs()

        # Should return exactly 3 pairs (LSTM, FC net, policy head)
        self.assertEqual(len(pairs), 3)

        # Check that pairs are tuples of (main_net, target_net)
        for main_net, target_net in pairs:
            self.assertIsInstance(main_net, torch.nn.Module)
            self.assertIsInstance(target_net, torch.nn.Module)
            self.assertIsNot(main_net, target_net)

        # Verify the specific pairs
        expected_pairs = [
            (self.module._lstm, self.module._old_lstm),
            (self.module._fc_net, self.module._old_fc_net),
            (self.module._pi_head, self.module._old_pi_head),
        ]
        self.assertEqual(pairs, expected_pairs)

    def test_forward_target(self):
        """Test that forward_target produces correct output structure."""
        # Create target networks first
        self.module.make_target_networks()

        # Create input batch
        input_dict = self._create_input_batch()

        # Forward through target networks
        output = self.module.forward_target(input_dict)

        # Output should be a dictionary
        self.assertIsInstance(output, dict)

        # Should contain TARGET_NETWORK_ACTION_DIST_INPUTS
        self.assertIn(TARGET_NETWORK_ACTION_DIST_INPUTS, output)

        # Action dist inputs should be a tensor with correct shape
        action_dist_inputs = output[TARGET_NETWORK_ACTION_DIST_INPUTS]
        self.assertIsInstance(action_dist_inputs, torch.Tensor)
        expected_shape = (self.batch_size, self.seq_len, self.action_dim)
        self.assertEqual(action_dist_inputs.shape, expected_shape)

    def test_target_networks_independence(self):
        """Test that target networks are independent from main networks."""
        # Create target networks
        self.module.make_target_networks()

        # Get initial parameters from both networks
        main_lstm_params = [p.clone().detach() for p in self.module._lstm.parameters()]
        target_lstm_params = [
            p.clone().detach() for p in self.module._old_lstm.parameters()
        ]

        # Initially, parameters should be equal (target is copied from main)
        for main_param, target_param in zip(main_lstm_params, target_lstm_params):
            self.assertTrue(torch.allclose(main_param, target_param))

        # Create input batch
        input_dict = self._create_input_batch()

        # Forward through main network and compute gradients
        main_output = self.module.forward_train(input_dict)
        main_loss = main_output[Columns.ACTION_DIST_INPUTS].sum()
        main_loss.backward()

        # Modify main network parameters (simulate training step)
        with torch.no_grad():
            for param in self.module._lstm.parameters():
                param.add_(0.1)

        # Target network parameters should remain unchanged
        for target_param, original_target_param in zip(
            self.module._old_lstm.parameters(), target_lstm_params
        ):
            self.assertTrue(torch.allclose(target_param, original_target_param))

        # Verify that main and target networks now have different parameters
        for main_param, target_param in zip(
            self.module._lstm.parameters(), self.module._old_lstm.parameters()
        ):
            self.assertFalse(torch.allclose(main_param, target_param))


if __name__ == "__main__":
    import sys

    import pytest

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
