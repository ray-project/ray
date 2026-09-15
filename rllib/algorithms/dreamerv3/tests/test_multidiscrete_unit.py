"""Unit tests for DreamerV3 MultiDiscrete action space support.

These tests verify the ActorNetwork functionality for MultiDiscrete action spaces
without requiring a full Ray cluster or environment interaction.
"""
import unittest

import gymnasium as gym
import torch

from ray.rllib.algorithms.dreamerv3.torch.models.actor_network import (
    ActorNetwork,
    MultiDiscreteOneHotDistribution,
)


# Model size configuration for "nano" (minimal for testing)
# Based on DreamerV3 paper Appendix B
NANO_H_DIM = 128  # GRU hidden size
NANO_Z_CATEGORICALS = 4  # Number of categorical distributions
NANO_Z_CLASSES = 4  # Number of classes per categorical


class TestMultiDiscreteActorNetwork(unittest.TestCase):
    """Test ActorNetwork with MultiDiscrete action spaces."""

    def setUp(self):
        """Set up test fixtures."""
        self.action_space = gym.spaces.MultiDiscrete([3, 2, 2])
        self.total_action_dim = sum(self.action_space.nvec)  # 7
        self.input_size = NANO_H_DIM + NANO_Z_CATEGORICALS * NANO_Z_CLASSES
        self.batch_size = 4

    def test_actor_network_multidiscrete_init(self):
        """Test that ActorNetwork initializes correctly with MultiDiscrete."""
        actor = ActorNetwork(
            input_size=self.input_size,
            model_size="nano",
            action_space=self.action_space,
        )

        # Check that mlp_heads was created with correct number of heads
        self.assertTrue(hasattr(actor, "mlp_heads"))
        self.assertEqual(len(actor.mlp_heads), len(self.action_space.nvec))

        # Check that each head has correct output size
        for i, head in enumerate(actor.mlp_heads):
            self.assertEqual(head.out_features, self.action_space.nvec[i])

        # Check actions_dim attribute
        self.assertEqual(actor.actions_dim, list(self.action_space.nvec))

    def test_actor_network_multidiscrete_forward(self):
        """Test forward pass produces correct output shapes."""
        actor = ActorNetwork(
            input_size=self.input_size,
            model_size="nano",
            action_space=self.action_space,
        )

        # Create dummy inputs
        h = torch.randn(self.batch_size, NANO_H_DIM)
        z = torch.randn(self.batch_size, NANO_Z_CATEGORICALS, NANO_Z_CLASSES)

        # Forward pass
        action, distr_params = actor(h, z, return_distr_params=True)

        # Check shapes
        self.assertEqual(action.shape, (self.batch_size, self.total_action_dim))
        self.assertEqual(distr_params.shape, (self.batch_size, self.total_action_dim))

        # Check that action is valid one-hot encoded (sums to ~1 per sub-action)
        split_actions = torch.split(action, list(self.action_space.nvec), dim=-1)
        for sub_action in split_actions:
            sums = sub_action.sum(dim=-1)
            self.assertTrue(torch.allclose(sums, torch.ones_like(sums), atol=1e-5))

    def test_actor_network_multidiscrete_distribution(self):
        """Test that get_action_dist_object returns working distribution."""
        actor = ActorNetwork(
            input_size=self.input_size,
            model_size="nano",
            action_space=self.action_space,
        )

        # Create dummy inputs and get distribution params
        h = torch.randn(self.batch_size, NANO_H_DIM)
        z = torch.randn(self.batch_size, NANO_Z_CATEGORICALS, NANO_Z_CLASSES)
        _, distr_params = actor(h, z, return_distr_params=True)

        # Get distribution object
        distr = actor.get_action_dist_object(distr_params)

        # Check it's our custom distribution
        self.assertIsInstance(distr, MultiDiscreteOneHotDistribution)

        # Test sample
        sample = distr.sample()
        self.assertEqual(sample.shape, (self.batch_size, self.total_action_dim))

        # Test log_prob
        log_prob = distr.log_prob(sample)
        self.assertEqual(log_prob.shape, (self.batch_size,))

        # Test entropy
        entropy = distr.entropy()
        self.assertEqual(entropy.shape, (self.batch_size,))
        self.assertTrue((entropy >= 0).all())

        # Test mode
        mode = distr.mode
        self.assertEqual(mode.shape, (self.batch_size, self.total_action_dim))

    def test_actor_network_various_multidiscrete_configs(self):
        """Test with various MultiDiscrete configurations."""
        configs = [
            [2, 2],  # Simple binary
            [3, 3, 3],  # Three 3-way
            [3, 2, 2, 6],  # SuperTux-like
            [5],  # Single (edge case)
            [10, 5, 3, 2],  # Varied sizes
        ]

        for nvec in configs:
            with self.subTest(nvec=nvec):
                action_space = gym.spaces.MultiDiscrete(nvec)
                total_dim = sum(nvec)

                actor = ActorNetwork(
                    input_size=self.input_size,
                    model_size="nano",
                    action_space=action_space,
                )

                h = torch.randn(2, NANO_H_DIM)
                z = torch.randn(2, NANO_Z_CATEGORICALS, NANO_Z_CLASSES)

                action, distr_params = actor(h, z, return_distr_params=True)

                self.assertEqual(action.shape, (2, total_dim))
                self.assertEqual(distr_params.shape, (2, total_dim))


class TestMultiDiscreteOneHotDistribution(unittest.TestCase):
    """Test the MultiDiscreteOneHotDistribution class directly."""

    def test_distribution_operations(self):
        """Test all distribution operations."""
        actions_dim = [3, 2, 2]
        batch_size = 4

        # Create dummy logits and distributions
        logits_list = [torch.randn(batch_size, dim) for dim in actions_dim]
        distributions = [
            torch.distributions.OneHotCategorical(logits=logits)
            for logits in logits_list
        ]

        distr = MultiDiscreteOneHotDistribution(distributions, actions_dim)

        # Test sample
        sample = distr.sample()
        self.assertEqual(sample.shape, (batch_size, sum(actions_dim)))

        # Verify one-hot structure
        split_samples = torch.split(sample, actions_dim, dim=-1)
        for sub_sample in split_samples:
            # Each should sum to 1
            self.assertTrue(
                torch.allclose(sub_sample.sum(dim=-1), torch.ones(batch_size))
            )

        # Test log_prob
        log_prob = distr.log_prob(sample)
        self.assertEqual(log_prob.shape, (batch_size,))
        self.assertTrue((log_prob <= 0).all())  # Log probs are negative

        # Test entropy
        entropy = distr.entropy()
        self.assertEqual(entropy.shape, (batch_size,))
        self.assertTrue((entropy >= 0).all())

        # Test mode
        mode = distr.mode
        self.assertEqual(mode.shape, (batch_size, sum(actions_dim)))

        # Verify mode is one-hot encoded (sums to 1.0 and contains only 0s and 1s)
        split_modes = torch.split(mode, actions_dim, dim=-1)
        for sub_mode in split_modes:
            # Each should sum to 1
            self.assertTrue(
                torch.allclose(sub_mode.sum(dim=-1), torch.ones(batch_size))
            )
            # Should contain only 0s and 1s
            self.assertTrue(((sub_mode == 0) | (sub_mode == 1)).all())


if __name__ == "__main__":
    unittest.main()
