import itertools
import unittest
from pathlib import Path

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.marwil import MARWILConfig
from ray.rllib.core import DEFAULT_POLICY_ID, Columns
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class TestMARWILRLModule(unittest.TestCase):
    """Tests for MARWIL RLModule functionality."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=2)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_forward_inference(self):
        """Test that forward_inference produces valid outputs for different envs."""
        env_ids = ["CartPole-v1", "Pendulum-v1"]

        # Test each environment.
        for env_id in env_ids:
            with self.subTest(env=env_id):
                env = gym.make(env_id)

                # Define MARWIL config.
                config = (
                    MARWILConfig()
                    .environment(env_id)
                    .rl_module(
                        model_config=DefaultModelConfig(
                            fcnet_hiddens=[64, 64],
                            fcnet_activation="relu",
                        ),
                    )
                )

                # Build the RLModule directly from config.
                module_spec = config.get_default_rl_module_spec()
                module_spec.observation_space = env.observation_space
                module_spec.action_space = env.action_space
                module = module_spec.build()

                # Create batch of observations.
                batch_size = 4
                obs = torch.tensor(
                    np.array(
                        [env.observation_space.sample() for _ in range(batch_size)]
                    ),
                    dtype=torch.float32,
                )

                # Run forward_inference.
                with torch.no_grad():
                    outputs = module.forward_inference({Columns.OBS: obs})

                # Check that action_dist_inputs are in the output.
                self.assertIn(Columns.ACTION_DIST_INPUTS, outputs)

                # Check output shape matches action space.
                action_dist_inputs = outputs[Columns.ACTION_DIST_INPUTS]
                # Verify shape based on action space type.
                if isinstance(env.action_space, gym.spaces.Discrete):
                    # For discrete: logits shape should be (batch_size, num_actions)
                    self.assertEqual(
                        action_dist_inputs.shape,
                        (batch_size, env.action_space.n),
                    )
                # For Box (continuous) action spaces
                else:
                    # For continuous: mean and log_std
                    # Shape is (batch_size, action_dim * 2) or (batch_size, action_dim)
                    action_dim = env.action_space.shape[0]
                    self.assertEqual(action_dist_inputs.shape[0], batch_size)
                    self.assertGreaterEqual(action_dist_inputs.shape[1], action_dim)

                env.close()

    def test_forward_exploration(self):
        """Test that forward_exploration produces valid outputs."""
        env_ids = ["CartPole-v1", "Pendulum-v1"]

        # Test for each environment.
        for env_id in env_ids:
            with self.subTest(env=env_id):
                env = gym.make(env_id)

                config = (
                    MARWILConfig()
                    .environment(env_id)
                    .rl_module(
                        model_config=DefaultModelConfig(
                            fcnet_hiddens=[32, 32],
                        ),
                    )
                )

                # Build the RLModule.
                module_spec = config.get_default_rl_module_spec()
                module_spec.observation_space = env.observation_space
                module_spec.action_space = env.action_space
                module = module_spec.build()

                # Create a batch of observations.
                batch_size = 8
                obs = torch.tensor(
                    np.array(
                        [env.observation_space.sample() for _ in range(batch_size)]
                    ),
                    dtype=torch.float32,
                )

                # Run forward_exploration.
                with torch.no_grad():
                    outputs = module.forward_exploration({Columns.OBS: obs})

                # Check outputs.
                self.assertIn(Columns.ACTION_DIST_INPUTS, outputs)
                self.assertEqual(
                    outputs[Columns.ACTION_DIST_INPUTS].shape[0], batch_size
                )

                env.close()

    def test_forward_train(self):
        """Test that forward_train produces outputs needed for MARWIL loss."""
        env = gym.make("CartPole-v1")

        config = (
            MARWILConfig()
            .environment("CartPole-v1")
            .rl_module(
                model_config=DefaultModelConfig(
                    fcnet_hiddens=[64, 64],
                ),
            )
        )

        # Build the RLModule.
        module_spec = config.get_default_rl_module_spec()
        module_spec.observation_space = env.observation_space
        module_spec.action_space = env.action_space
        module = module_spec.build()

        # Create a batch of observations.
        batch_size = 16
        obs = torch.tensor(
            np.array([env.observation_space.sample() for _ in range(batch_size)]),
            dtype=torch.float32,
        )

        # forward_train should work with gradients enabled.
        outputs = module.forward_train({Columns.OBS: obs})

        # MARWIL needs action distribution inputs for policy loss.
        self.assertIn(Columns.ACTION_DIST_INPUTS, outputs)

        # Check shapes.
        self.assertEqual(outputs[Columns.ACTION_DIST_INPUTS].shape[0], batch_size)

        # Verify gradients can flow.
        loss = outputs[Columns.ACTION_DIST_INPUTS].sum()
        loss.backward()

        # Check that parameters have gradients.
        has_grad = False
        for param in module.parameters():
            if param.grad is not None:
                has_grad = True
                break
        self.assertTrue(has_grad, "No gradients computed in forward_train")

        env.close()

    def test_different_model_configs(self):
        """Test RLModule with different model configurations."""
        hidden_configs = [
            [32],
            [64, 64],
            [128, 128, 128],
        ]
        activations = ["relu", "tanh"]

        env = gym.make("CartPole-v1")

        # Test all combinations of hidden layers and activations.
        for hiddens, activation in itertools.product(hidden_configs, activations):
            with self.subTest(hiddens=hiddens, activation=activation):
                config = (
                    MARWILConfig()
                    .environment("CartPole-v1")
                    .rl_module(
                        model_config=DefaultModelConfig(
                            fcnet_hiddens=hiddens,
                            fcnet_activation=activation,
                        ),
                    )
                )

                # Build the RLModule.
                module_spec = config.get_default_rl_module_spec()
                module_spec.observation_space = env.observation_space
                module_spec.action_space = env.action_space
                module = module_spec.build()

                # Create a batch of observations.
                obs = torch.tensor(
                    np.array([env.observation_space.sample() for _ in range(4)]),
                    dtype=torch.float32,
                )

                # Run forward_inference.
                with torch.no_grad():
                    outputs = module.forward_inference({Columns.OBS: obs})

                # Check outputs.
                self.assertIn(Columns.ACTION_DIST_INPUTS, outputs)
                self.assertEqual(
                    outputs[Columns.ACTION_DIST_INPUTS].shape[1], env.action_space.n
                )

        env.close()

    def test_value_function_api(self):
        """Test that the MARWIL RLModule implements ValueFunctionAPI correctly."""
        env = gym.make("CartPole-v1")

        config = (
            MARWILConfig()
            .environment("CartPole-v1")
            .rl_module(
                model_config=DefaultModelConfig(fcnet_hiddens=[64, 64]),
            )
        )

        # Build the RLModule.
        module_spec = config.get_default_rl_module_spec()
        module_spec.observation_space = env.observation_space
        module_spec.action_space = env.action_space
        module = module_spec.build()

        # Create a batch of observations.
        batch_size = 8
        obs = torch.tensor(
            np.array([env.observation_space.sample() for _ in range(batch_size)]),
            dtype=torch.float32,
        )

        # Test compute_values method (from ValueFunctionAPI).
        with torch.no_grad():
            # First run forward to populate internal state.
            _ = module.forward_train({Columns.OBS: obs})
            values = module.compute_values({Columns.OBS: obs})

        # Ensure values shape and validity.
        self.assertEqual(values.shape, (batch_size,))
        self.assertTrue(torch.isfinite(values).all())

        env.close()

    def test_inference_only_mode(self):
        """Test that RLModule can be built in inference-only mode."""
        env = gym.make("CartPole-v1")

        # Define MARWIL config with inference_only=True
        config = (
            MARWILConfig()
            .environment("CartPole-v1")
            .rl_module(
                model_config=DefaultModelConfig(fcnet_hiddens=[64, 64]),
            )
        )

        # Build the RLModule in inference-only mode.
        module_spec = config.get_default_rl_module_spec()
        module_spec.observation_space = env.observation_space
        module_spec.action_space = env.action_space
        module_spec.inference_only = True
        module = module_spec.build()

        # Create a batch of observations.
        obs = torch.tensor(
            np.array([env.observation_space.sample() for _ in range(4)]),
            dtype=torch.float32,
        )

        # Run forward_inference.
        with torch.no_grad():
            outputs = module.forward_inference({Columns.OBS: obs})

        # Check outputs.
        self.assertIn(Columns.ACTION_DIST_INPUTS, outputs)

        env.close()

    def test_action_distribution_classes(self):
        """Test that action distribution classes are correctly returned."""
        # Test discrete action space.
        env_discrete = gym.make("CartPole-v1")
        config_discrete = MARWILConfig().environment("CartPole-v1")

        # Build the RLModule.
        module_spec = config_discrete.get_default_rl_module_spec()
        module_spec.observation_space = env_discrete.observation_space
        module_spec.action_space = env_discrete.action_space
        module_discrete = module_spec.build()

        # Get action distribution classes.
        inference_dist_cls = module_discrete.get_inference_action_dist_cls()
        exploration_dist_cls = module_discrete.get_exploration_action_dist_cls()

        # Verify they are not None.
        self.assertIsNotNone(inference_dist_cls)
        self.assertIsNotNone(exploration_dist_cls)

        # Close the environment.
        env_discrete.close()

        # Test continuous action space.
        env_continuous = gym.make("Pendulum-v1")
        config_continuous = MARWILConfig().environment("Pendulum-v1")

        # Build the RLModule.
        module_spec = config_continuous.get_default_rl_module_spec()
        module_spec.observation_space = env_continuous.observation_space
        module_spec.action_space = env_continuous.action_space
        module_continuous = module_spec.build()

        # Get action distribution classes.
        inference_dist_cls = module_continuous.get_inference_action_dist_cls()
        exploration_dist_cls = module_continuous.get_exploration_action_dist_cls()

        # Verify they are not None.
        self.assertIsNotNone(inference_dist_cls)
        self.assertIsNotNone(exploration_dist_cls)

        env_continuous.close()

    def test_sampling_loop(self):
        """Test a complete sampling loop with the RLModule."""
        env = gym.make("CartPole-v1")

        # Define MARWIL config.
        config = (
            MARWILConfig()
            .environment("CartPole-v1")
            .rl_module(
                model_config=DefaultModelConfig(fcnet_hiddens=[64, 64]),
            )
        )

        # Build the RLModule.
        module_spec = config.get_default_rl_module_spec()
        module_spec.observation_space = env.observation_space
        module_spec.action_space = env.action_space
        module = module_spec.build()

        # Get exploration action distribution class.
        action_dist_cls = module.get_exploration_action_dist_cls()

        # Run a sampling loop.
        obs, _ = env.reset()
        total_reward = 0
        done = False
        max_steps = 100
        steps = 0

        while not done and steps < max_steps:
            obs_tensor = torch.tensor([obs], dtype=torch.float32)

            with torch.no_grad():
                outputs = module.forward_exploration({Columns.OBS: obs_tensor})

            action_dist = action_dist_cls.from_logits(
                outputs[Columns.ACTION_DIST_INPUTS]
            )
            action = action_dist.sample()[0].numpy()

            obs, reward, terminated, truncated, _ = env.step(action)
            total_reward += reward
            done = terminated or truncated
            steps += 1

        # Just verify the loop completed without errors.
        self.assertGreater(steps, 0)

        env.close()


class TestMARWILAlgorithmIntegration(unittest.TestCase):
    """Integration tests for MARWIL algorithm with RLModule."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=2, ignore_reinit_error=True)
        # Set up data path.
        cls.rllib_dir = Path(__file__).parents[3]
        cls.data_path = (
            cls.rllib_dir
            / "offline"
            / "tests"
            / "data"
            / "cartpole"
            / "cartpole-v1_large"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_algorithm_build_with_rl_module(self):
        """Test that MARWIL algorithm builds correctly with RLModule."""
        if not self.data_path.exists():
            self.skipTest(f"Test data not found at {self.data_path}")

        # Create the environment for spaces.
        env = gym.make("CartPole-v1")
        # Define MARWIL config.
        config = (
            MARWILConfig()
            .environment(
                observation_space=env.observation_space,
                action_space=env.action_space,
            )
            .offline_data(
                input_=[f"local://{self.data_path}"],
                dataset_num_iters_per_learner=1,
            )
            .training(
                beta=1.0,
                gamma=0.99,
            )
            .rl_module(
                model_config=DefaultModelConfig(fcnet_hiddens=[64, 64]),
            )
        )

        # Build and verify algorithm creates RLModule.
        algo = config.build()
        module = algo.learner_group._learner.module["default_policy"]

        # Verify module is not None.
        self.assertIsNotNone(module)

        # Test that module can do forward passes.
        batch = algo.offline_data.sample(2, return_iterator=False, num_shards=0)

        # Run forward_inference.
        with torch.no_grad():
            outputs = module.forward_inference(
                {Columns.OBS: batch[DEFAULT_POLICY_ID][Columns.OBS]}
            )

        # Check outputs.
        self.assertIn(Columns.ACTION_DIST_INPUTS, outputs)

        algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
