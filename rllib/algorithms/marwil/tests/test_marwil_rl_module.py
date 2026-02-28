import itertools
from pathlib import Path

import gymnasium as gym
import numpy as np
import pytest

import ray
from ray.rllib.algorithms.marwil import MARWILConfig
from ray.rllib.core import DEFAULT_POLICY_ID, Columns
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


# --- Fixtures ---


@pytest.fixture(scope="class")
def ray_init():
    """Initialize Ray for the test module."""
    ray.init(num_cpus=2, ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture
def cartpole_env():
    """Create a CartPole environment."""
    env = gym.make("CartPole-v1")
    yield env
    env.close()


@pytest.fixture
def pendulum_env():
    """Create a Pendulum environment."""
    env = gym.make("Pendulum-v1")
    yield env
    env.close()


@pytest.fixture
def data_path():
    """Path to test data for integration tests."""
    rllib_dir = Path(__file__).parents[3]
    return rllib_dir / "offline" / "tests" / "data" / "cartpole" / "cartpole-v1_large"


# --- Helper Functions ---


def sample_obs_batch(env, batch_size):
    """Create a batch of sampled observations as a tensor."""
    return torch.tensor(
        np.array([env.observation_space.sample() for _ in range(batch_size)]),
        dtype=torch.float32,
    )


@pytest.fixture
def make_module():
    """Factory fixture for building MARWIL RLModules."""

    def _make(env, hiddens=(64, 64), activation="relu", inference_only=False):
        config = (
            MARWILConfig()
            .environment(env.spec.id)
            .rl_module(
                model_config=DefaultModelConfig(
                    fcnet_hiddens=list(hiddens),
                    fcnet_activation=activation,
                ),
            )
        )
        module_spec = config.get_default_rl_module_spec()
        module_spec.observation_space = env.observation_space
        module_spec.action_space = env.action_space
        module_spec.inference_only = inference_only
        return module_spec.build()

    return _make


# --- Tests for MARWIL RLModule Functionality ---


class TestMARWILRLModule:
    """Tests for MARWIL RLModule functionality."""

    @pytest.mark.parametrize("env_id", ["CartPole-v1", "Pendulum-v1"])
    def test_forward_inference(self, ray_init, make_module, env_id):
        """Test that forward_inference produces valid outputs for different envs."""
        env = gym.make(env_id)

        try:
            module = make_module(env)
            batch_size = 4
            obs = sample_obs_batch(env, batch_size)

            with torch.no_grad():
                outputs = module.forward_inference({Columns.OBS: obs})

            assert Columns.ACTION_DIST_INPUTS in outputs

            action_dist_inputs = outputs[Columns.ACTION_DIST_INPUTS]

            if isinstance(env.action_space, gym.spaces.Discrete):
                assert action_dist_inputs.shape == (batch_size, env.action_space.n)
            else:
                # For continuous (Box) action spaces.
                action_dim = env.action_space.shape[0]
                assert action_dist_inputs.shape[0] == batch_size
                assert action_dist_inputs.shape[1] >= action_dim
        finally:
            env.close()

    @pytest.mark.parametrize("env_id", ["CartPole-v1", "Pendulum-v1"])
    def test_forward_exploration(self, ray_init, make_module, env_id):
        """Test that forward_exploration produces valid outputs."""
        env = gym.make(env_id)

        try:
            module = make_module(env, hiddens=(32, 32))
            batch_size = 8
            obs = sample_obs_batch(env, batch_size)

            with torch.no_grad():
                outputs = module.forward_exploration({Columns.OBS: obs})

            assert Columns.ACTION_DIST_INPUTS in outputs
            assert outputs[Columns.ACTION_DIST_INPUTS].shape[0] == batch_size
        finally:
            env.close()

    def test_forward_train(self, ray_init, make_module, cartpole_env):
        """Test that forward_train produces outputs needed for MARWIL loss."""
        module = make_module(cartpole_env)
        batch_size = 16
        obs = sample_obs_batch(cartpole_env, batch_size)

        # forward_train should work with gradients enabled.
        outputs = module.forward_train({Columns.OBS: obs})

        assert Columns.ACTION_DIST_INPUTS in outputs
        assert outputs[Columns.ACTION_DIST_INPUTS].shape[0] == batch_size

        # Verify gradients can flow.
        loss = outputs[Columns.ACTION_DIST_INPUTS].sum()
        loss.backward()

        has_grad = any(p.grad is not None for p in module.parameters())
        assert has_grad, "No gradients computed in forward_train"

    @pytest.mark.parametrize(
        "hiddens,activation",
        list(
            itertools.product(
                [[32], [64, 64], [128, 128, 128]],
                ["relu", "tanh"],
            )
        ),
    )
    def test_different_model_configs(
        self, ray_init, make_module, cartpole_env, hiddens, activation
    ):
        """Test RLModule with different model configurations."""
        module = make_module(cartpole_env, hiddens=hiddens, activation=activation)
        obs = sample_obs_batch(cartpole_env, batch_size=4)

        with torch.no_grad():
            outputs = module.forward_inference({Columns.OBS: obs})

        assert Columns.ACTION_DIST_INPUTS in outputs
        assert (
            outputs[Columns.ACTION_DIST_INPUTS].shape[1] == cartpole_env.action_space.n
        )

    def test_value_function_api(self, ray_init, make_module, cartpole_env):
        """Test that the MARWIL RLModule implements ValueFunctionAPI correctly."""
        module = make_module(cartpole_env)
        batch_size = 8
        obs = sample_obs_batch(cartpole_env, batch_size)

        with torch.no_grad():
            values = module.compute_values({Columns.OBS: obs})

        assert values.shape == (batch_size,)
        assert torch.isfinite(values).all()

    def test_inference_only_mode(self, ray_init, make_module, cartpole_env):
        """Test that RLModule can be built in inference-only mode."""
        module = make_module(cartpole_env, inference_only=True)
        obs = sample_obs_batch(cartpole_env, batch_size=4)

        with torch.no_grad():
            outputs = module.forward_inference({Columns.OBS: obs})

        # Action distribution inputs are supposed to be returned by an inference-only module.
        assert Columns.ACTION_DIST_INPUTS in outputs
        # Values are not supposed to be returned.
        assert Columns.VF_PREDS not in outputs

    @pytest.mark.parametrize(
        "env_id",
        ["CartPole-v1", "Pendulum-v1"],
        ids=["discrete", "continuous"],
    )
    def test_action_distribution_classes(self, ray_init, make_module, env_id):
        """Test that action distribution classes are correctly returned."""
        env = gym.make(env_id)

        try:
            module = make_module(env)

            inference_dist_cls = module.get_inference_action_dist_cls()
            exploration_dist_cls = module.get_exploration_action_dist_cls()
            training_dist_cls = module.get_train_action_dist_cls()

            assert inference_dist_cls is not None
            assert exploration_dist_cls is not None
            assert training_dist_cls is not None
        finally:
            env.close()

    def test_sampling_loop(self, ray_init, make_module, cartpole_env):
        """Test a complete sampling loop with the RLModule."""
        module = make_module(cartpole_env)
        action_dist_cls = module.get_exploration_action_dist_cls()

        obs, _ = cartpole_env.reset()
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

            obs, _, terminated, truncated, _ = cartpole_env.step(action)
            done = terminated or truncated
            steps += 1

        assert steps > 0


class TestMARWILAlgorithmIntegration:
    """Integration tests for MARWIL algorithm with RLModule."""

    def test_algorithm_build_with_rl_module(self, ray_init, cartpole_env, data_path):
        """Test that MARWIL algorithm builds correctly with RLModule."""
        if not data_path.exists():
            pytest.skip(f"Test data not found at {data_path}")

        config = (
            MARWILConfig()
            .environment(
                observation_space=cartpole_env.observation_space,
                action_space=cartpole_env.action_space,
            )
            .offline_data(
                input_=[f"local://{data_path}"],
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
        # NOTE: This test intentionally accesses the internal `_learner` attribute
        # of `learner_group` to inspect the underlying RLModule. This relies on
        # internal RLlib APIs and may need to be updated if those internals change.
        algo = config.build()

        try:
            module = algo.learner_group._learner.module[DEFAULT_POLICY_ID]
            assert module is not None

            batch = algo.offline_data.sample(2, return_iterator=False, num_shards=0)

            with torch.no_grad():
                outputs = module.forward_inference(
                    {Columns.OBS: torch.tensor(batch[DEFAULT_POLICY_ID][Columns.OBS])}
                )

            assert Columns.ACTION_DIST_INPUTS in outputs
        finally:
            algo.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
