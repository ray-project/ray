#!/usr/bin/env python
import os

import pytest

import ray.rllib.algorithms.ppo as ppo


def test_dont_import_torch_error():
    """Check error being thrown, if torch not installed but configured."""
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"
    config = (
        ppo.PPOConfig()
        .api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
        .environment("CartPole-v1")
        .framework("torch")
    )
    with pytest.raises(ImportError, match="However, no installation was found"):
        config.build()


if __name__ == "__main__":
    test_dont_import_torch_error()
