#!/usr/bin/env python
import os
import pytest

import ray.rllib.algorithms.ppo as ppo
from ray.rllib.utils.test_utils import framework_iterator


def test_dont_import_tf_error():
    """Check error being thrown, if tf not installed but configured."""
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"

    config = ppo.PPOConfig().environment("CartPole-v1")
    for _ in framework_iterator(config, frameworks=("tf", "tf2")):
        with pytest.raises(ImportError, match="However, no installation was found"):
            config.build()


def test_dont_import_torch_error():
    """Check error being thrown, if torch not installed but configured."""
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"
    config = ppo.PPOConfig().environment("CartPole-v1").framework("torch")
    with pytest.raises(ImportError, match="However, no installation was found"):
        config.build()


if __name__ == "__main__":
    test_dont_import_tf_error()
    test_dont_import_torch_error()
