#!/usr/bin/env python
import os
import pytest

import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.test_utils import framework_iterator


def test_dont_import_tf_error():
    """Check that an error is thrown when tf isn't installed
    but we try to run a tf experiment.
    """
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"

    config = {}
    for _ in framework_iterator(config, frameworks=("tf", "tf2", "tfe")):
        with pytest.raises(
            ImportError, match="However, there was no installation found."
        ):
            ppo.PPOTrainer(config, env="CartPole-v1")


def test_dont_import_torch_error():
    """Check that an error is thrown when torch isn't installed
    but we try to run a torch experiment.
    """
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"
    config = {"framework": "torch"}
    with pytest.raises(ImportError, match="However, there was no installation found."):
        ppo.PPOTrainer(config, env="CartPole-v1")


if __name__ == "__main__":
    test_dont_import_tf_error()
    test_dont_import_torch_error()
