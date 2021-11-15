#!/usr/bin/env python
import os

import pytest
import ray.rllib.agents.ppo as ppo


def test_dont_import_tf_error():
    """Check that an error is thrown when tf isn't installed
        but we try to run a tf experiment.
    """
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"
    tf_import_error_string = (
        "TensorFlow was specified as the 'framework' "
        "inside of your config dictionary. However, there was "
        "no installation found. You can install tensorflow via"
        " pip: pip install tensorflow")
    with pytest.raises(ImportError, match=tf_import_error_string):
        trainer = ppo.PPOTrainer(env="CartPole-v1")
        trainer.train()


def test_dont_import_torch_error():
    """Check that an error is thrown when torch isn't installed
        but we try to run a torch experiment.
    """
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"
    torch_import_error_string = (
        "torch was specified as the 'framework' inside "
        "of your config dictionary. However, there was no "
        "installation found. You can install torch via "
        "pip: pip install torch")
    with pytest.raises(ImportError, match=torch_import_error_string):
        trainer = ppo.PPOTrainer(
            env="CartPole-v1", config={"framework": "torch"})
        trainer.train()


if __name__ == "__main__":
    test_dont_import_tf_error()
    test_dont_import_torch_error()
