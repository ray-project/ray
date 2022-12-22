#!/usr/bin/env python

import unittest

import ray

from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import framework_iterator
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy import Policy

# We need to call this here so that TF does not complain that we should have imported
# earlier
try_import_tf()


def _do_checkpoint_twice_test(framework):
    # Checks if we can load a policy from a checkpoint (at least) twice
    config = PPOConfig()
    for fw in framework_iterator(config, frameworks=[framework]):
        algo1 = config.build(env="CartPole-v1")
        algo2 = config.build(env="PongNoFrameskip-v4")

        algo1.train()
        algo2.train()

        policy1 = algo1.get_policy()
        policy1.export_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_1")

        policy2 = algo2.get_policy()
        policy2.export_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_2")

        algo1.stop()
        algo2.stop()

        # Create two policies from different checkpoints
        Policy.from_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_1")
        Policy.from_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_2")


class TestPolicyFromCheckpointTwiceTF(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_from_checkpoint_twice_tf(self):
        return _do_checkpoint_twice_test("tf")


class TestPolicyFromCheckpointTwiceTF2(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_from_checkpoint_twice_tf2(self):
        return _do_checkpoint_twice_test("tf2")


class TestPolicyFromCheckpointTwiceTorch(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_from_checkpoint_twice_torch(self):
        return _do_checkpoint_twice_test("torch")


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
