import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


class TestPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_get_and_set_state(self):
        config = PPOConfig()
        for fw in framework_iterator(config):
            algo = config.build(env="CartPole-v1")
            policy = algo.get_policy()
            state1 = policy.get_state()
            algo.train()
            state2 = policy.get_state()
            check(state1["global_timestep"], state2["global_timestep"], false=True)

            # Reset policy to its original state and compare.
            policy.set_state(state1)
            state3 = policy.get_state()
            # Make sure everything is the same.
            check(state1["_exploration_state"], state3["_exploration_state"])
            check(state1["global_timestep"], state3["global_timestep"])
            check(state1["weights"], state3["weights"])

            # Create a new Policy only from state (which could be part of an algorithm's
            # checkpoint). This would allow users to restore a policy w/o having access
            # to the original code (e.g. the config, policy class used, etc..).
            if isinstance(policy, (EagerTFPolicyV2, DynamicTFPolicyV2, TorchPolicyV2)):
                policy_restored_from_scratch = Policy.from_state(state3)
                state4 = policy_restored_from_scratch.get_state()
                check(state3["_exploration_state"], state4["_exploration_state"])
                check(state3["global_timestep"], state4["global_timestep"])
                # For tf static graph, the new model has different layer names
                # (as it gets written into the same graph as the old one).
                if fw != "tf":
                    check(state3["weights"], state4["weights"])


def _do_test_from_checkpoint_twice(framework):
    # Checks if we can load a policy from a checkpoint (at least) twice
    config = PPOConfig().framework(framework=framework)
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
        return _do_test_from_checkpoint_twice("tf")


class TestPolicyFromCheckpointTwiceTF2(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_from_checkpoint_twice_tf2(self):
        return _do_test_from_checkpoint_twice("tf2")


class TestPolicyFromCheckpointTwiceTorch(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_from_checkpoint_twice_torch(self):
        return _do_test_from_checkpoint_twice("torch")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
