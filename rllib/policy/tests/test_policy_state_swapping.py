import gym
import numpy as np
import time
from typing import Optional
import unittest

import ray
from ray.rllib.algorithms.appo import APPOConfig, APPOTF2Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.tf_utils import get_tf_eager_cls_if_necessary


class TestPolicyStateSwapping(unittest.TestCase):
    """Tests, whether Policies can be "swapped out" via their state on a GPU."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_swap_gpu(self):
        config = APPOConfig().framework("tf2", eager_tracing=True).resources(num_gpus=0)
        obs_space = gym.spaces.Box(-1.0, 1.0, (4,), dtype=np.float32)
        dummy_obs = obs_space.sample()
        act_space = gym.spaces.Discrete(2)
        num_policies = 2
        capacity = 1

        cls = get_tf_eager_cls_if_necessary(APPOTF2Policy, config.to_dict())

        # Create empty, swappable-policies PolicyMap.
        policy_map = PolicyMap(capacity=capacity, policies_swappable=True)

        # Create and add some TF2 policies.
        for i in range(num_policies):
            config.training(lr=(i + 1) * 0.00001)
            policy = cls(
                observation_space=obs_space,
                action_space=act_space,
                config=config.to_dict(),
            )
            policy_map[f"pol{i}"] = policy

        logits = {
            pid: p.compute_single_action(dummy_obs)[2]["action_dist_inputs"]
            for pid, p in policy_map.items()
        }
        # Make sure policies output different deterministic logits. Otherwise,
        # this test would not work.
        check(logits["pol0"], logits["pol1"], false=True)

        # Time the random access performance of our map.
        for i in range(50):
            pid = f"pol{i % num_policies}"
            # Actually comptue one action to trigger tracing operations of the graph.
            # These may be performed lazily by the DL framework.
            print(i)
            pol = policy_map[pid]
            # Make sure config has been changed properly.
            self.assertTrue(pol.config["lr"] == (i + 1) * 0.00001)
            # After accessing `pid`, assume it's the most recently accessed item now.
            self.assertTrue(policy_map.deque[-1] == pid)
            self.assertTrue(len(policy_map.deque) == capacity)
            self.assertTrue(len(policy_map.cache) == capacity)
            self.assertTrue(pid in policy_map.cache)
            check(
                pol.compute_single_action(dummy_obs)[2]["action_dist_inputs"],
                logits[pid],
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
