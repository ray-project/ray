import gym
import numpy as np
import time
from typing import Optional
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig, PPOTF2Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.tf_utils import get_tf_eager_cls_if_necessary

TIME_NO_SWAPS: Optional[float] = None
TIME_SWAPS: Optional[float] = None


class TestPolicyMap(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_map_no_swaps(self):
        config = PPOConfig().framework("tf2", eager_tracing=True)
        obs_space = gym.spaces.Box(-1.0, 1.0, (4,), dtype=np.float32)
        dummy_obs = obs_space.sample()
        act_space = gym.spaces.Discrete(2)
        num_policies = 6

        cls = get_tf_eager_cls_if_necessary(PPOTF2Policy, config)

        # Create empty PolicyMap.
        policy_map = PolicyMap(capacity=2)

        # Create and add some TF2 policies.
        for i in range(num_policies):
            config.training(lr=(i + 1) * 0.00001)
            policy = cls(
                observation_space=obs_space,
                action_space=act_space,
                config=config.to_dict(),
            )
            policy_map[f"pol{i}"] = policy
            expected = [f"pol{j}" for j in range(max(i - 1, 0), i + 1)]
            self.assertEqual(list(policy_map.deque), expected)
            self.assertEqual(list(policy_map.cache.keys()), expected)
            self.assertEqual(policy_map.valid_keys, {f"pol{j}" for j in range(i + 1)})

        logits = {
            pid: p.compute_single_action(dummy_obs)[2]["action_dist_inputs"]
            for pid, p in policy_map.items()
        }

        # Time the random access performance of our map.
        start = time.time()
        for i in range(50):
            pid = f"pol{i % num_policies}"
            # Actually comptue one action to trigger tracing operations of the graph.
            # These may be performed lazily by the DL framework.
            print(i)
            pol = policy_map[pid]
            # After accessing `pid`, assume it's the most recently accessed item now.
            self.assertTrue(policy_map.deque[-1] == pid)
            self.assertTrue(len(policy_map.deque) == 2)
            self.assertTrue(len(policy_map.cache) == 2)
            self.assertTrue(pid in policy_map.cache)
            check(
                pol.compute_single_action(dummy_obs)[2]["action_dist_inputs"],
                logits[pid],
            )

        global TIME_NO_SWAPS
        global TIME_SWAPS
        TIME_NO_SWAPS = time.time() - start
        print(f"Random access w/o swapping took {TIME_NO_SWAPS}sec.")
        if TIME_SWAPS is not None:
            self.assertTrue(TIME_NO_SWAPS >= 10 * TIME_SWAPS)

    def test_policy_map_with_swaps(self):
        config = PPOConfig().framework("tf2", eager_tracing=True)
        obs_space = gym.spaces.Box(-1.0, 1.0, (4,), dtype=np.float32)
        dummy_obs = obs_space.sample()
        act_space = gym.spaces.Discrete(2)
        num_policies = 6

        cls = get_tf_eager_cls_if_necessary(PPOTF2Policy, config)

        # Create empty, swappable-policies PolicyMap.
        policy_map = PolicyMap(capacity=2, policies_swappable=True)

        # Create and add some TF2 policies.
        for i in range(num_policies):
            config.training(lr=(i + 1) * 0.00001)
            policy = cls(
                observation_space=obs_space,
                action_space=act_space,
                config=config.to_dict(),
            )
            policy_map[f"pol{i}"] = policy
            expected = [f"pol{j}" for j in range(max(i - 1, 0), i + 1)]
            self.assertEqual(list(policy_map.deque), expected)
            self.assertEqual(list(policy_map.cache.keys()), expected)
            self.assertEqual(policy_map.valid_keys, {f"pol{j}" for j in range(i + 1)})

        logits = {
            pid: p.compute_single_action(dummy_obs)[2]["action_dist_inputs"]
            for pid, p in policy_map.items()
        }

        # Time the random access performance of our map.
        start = time.time()
        for i in range(50):
            pid = f"pol{i % num_policies}"
            # Actually comptue one action to trigger tracing operations of the graph.
            # These may be performed lazily by the DL framework.
            print(i)
            pol = policy_map[pid]
            # After accessing `pid`, assume it's the most recently accessed item now.
            self.assertTrue(policy_map.deque[-1] == pid)
            self.assertTrue(len(policy_map.deque) == 2)
            self.assertTrue(len(policy_map.cache) == 2)
            self.assertTrue(pid in policy_map.cache)
            check(
                pol.compute_single_action(dummy_obs)[2]["action_dist_inputs"],
                logits[pid],
            )

        global TIME_NO_SWAPS
        global TIME_SWAPS
        TIME_SWAPS = time.time() - start
        print(f"Random access w/ swapping took {TIME_SWAPS}sec.")
        if TIME_NO_SWAPS is not None:
            self.assertTrue(TIME_NO_SWAPS >= 10 * TIME_SWAPS)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
