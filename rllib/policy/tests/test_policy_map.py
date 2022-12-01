import gymnasium as gym
import numpy as np
import time
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig, PPOTF2Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.tf_utils import get_tf_eager_cls_if_necessary


class TestPolicyMap(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_map(self):
        config = PPOConfig().framework("tf2", eager_tracing=True)
        obs_space = gym.spaces.Box(-1.0, 1.0, (4,), dtype=np.float32)
        dummy_obs = obs_space.sample()
        act_space = gym.spaces.Discrete(10000)
        num_policies = 6
        capacity = 2

        cls = get_tf_eager_cls_if_necessary(PPOTF2Policy, config)

        # Create empty PolicyMap.
        for use_swapping in [False, True]:
            policy_map = PolicyMap(
                capacity=capacity, policy_states_are_swappable=use_swapping
            )

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
                self.assertEqual(list(policy_map._deque), expected)
                self.assertEqual(list(policy_map.cache.keys()), expected)
                self.assertEqual(
                    policy_map._valid_keys, {f"pol{j}" for j in range(i + 1)}
                )

            actions = {
                pid: p.compute_single_action(dummy_obs, explore=False)[0]
                for pid, p in policy_map.items()
            }

            # Time the random access performance of our map.
            start = time.time()
            for i in range(50):
                pid = f"pol{i % num_policies}"
                # Actually compute one action to trigger tracing operations of the
                # graph. These may be performed lazily by the DL framework.
                print(
                    f"{i}) Testing `compute_single_action()` resulting in same outputs "
                    f"for stashed/recovered policy ({pid}) ..."
                )
                pol = policy_map[pid]
                # After accessing `pid`, assume it's the most recently accessed item
                # now.
                self.assertTrue(policy_map._deque[-1] == pid)
                self.assertTrue(len(policy_map._deque) == 2)
                self.assertTrue(len(policy_map.cache) == 2)
                self.assertTrue(pid in policy_map.cache)
                check(
                    pol.compute_single_action(dummy_obs, explore=False)[0], actions[pid]
                )

            time_total = time.time() - start
            print(f"Random access (swapping={use_swapping} took {time_total}sec.")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
