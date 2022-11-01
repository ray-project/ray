import gym
import numpy as np
import tree  # pip install dm_tree
import unittest

import ray
from ray.rllib.algorithms.appo import APPOConfig, APPOTF2Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.utils.test_utils import check, framework_iterator
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
        config = APPOConfig().framework("tf2", eager_tracing=True).resources(num_gpus=1)
        obs_space = gym.spaces.Box(-1.0, 1.0, (4,), dtype=np.float32)
        dummy_obs = obs_space.sample()
        act_space = gym.spaces.Discrete(2)
        num_policies = 2
        capacity = 1

        for fw in framework_iterator(config):
            cls = get_tf_eager_cls_if_necessary(APPOTF2Policy, config)

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

            dummy_batch = tree.map_structure(
                lambda s: np.ones_like(s),
                policy_map["pol0"]._dummy_batch,
            )

            logits = {
                pid: p.compute_single_action(dummy_obs)[2]["action_dist_inputs"]
                for pid, p in policy_map.items()
            }
            # Make sure policies output different deterministic logits. Otherwise,
            # this test would not work.
            check(logits["pol0"], logits["pol1"], false=True)

            # Test proper policy state swapping.
            for i in range(5):
                pid = f"pol{i % num_policies}"
                print(i)
                pol = policy_map[pid]
                # Make sure config has been changed properly.
                self.assertTrue(pol.config["lr"] == ((i % num_policies) + 1) * 0.00001)
                # After accessing `pid`, assume it's the most recently accessed item now.
                self.assertTrue(policy_map.deque[-1] == pid)
                self.assertTrue(len(policy_map.deque) == capacity)
                self.assertTrue(len(policy_map.cache) == capacity)
                self.assertTrue(pid in policy_map.cache)
                # Actually compute one action to trigger tracing operations of the graph.
                # These may be performed lazily by the DL framework.
                check(
                    pol.compute_single_action(dummy_obs)[2]["action_dist_inputs"],
                    logits[pid],
                )

            # Test, whether training (on the GPU) will affect the state swapping.
            for i in range(num_policies):
                pid = f"pol{i % num_policies}"
                pol = policy_map[pid]
                pol.learn_on_batch(dummy_batch)

                # Make sure, we really changed the NN during training and update our logits
                # dict.
                old_logits = logits[pid]
                logits[pid] = pol.compute_single_action(dummy_obs)[2]["action_dist_inputs"]
                check(logits[pid], old_logits, false=True)

            # Make sure policies output different deterministic logits. Otherwise,
            # this test would not work.
            check(logits["pol0"], logits["pol1"], false=True)

            # Once more, test proper policy state swapping.
            for i in range(5):
                pid = f"pol{i % num_policies}"
                pol = policy_map[pid]
                check(
                    pol.compute_single_action(dummy_obs)[2]["action_dist_inputs"],
                    logits[pid],
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
