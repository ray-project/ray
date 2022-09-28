import gym
import numpy as np
import time
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig, PPOTF2Policy
from ray.rllib.policy.policy_map import PolicyMap


class TestPolicyMap(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_map(self):
        config_dict = PPOConfig().framework("tf2", eager_tracing=True).to_dict()
        obs_space = gym.spaces.Box(-1.0, 1.0, (4,), dtype=np.float32)
        act_space = gym.spaces.Discrete(2)
        num_policies = 6

        # Create empty PolicyMap.
        policy_map = PolicyMap(
            worker_index=0,
            num_workers=0,
            capacity=2,
            policy_config=config_dict,
        )
        # Create and add some policies.
        for i in range(num_policies):
            policy_map.create_policy(
                policy_id=f"pol{i}",
                policy_cls=PPOTF2Policy,
                observation_space=obs_space,
                action_space=act_space,
                config_override={"lr": (i+1) * 0.00001},
                merged_config=config_dict,
            )
        # Time the random access performance of our map.
        start = time.time()
        for i in range(100):
            policy_id_to_grab = f"pol{i % num_policies}"
            # Actually comptue one action to trigger tracing operations of the graph.
            # These may be performed lazily by the DL framework.
            print(i)
            pol = policy_map[policy_id_to_grab]
            pol.compute_single_action(obs_space.sample())
        print(f"Random access took {time.time() - start}sec.")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
