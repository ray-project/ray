import gym
import numpy as np
import unittest

import ray
from ray.rllib.evaluation.collectors.simple_list_collector import SimpleListCollector


class TestCollectors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_simple_list_collector(self):
        #policy_map = {
        #    "pol1": ()
        #    "pol2",
        #}
        #ev = SimpleListCollector(
        #    policy_map=
        #    env_creator=lambda _: gym.make("CartPole-v0"),
        #    policy_spec=MockPolicy)
        batch = ev.sample()
        for key in [
                "obs", "actions", "rewards", "dones", "advantages",
                "prev_rewards", "prev_actions"
        ]:
            self.assertIn(key, batch)
            self.assertGreater(np.abs(np.mean(batch[key])), 0)

        def to_prev(vec):
            out = np.zeros_like(vec)
            for i, v in enumerate(vec):
                if i + 1 < len(out) and not batch["dones"][i]:
                    out[i + 1] = v
            return out.tolist()

        self.assertEqual(batch["prev_rewards"].tolist(),
                         to_prev(batch["rewards"]))
        self.assertEqual(batch["prev_actions"].tolist(),
                         to_prev(batch["actions"]))
        self.assertGreater(batch["advantages"][0], 1)
        ev.stop()



if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
