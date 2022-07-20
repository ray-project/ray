import unittest

from ray.rllib.env.wrappers.group_agents_wrapper import GroupAgentsWrapper
from ray.rllib.env.multi_agent_env import make_multi_agent


class TestGroupAgentsWrapper(unittest.TestCase):
    def test_group_agents_wrapper(self):
        MultiAgentCartPole = make_multi_agent("CartPole-v0")
        grouped_ma_cartpole = GroupAgentsWrapper(
            env=MultiAgentCartPole({"num_agents": 4}),
            groups={"group1": [0, 1], "group2": [2, 3]},
        )
        obs = grouped_ma_cartpole.reset()
        self.assertTrue(len(obs) == 2)
        self.assertTrue("group1" in obs and "group2" in obs)
        self.assertTrue(isinstance(obs["group1"], list) and len(obs["group1"]) == 2)
        self.assertTrue(isinstance(obs["group2"], list) and len(obs["group2"]) == 2)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
