import unittest


class TestKaggleFootballMultiAgentEnv(unittest.TestCase):
    def test_football_env(self):
        from ray.rllib.env.wrappers.kaggle_wrapper import KaggleFootballMultiAgentEnv

        env = KaggleFootballMultiAgentEnv()
        obs, info = env.reset()
        self.assertEqual(list(obs.keys()), ["agent0", "agent1"])
        action_dict = {"agent0": 0, "agent1": 0}
        obs, reward, done, truncated, info = env.step(action_dict)
        self.assertEqual(list(obs.keys()), ["agent0", "agent1"])
        self.assertEqual(reward, {"agent0": 0, "agent1": 0})
        self.assertEqual(
            done,
            {
                "agent0": False,
                "agent1": False,
                "__all__": False,
            },
        )
        self.assertEqual(info, {"agent0": {}, "agent1": {}})

    def test_football_env_run_30_steps(self):
        from ray.rllib.env.wrappers.kaggle_wrapper import KaggleFootballMultiAgentEnv

        env = KaggleFootballMultiAgentEnv()

        # use the built-in agents in the kaggle environment
        run_right_agent = env.kaggle_env.agents["run_right"]
        do_nothing_agent = env.kaggle_env.agents["do_nothing"]

        obs, info = env.reset()
        self.assertEqual(list(obs.keys()), ["agent0", "agent1"])
        done = {"__all__": False}
        num_steps_completed = 0
        while not done["__all__"] and num_steps_completed <= 30:
            action0 = run_right_agent(structify(obs["agent0"]))[0]
            action1 = do_nothing_agent(structify(obs["agent1"]))[0]
            action_dict = {"agent0": action0, "agent1": action1}
            obs, _, done, truncated, _ = env.step(action_dict)
            num_steps_completed += 1

    def test_kaggle_football_agent_spaces(self):
        from ray.rllib.env.wrappers.kaggle_wrapper import KaggleFootballMultiAgentEnv

        env = KaggleFootballMultiAgentEnv()
        obs, info = env.reset()
        action_space, obs_space = env.build_agent_spaces()
        self.assertTrue(obs_space.contains(obs["agent0"]))
        self.assertTrue(obs_space.contains(obs["agent1"]))

        action_dict = {
            "agent0": action_space.sample(),
            "agent1": action_space.sample(),
        }
        obs, _, _, _, _ = env.step(action_dict)
        self.assertTrue(obs_space.contains(obs["agent0"]))
        self.assertTrue(obs_space.contains(obs["agent1"]))


if __name__ == "__main__":
    from kaggle_environments.utils import structify
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
