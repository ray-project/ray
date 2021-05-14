import gym
import unittest

import ray
from ray.rllib.agents.dqn import DQNTrainer, DEFAULT_CONFIG as DQN_DEFAULT_CONFIG
from ray.rllib.agents.a3c import A3CTrainer
from ray.rllib.agents.dqn.dqn_tf_policy import _adjust_nstep
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.registry import register_env


class EvalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(object_store_memory=1000 * 1024 * 1024, num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dqn_n_step(self):
        obs = [1, 2, 3, 4, 5, 6, 7]
        actions = ["a", "b", "a", "a", "a", "b", "a"]
        rewards = [10.0, 0.0, 100.0, 100.0, 100.0, 100.0, 100.0]
        new_obs = [2, 3, 4, 5, 6, 7, 8]
        dones = [0, 0, 0, 0, 0, 0, 1]
        _adjust_nstep(3, 0.9, obs, actions, rewards, new_obs, dones)
        self.assertEqual(obs, [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(actions, ["a", "b", "a", "a", "a", "b", "a"])
        self.assertEqual(new_obs, [4, 5, 6, 7, 8, 8, 8])
        self.assertEqual(dones, [0, 0, 0, 0, 1, 1, 1])
        self.assertEqual(rewards,
                         [91.0, 171.0, 271.0, 271.0, 271.0, 190.0, 100.0])

    def test_evaluation_option(self):
        def env_creator(env_config):
            return gym.make("CartPole-v0")

        agent_classes = [A3CTrainer, DQNTrainer]
        DQN_DEFAULT_CONFIG["learning_starts"] = 20

        for agent_cls in agent_classes:
            for evaluation_reward_threshold, evaluation_interval in [(None, 2), (20, 3)]:
                for fw in framework_iterator(frameworks=("torch", "tf")):
                    register_env("CartPoleWrapped-v0", env_creator)
                    agent = agent_cls(
                        env="CartPoleWrapped-v0",
                        config={
                            "evaluation_interval": evaluation_interval,
                            "evaluation_reward_threshold": evaluation_reward_threshold,
                            "evaluation_num_episodes": 2,
                            "evaluation_config": {
                                "gamma": 0.98,
                                "env_config": {
                                    "fake_arg": True
                                }
                            },
                            "framework": fw,
                        })
                    if evaluation_reward_threshold is None:
                        # Given evaluation_interval=2, r0, r2, r4 should not contain
                        # evaluation metrics, while r1, r3 should.
                        r0 = agent.train()
                        r1 = agent.train()
                        r2 = agent.train()
                        r3 = agent.train()
                        agent.stop()

                        self.assertTrue("evaluation" in r1)
                        self.assertTrue("evaluation" in r3)
                        self.assertFalse("evaluation" in r0)
                        self.assertFalse("evaluation" in r2)
                        self.assertTrue("episode_reward_mean" in r1["evaluation"])
                        self.assertNotEqual(r1["evaluation"], r3["evaluation"])
                    else:
                        # Given evaluation_interval=2, r0, r2, r4 should not contain
                        # evaluation metrics, while r1, r3, r5 could.
                        # Given evaluation_reward_threshold of 32, r1 should not contain
                        # evaluation metrics, while r3 and r5
                        r0 = agent.train()
                        r1 = agent.train()
                        r2 = agent.train()
                        r3 = agent.train()
                        r4 = agent.train()
                        r5 = agent.train()
                        agent.stop()

                        self.assertFalse("evaluation" in r0)
                        self.assertFalse("evaluation" in r1)
                        self.assertFalse("evaluation" in r3)
                        self.assertFalse("evaluation" in r4)
                        r2_episode_reward_mean = r2["episode_reward_mean"]
                        self.assertTrue("evaluation" in r2,
                                        f"{r2_episode_reward_mean} "
                                        f"< {evaluation_reward_threshold}")
                        self.assertTrue("evaluation" in r5)
                        self.assertTrue("episode_reward_mean" in r2["evaluation"])
                        self.assertTrue("episode_reward_mean" in r5["evaluation"])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
