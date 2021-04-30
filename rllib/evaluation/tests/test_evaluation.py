import unittest

import ray
import ray.rllib.agents.a3c as a3c
import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.test_utils import framework_iterator


class TestTrainerEvaluation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_evaluation_option(self):
        config = dqn.DEFAULT_CONFIG.copy()
        config.update({
            "env": "CartPole-v0",
            "evaluation_interval": 2,
            "evaluation_num_episodes": 2,
            "evaluation_config": {
                "gamma": 0.98,
            }
        })

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            agent = dqn.DQNTrainer(config=config)
            # Given evaluation_interval=2, r0, r2, r4 should not contain
            # evaluation metrics, while r1, r3 should.
            r0 = agent.train()
            print(r0)
            r1 = agent.train()
            print(r1)
            r2 = agent.train()
            print(r2)
            r3 = agent.train()
            print(r3)
            agent.stop()

            self.assertFalse("evaluation" in r0)
            self.assertTrue("evaluation" in r1)
            self.assertFalse("evaluation" in r2)
            self.assertTrue("evaluation" in r3)
            self.assertTrue("episode_reward_mean" in r1["evaluation"])
            self.assertNotEqual(r1["evaluation"], r3["evaluation"])

    def test_evaluation_wo_evaluation_worker_set(self):
        config = a3c.DEFAULT_CONFIG.copy()
        config.update({
            "env": "CartPole-v0",
            # Switch off evaluation (this should already be the default).
            "evaluation_interval": None,
        })
        for _ in framework_iterator(frameworks=("tf", "torch")):
            # Setup trainer w/o evaluation worker set and still call
            # evaluate() -> Expect error.
            agent_wo_env_on_driver = a3c.A3CTrainer(config=config)
            self.assertRaisesRegexp(
                ValueError, "Cannot evaluate w/o an evaluation worker set",
                agent_wo_env_on_driver.evaluate)
            agent_wo_env_on_driver.stop()

            # Try again using `create_env_on_driver=True`.
            # This force-adds the env on the local-worker, so this Trainer
            # can `evaluate` even though, it doesn't have an evaluation-worker
            # set.
            config["create_env_on_driver"] = True
            agent_w_env_on_driver = a3c.A3CTrainer(config=config)
            results = agent_w_env_on_driver.evaluate()
            assert "evaluation" in results
            assert "episode_reward_mean" in results["evaluation"]
            agent_w_env_on_driver.stop()
            config["create_env_on_driver"] = False


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
