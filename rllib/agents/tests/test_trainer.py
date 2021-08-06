from random import choice
import unittest

import ray
import ray.rllib.agents.a3c as a3c
import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.pg as pg
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import framework_iterator


class TestTrainer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_add_delete_policy(self):
        config = pg.DEFAULT_CONFIG.copy()
        config.update({
            "env": MultiAgentCartPole,
            "env_config": {
                "config": {
                    "num_agents": 4,
                },
            },
            "num_workers": 2,  # Test on remote workers as well.
            "multiagent": {
                # Start with a single policy.
                "policies": {"p0"},
                "policy_mapping_fn": lambda aid, episode, **kwargs: "p0",
                "policy_map_capacity": 2,
            },
        })

        for _ in framework_iterator(config):
            trainer = pg.PGTrainer(config=config)
            r = trainer.train()
            self.assertTrue("p0" in r["policy_reward_min"])
            checkpoints = []
            for i in range(1, 3):

                def new_mapping_fn(agent_id, episode, **kwargs):
                    return f"p{choice([i, i - 1])}"

                # Add a new policy.
                pid = f"p{i}"
                new_pol = trainer.add_policy(
                    pid,
                    trainer._policy_class,
                    # Test changing the mapping fn.
                    policy_mapping_fn=new_mapping_fn,
                    # Change the list of policies to train.
                    policies_to_train=[f"p{i}", f"p{i-1}"],
                )
                pol_map = trainer.workers.local_worker().policy_map
                self.assertTrue(new_pol is not trainer.get_policy("p0"))
                for j in range(i):
                    self.assertTrue(f"p{j}" in pol_map)
                self.assertTrue(len(pol_map) == i + 1)
                r = trainer.train()
                self.assertTrue("p1" in r["policy_reward_min"])
                checkpoints.append(trainer.save())

                # Test restoring from the checkpoint (which has more policies
                # than what's defined in the config dict).
                test = pg.PGTrainer(config=config)
                test.restore(checkpoints[-1])
                test.train()
                # Test creating an action with the added (and restored) policy.
                a = test.compute_single_action(
                    test.get_policy("p0").observation_space.sample(),
                    policy_id=pid)
                self.assertTrue(test.get_policy("p0").action_space.contains(a))
                test.stop()

            # Delete all added policies again from trainer.
            for i in range(2, 0, -1):
                trainer.remove_policy(
                    f"p{i}",
                    policy_mapping_fn=lambda aid, eps, **kwargs: f"p{i - 1}",
                    policies_to_train=[f"p{i - 1}"])

            trainer.stop()

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
            trainer = dqn.DQNTrainer(config=config)
            # Given evaluation_interval=2, r0, r2, r4 should not contain
            # evaluation metrics, while r1, r3 should.
            r0 = trainer.train()
            print(r0)
            r1 = trainer.train()
            print(r1)
            r2 = trainer.train()
            print(r2)
            r3 = trainer.train()
            print(r3)
            trainer.stop()

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
            trainer_wo_env_on_driver = a3c.A3CTrainer(config=config)
            self.assertRaisesRegexp(
                ValueError, "Cannot evaluate w/o an evaluation worker set",
                trainer_wo_env_on_driver.evaluate)
            trainer_wo_env_on_driver.stop()

            # Try again using `create_env_on_driver=True`.
            # This force-adds the env on the local-worker, so this Trainer
            # can `evaluate` even though it doesn't have an evaluation-worker
            # set.
            config["create_env_on_driver"] = True
            trainer_w_env_on_driver = a3c.A3CTrainer(config=config)
            results = trainer_w_env_on_driver.evaluate()
            assert "evaluation" in results
            assert "episode_reward_mean" in results["evaluation"]
            trainer_w_env_on_driver.stop()
            config["create_env_on_driver"] = False


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
