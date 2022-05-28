import copy
import gym
import numpy as np
import os
from pathlib import Path
from random import choice
import time
import unittest

import ray
import ray.rllib.agents.a3c as a3c
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.algorithms.marwil import BCConfig, BCTrainer
import ray.rllib.algorithms.pg as pg
from ray.rllib.agents.trainer import COMMON_CONFIG
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.examples.parallel_evaluation_and_training import AssertEvalCallback
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check, framework_iterator


class TestTrainer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_validate_config_idempotent(self):
        """
        Asserts that validate_config run multiple
        times on COMMON_CONFIG will be idempotent
        """
        # Given:
        standard_config = copy.deepcopy(COMMON_CONFIG)
        trainer = pg.PGTrainer(env="CartPole-v0", config=standard_config)

        # When (we validate config 2 times).
        # Try deprecated `Trainer._validate_config()` method (static).
        trainer._validate_config(standard_config, trainer)
        config_v1 = copy.deepcopy(standard_config)
        # Try new method: `Trainer.validate_config()` (non-static).
        trainer.validate_config(standard_config)
        config_v2 = copy.deepcopy(standard_config)

        # Make sure nothing changed.
        self.assertEqual(config_v1, config_v2)

        trainer.stop()

    def test_add_delete_policy(self):
        config = pg.DEFAULT_CONFIG.copy()
        config.update(
            {
                "env": MultiAgentCartPole,
                "env_config": {
                    "config": {
                        "num_agents": 4,
                    },
                },
                "num_workers": 2,  # Test on remote workers as well.
                "num_cpus_per_worker": 0.1,
                "model": {
                    "fcnet_hiddens": [5],
                    "fcnet_activation": "linear",
                },
                "train_batch_size": 100,
                "rollout_fragment_length": 50,
                "multiagent": {
                    # Start with a single policy.
                    "policies": {"p0"},
                    "policy_mapping_fn": lambda aid, eps, worker, **kwargs: "p0",
                    # And only two policies that can be stored in memory at a
                    # time.
                    "policy_map_capacity": 2,
                },
                "evaluation_num_workers": 1,
                "evaluation_config": {
                    "num_cpus_per_worker": 0.1,
                },
            }
        )

        for _ in framework_iterator(config):
            trainer = pg.PGTrainer(config=config)
            pol0 = trainer.get_policy("p0")
            r = trainer.train()
            self.assertTrue("p0" in r["info"][LEARNER_INFO])
            for i in range(1, 3):

                def new_mapping_fn(agent_id, episode, worker, **kwargs):
                    return f"p{choice([i, i - 1])}"

                # Add a new policy.
                pid = f"p{i}"
                new_pol = trainer.add_policy(
                    pid,
                    trainer.get_default_policy_class(config),
                    # Test changing the mapping fn.
                    policy_mapping_fn=new_mapping_fn,
                    # Change the list of policies to train.
                    policies_to_train=[f"p{i}", f"p{i-1}"],
                )
                pol_map = trainer.workers.local_worker().policy_map
                self.assertTrue(new_pol is not pol0)
                for j in range(i + 1):
                    self.assertTrue(f"p{j}" in pol_map)
                self.assertTrue(len(pol_map) == i + 1)
                trainer.train()
                checkpoint = trainer.save()

                # Test restoring from the checkpoint (which has more policies
                # than what's defined in the config dict).
                test = pg.PGTrainer(config=config)
                test.restore(checkpoint)

                # Make sure evaluation worker also gets the restored policy.
                def _has_policy(w):
                    return w.get_policy("p0") is not None

                self.assertTrue(
                    all(test.evaluation_workers.foreach_worker(_has_policy))
                )

                # Make sure trainer can continue training the restored policy.
                pol0 = test.get_policy("p0")
                test.train()
                # Test creating an action with the added (and restored) policy.
                a = test.compute_single_action(
                    np.zeros_like(pol0.observation_space.sample()), policy_id=pid
                )
                self.assertTrue(pol0.action_space.contains(a))
                test.stop()

            # Delete all added policies again from trainer.
            for i in range(2, 0, -1):
                trainer.remove_policy(
                    f"p{i}",
                    # Note that the complete signature of a policy_mapping_fn
                    # is: `agent_id, episode, worker, **kwargs`.
                    policy_mapping_fn=lambda aid, eps, **kwargs: f"p{i - 1}",
                    policies_to_train=[f"p{i - 1}"],
                )

            trainer.stop()

    def test_evaluation_option(self):
        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation.
        config = (
            dqn.DQNConfig()
            .environment(env="CartPole-v0")
            .evaluation(
                evaluation_interval=2,
                evaluation_duration=2,
                evaluation_duration_unit="episodes",
                evaluation_config={
                    "gamma": 0.98,
                },
            )
            .callbacks(callbacks_class=AssertEvalCallback)
        )

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = config.build()
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

    def test_evaluation_option_always_attach_eval_metrics(self):
        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation.
        config = (
            dqn.DQNConfig()
            .environment(env="CartPole-v0")
            .evaluation(
                evaluation_interval=2,
                evaluation_duration=2,
                evaluation_duration_unit="episodes",
                evaluation_config={
                    "gamma": 0.98,
                },
                always_attach_evaluation_results=True,
            )
            .callbacks(callbacks_class=AssertEvalCallback)
        )
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = config.build()
            # Should always see latest available eval results.
            r0 = trainer.train()
            r1 = trainer.train()
            r2 = trainer.train()
            r3 = trainer.train()
            trainer.stop()

            # Eval results are not available at step 0.
            # But step 3 should still have it, even though no eval was
            # run during that step.
            self.assertTrue("evaluation" in r0)
            self.assertTrue("evaluation" in r1)
            self.assertTrue("evaluation" in r2)
            self.assertTrue("evaluation" in r3)

    def test_evaluation_wo_evaluation_worker_set(self):
        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation.
        config = (
            a3c.A3CConfig()
            .environment(env="CartPole-v0")
            .callbacks(callbacks_class=AssertEvalCallback)
        )

        for _ in framework_iterator(frameworks=("tf", "torch")):
            # Setup trainer w/o evaluation worker set and still call
            # evaluate() -> Expect error.
            trainer_wo_env_on_driver = config.build()
            self.assertRaisesRegex(
                ValueError,
                "Cannot evaluate w/o an evaluation worker set",
                trainer_wo_env_on_driver.evaluate,
            )
            trainer_wo_env_on_driver.stop()

            # Try again using `create_env_on_driver=True`.
            # This force-adds the env on the local-worker, so this Trainer
            # can `evaluate` even though it doesn't have an evaluation-worker
            # set.
            config.create_env_on_local_worker = True
            trainer_w_env_on_driver = config.build()
            results = trainer_w_env_on_driver.evaluate()
            assert "evaluation" in results
            assert "episode_reward_mean" in results["evaluation"]
            trainer_w_env_on_driver.stop()
            config.create_env_on_local_worker = False

    def test_space_inference_from_remote_workers(self):
        # Expect to not do space inference if the learner has an env.

        env = gym.make("CartPole-v0")

        config = (
            pg.PGConfig()
            .rollouts(num_rollout_workers=1, validate_workers_after_construction=False)
            .environment(env="CartPole-v0")
        )

        # No env on driver -> expect longer build time due to space
        # lookup from remote worker.
        t0 = time.time()
        trainer = config.build()
        w_lookup = time.time() - t0
        print(f"No env on learner: {w_lookup}sec")
        trainer.stop()

        # Env on driver -> expect shorted build time due to no space
        # lookup required from remote worker.
        config.create_env_on_local_worker = True
        t0 = time.time()
        trainer = config.build()
        wo_lookup = time.time() - t0
        print(f"Env on learner: {wo_lookup}sec")
        self.assertLess(wo_lookup, w_lookup)
        trainer.stop()

        # Spaces given -> expect shorter build time due to no space
        # lookup required from remote worker.
        config.create_env_on_driver = False
        config.environment(
            observation_space=env.observation_space,
            action_space=env.action_space,
        )
        t0 = time.time()
        trainer = config.build()
        wo_lookup = time.time() - t0
        print(f"Spaces given manually in config: {wo_lookup}sec")
        self.assertLess(wo_lookup, w_lookup)
        trainer.stop()

    def test_worker_validation_time(self):
        """Tests the time taken by `validate_workers_after_construction=True`."""
        config = pg.PGConfig().environment(env="CartPole-v0")
        config.validate_workers_after_construction = True

        # Test, whether validating one worker takes just as long as validating
        # >> 1 workers.
        config.num_workers = 1
        t0 = time.time()
        trainer = config.build()
        total_time_1 = time.time() - t0
        print(f"Validating w/ 1 worker: {total_time_1}sec")
        trainer.stop()

        config.num_workers = 5
        t0 = time.time()
        trainer = config.build()
        total_time_5 = time.time() - t0
        print(f"Validating w/ 5 workers: {total_time_5}sec")
        trainer.stop()

        check(total_time_5 / total_time_1, 1.0, atol=1.0)

    def test_no_env_but_eval_workers_do_have_env(self):
        """Tests whether no env on workers, but env on eval workers works ok."""
        script_path = Path(__file__)
        input_file = os.path.join(
            script_path.parent.parent.parent, "tests/data/cartpole/small.json"
        )

        env = gym.make("CartPole-v0")

        offline_rl_config = (
            BCConfig()
            .environment(
                observation_space=env.observation_space, action_space=env.action_space
            )
            .evaluation(
                evaluation_interval=1,
                evaluation_num_workers=1,
                evaluation_config={
                    "env": "CartPole-v0",
                    "input": "sampler",
                    "observation_space": None,  # Test, whether this is inferred.
                    "action_space": None,  # Test, whether this is inferred.
                },
            )
            .offline_data(input_=[input_file])
        )

        bc_trainer = BCTrainer(config=offline_rl_config)
        bc_trainer.train()
        bc_trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
