import gymnasium as gym
import numpy as np
import os
from pathlib import Path
from random import choice
import time
import unittest

import ray
import ray.rllib.algorithms.a3c as a3c
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.algorithms.bc import BCConfig
import ray.rllib.algorithms.pg as pg
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.examples.parallel_evaluation_and_training import AssertEvalCallback
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check, framework_iterator


class TestAlgorithm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_add_delete_policy(self):
        config = pg.PGConfig()
        config.environment(
            env=MultiAgentCartPole,
            env_config={
                "config": {
                    "num_agents": 4,
                },
            },
        ).rollouts(num_rollout_workers=2, rollout_fragment_length=50).resources(
            num_cpus_per_worker=0.1
        ).training(
            train_batch_size=100,
        ).multi_agent(
            # Start with a single policy.
            policies={"p0"},
            policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "p0",
            # And only two policies that can be stored in memory at a
            # time.
            policy_map_capacity=2,
        ).evaluation(
            evaluation_num_workers=1,
            evaluation_config=pg.PGConfig.overrides(num_cpus_per_worker=0.1),
        )
        # Don't override existing model settings.
        config.model.update(
            {
                "fcnet_hiddens": [5],
                "fcnet_activation": "linear",
            }
        )

        obs_space = gym.spaces.Box(-2.0, 2.0, (4,))
        act_space = gym.spaces.Discrete(2)

        for fw in framework_iterator(config):
            # Pre-generate a policy instance to test adding these directly to an
            # existing algorithm.
            if fw == "tf":
                policy_obj = pg.PGTF1Policy(obs_space, act_space, config.to_dict())
            elif fw == "tf2":
                policy_obj = pg.PGTF2Policy(obs_space, act_space, config.to_dict())
            else:
                policy_obj = pg.PGTorchPolicy(obs_space, act_space, config.to_dict())

            # Construct the Algorithm with a single policy in it.
            algo = config.build()
            pol0 = algo.get_policy("p0")
            r = algo.train()
            self.assertTrue("p0" in r["info"][LEARNER_INFO])
            for i in range(1, 3):

                def new_mapping_fn(agent_id, episode, worker, **kwargs):
                    return f"p{choice([i, i - 1])}"

                # Add a new policy either by class (and options) or by instance.
                pid = f"p{i}"
                print(f"Adding policy {pid} ...")
                # By instance.
                if i == 2:
                    new_pol = algo.add_policy(
                        pid,
                        # Pass in an already existing policy instance.
                        policy=policy_obj,
                        # Test changing the mapping fn.
                        policy_mapping_fn=new_mapping_fn,
                        # Change the list of policies to train.
                        policies_to_train=[f"p{i}", f"p{i - 1}"],
                    )
                # By class (and options).
                else:
                    new_pol = algo.add_policy(
                        pid,
                        algo.get_default_policy_class(config),
                        # Test changing the mapping fn.
                        policy_mapping_fn=new_mapping_fn,
                        # Change the list of policies to train.
                        policies_to_train=[f"p{i}", f"p{i-1}"],
                    )

                # Make sure new policy is part of remote workers in the
                # worker set and the eval worker set.
                self.assertTrue(
                    algo.workers.foreach_worker(func=lambda w: pid in w.policy_map)[0]
                )
                self.assertTrue(
                    algo.evaluation_workers.foreach_worker(
                        func=lambda w: pid in w.policy_map
                    )[0]
                )

                # Assert new policy is part of local worker (eval worker set does NOT
                # have a local worker, only the main WorkerSet does).
                pol_map = algo.workers.local_worker().policy_map
                self.assertTrue(new_pol is not pol0)
                for j in range(i + 1):
                    self.assertTrue(f"p{j}" in pol_map)
                self.assertTrue(len(pol_map) == i + 1)
                algo.train()
                checkpoint = algo.save()

                # Test restoring from the checkpoint (which has more policies
                # than what's defined in the config dict).
                test = pg.PG.from_checkpoint(checkpoint)

                # Make sure evaluation worker also got the restored, added policy.
                def _has_policies(w):
                    return (
                        w.get_policy("p0") is not None and w.get_policy(pid) is not None
                    )

                self.assertTrue(
                    all(test.evaluation_workers.foreach_worker(_has_policies))
                )

                # Make sure algorithm can continue training the restored policy.
                pol0 = test.get_policy("p0")
                test.train()
                # Test creating an action with the added (and restored) policy.
                a = test.compute_single_action(
                    np.zeros_like(pol0.observation_space.sample()), policy_id=pid
                )
                self.assertTrue(pol0.action_space.contains(a))
                test.stop()

                # After having added 2 policies, try to restore the Algorithm,
                # but only with 1 of the originally added policies (plus the initial
                # p0).
                if i == 2:

                    def new_mapping_fn(agent_id, episode, worker, **kwargs):
                        return f"p{choice([0, 2])}"

                    test2 = pg.PG.from_checkpoint(
                        checkpoint=checkpoint,
                        policy_ids=["p0", "p2"],
                        policy_mapping_fn=new_mapping_fn,
                        policies_to_train=["p0"],
                    )

                    # Make sure evaluation workers have the same policies.
                    def _has_policies(w):
                        return (
                            w.get_policy("p0") is not None
                            and w.get_policy("p2") is not None
                            and w.get_policy("p1") is None
                        )

                    self.assertTrue(
                        all(test2.evaluation_workers.foreach_worker(_has_policies))
                    )

                    # Make sure algorithm can continue training the restored policy.
                    pol2 = test2.get_policy("p2")
                    test2.train()
                    # Test creating an action with the added (and restored) policy.
                    a = test2.compute_single_action(
                        np.zeros_like(pol2.observation_space.sample()), policy_id=pid
                    )
                    self.assertTrue(pol2.action_space.contains(a))
                    test2.stop()

            # Delete all added policies again from Algorithm.
            for i in range(2, 0, -1):
                pid = f"p{i}"
                algo.remove_policy(
                    pid,
                    # Note that the complete signature of a policy_mapping_fn
                    # is: `agent_id, episode, worker, **kwargs`.
                    policy_mapping_fn=(
                        lambda agent_id, episode, worker, **kwargs: f"p{i - 1}"
                    ),
                    # Update list of policies to train.
                    policies_to_train=[f"p{i - 1}"],
                )
                # Make sure removed policy is no longer part of remote workers in the
                # worker set and the eval worker set.
                self.assertTrue(
                    algo.workers.foreach_worker(func=lambda w: pid not in w.policy_map)[
                        0
                    ]
                )
                self.assertTrue(
                    algo.evaluation_workers.foreach_worker(
                        func=lambda w: pid not in w.policy_map
                    )[0]
                )
                # Assert removed policy is no longer part of local worker
                # (eval worker set does NOT have a local worker, only the main WorkerSet
                # does).
                pol_map = algo.workers.local_worker().policy_map
                self.assertTrue(pid not in pol_map)
                self.assertTrue(len(pol_map) == i)

            algo.stop()

    def test_evaluation_option(self):
        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation.
        config = (
            dqn.DQNConfig()
            .environment(env="CartPole-v1")
            .evaluation(
                evaluation_interval=2,
                evaluation_duration=2,
                evaluation_duration_unit="episodes",
                evaluation_config=dqn.DQNConfig.overrides(gamma=0.98),
            )
            .callbacks(callbacks_class=AssertEvalCallback)
        )

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            algo = config.build()
            # Given evaluation_interval=2, r0, r2, r4 should not contain
            # evaluation metrics, while r1, r3 should.
            r0 = algo.train()
            print(r0)
            r1 = algo.train()
            print(r1)
            r2 = algo.train()
            print(r2)
            r3 = algo.train()
            print(r3)
            algo.stop()

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
            .environment(env="CartPole-v1")
            .evaluation(
                evaluation_interval=2,
                evaluation_duration=2,
                evaluation_duration_unit="episodes",
                evaluation_config=dqn.DQNConfig.overrides(gamma=0.98),
                always_attach_evaluation_results=True,
            )
            .reporting(min_sample_timesteps_per_iteration=100)
            .callbacks(callbacks_class=AssertEvalCallback)
        )
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            algo = config.build()
            # Should always see latest available eval results.
            r0 = algo.train()
            r1 = algo.train()
            r2 = algo.train()
            r3 = algo.train()
            algo.stop()

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
            .environment(env="CartPole-v1")
            .callbacks(callbacks_class=AssertEvalCallback)
        )

        for _ in framework_iterator(frameworks=("tf", "torch")):
            # Setup algorithm w/o evaluation worker set and still call
            # evaluate() -> Expect error.
            algo_wo_env_on_local_worker = config.build()
            self.assertRaisesRegex(
                ValueError,
                "Cannot evaluate w/o an evaluation worker set",
                algo_wo_env_on_local_worker.evaluate,
            )
            algo_wo_env_on_local_worker.stop()

            # Try again using `create_env_on_driver=True`.
            # This force-adds the env on the local-worker, so this Algorithm
            # can `evaluate` even though it doesn't have an evaluation-worker
            # set.
            config.create_env_on_local_worker = True
            algo_w_env_on_local_worker = config.build()
            results = algo_w_env_on_local_worker.evaluate()
            assert "evaluation" in results
            assert "episode_reward_mean" in results["evaluation"]
            algo_w_env_on_local_worker.stop()
            config.create_env_on_local_worker = False

    def test_space_inference_from_remote_workers(self):
        # Expect to not do space inference if the learner has an env.

        env = gym.make("CartPole-v1")

        config = (
            pg.PGConfig()
            .rollouts(num_rollout_workers=1, validate_workers_after_construction=False)
            .environment(env="CartPole-v1")
        )

        # No env on driver -> expect longer build time due to space
        # lookup from remote worker.
        t0 = time.time()
        algo = config.build()
        w_lookup = time.time() - t0
        print(f"No env on learner: {w_lookup}sec")
        algo.stop()

        # Env on driver -> expect shorted build time due to no space
        # lookup required from remote worker.
        config.create_env_on_local_worker = True
        t0 = time.time()
        algo = config.build()
        wo_lookup = time.time() - t0
        print(f"Env on learner: {wo_lookup}sec")
        self.assertLess(wo_lookup, w_lookup)
        algo.stop()

        # Spaces given -> expect shorter build time due to no space
        # lookup required from remote worker.
        config.create_env_on_local_worker = False
        config.environment(
            observation_space=env.observation_space,
            action_space=env.action_space,
        )
        t0 = time.time()
        algo = config.build()
        wo_lookup = time.time() - t0
        print(f"Spaces given manually in config: {wo_lookup}sec")
        self.assertLess(wo_lookup, w_lookup)
        algo.stop()

    def test_worker_validation_time(self):
        """Tests the time taken by `validate_workers_after_construction=True`."""
        config = pg.PGConfig().environment(env="CartPole-v1")
        config.validate_workers_after_construction = True

        # Test, whether validating one worker takes just as long as validating
        # >> 1 workers.
        config.num_rollout_workers = 1
        t0 = time.time()
        algo = config.build()
        total_time_1 = time.time() - t0
        print(f"Validating w/ 1 worker: {total_time_1}sec")
        algo.stop()

        config.num_rollout_workers = 5
        t0 = time.time()
        algo = config.build()
        total_time_5 = time.time() - t0
        print(f"Validating w/ 5 workers: {total_time_5}sec")
        algo.stop()

        check(total_time_5 / total_time_1, 1.0, atol=1.0)

    def test_no_env_but_eval_workers_do_have_env(self):
        """Tests whether no env on workers, but env on eval workers works ok."""
        script_path = Path(__file__)
        input_file = os.path.join(
            script_path.parent.parent.parent, "tests/data/cartpole/small.json"
        )

        env = gym.make("CartPole-v1")

        offline_rl_config = (
            BCConfig()
            .environment(
                observation_space=env.observation_space, action_space=env.action_space
            )
            .evaluation(
                evaluation_interval=1,
                evaluation_num_workers=1,
                evaluation_config=BCConfig.overrides(
                    env="CartPole-v1",
                    input_="sampler",
                    observation_space=None,  # Test, whether this is inferred.
                    action_space=None,  # Test, whether this is inferred.
                ),
            )
            .offline_data(input_=[input_file])
        )

        bc = offline_rl_config.build()
        bc.train()
        bc.stop()

    def test_counters_after_checkpoint(self):
        # We expect algorithm to no start counters from zero after loading a
        # checkpoint on a fresh Algorithm instance
        config = pg.PGConfig().environment(env="CartPole-v1")
        algo = config.build()

        self.assertTrue(all(c == 0 for c in algo._counters.values()))
        algo.step()
        self.assertTrue((all(c != 0 for c in algo._counters.values())))
        counter_values = list(algo._counters.values())
        state = algo.__getstate__()
        algo.stop()

        algo2 = config.build()
        self.assertTrue(all(c == 0 for c in algo2._counters.values()))
        algo2.__setstate__(state)
        counter_values2 = list(algo2._counters.values())
        self.assertEqual(counter_values, counter_values2)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
