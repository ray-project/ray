import gymnasium as gym
import numpy as np
import os
from pathlib import Path
from random import choice
import time
import unittest

import ray
from ray.rllib.algorithms.algorithm import Algorithm
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.algorithms.bc import BCConfig
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples.evaluation.evaluation_parallel_to_training import (
    AssertEvalCallback,
)
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.framework import convert_to_tensor
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    LEARNER_RESULTS,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check
from ray.tune import register_env


class TestAlgorithm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)
        register_env("multi_cart", lambda cfg: MultiAgentCartPole(cfg))

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_add_module_and_remove_module(self):
        config = (
            ppo.PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment(
                env="multi_cart",
                env_config={"num_agents": 4},
            )
            .env_runners(num_cpus_per_env_runner=0.1)
            .training(
                train_batch_size=100,
                minibatch_size=50,
                num_epochs=1,
            )
            .rl_module(
                model_config_dict={
                    "fcnet_hiddens": [5],
                    "fcnet_activation": "linear",
                },
            )
            .multi_agent(
                # Start with a single policy.
                policies={"p0"},
                policy_mapping_fn=lambda *a, **kw: "p0",
                # TODO (sven): Support object store caching on new API stack.
                # # And only two policies that can be stored in memory at a
                # # time.
                # policy_map_capacity=2,
            )
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_config=ppo.PPOConfig.overrides(num_cpus_per_env_runner=0.1),
            )
        )

        # Construct the Algorithm with a single policy in it.
        algo = config.build()
        mod0 = algo.get_module("p0")
        r = algo.train()
        self.assertTrue("p0" in r[LEARNER_RESULTS])
        for i in range(1, 3):

            def new_mapping_fn(agent_id, episode, i=i, **kwargs):
                return f"p{choice([i, i - 1])}"

            # Add a new RLModule by class (and options).
            mid = f"p{i}"
            print(f"Adding new RLModule {mid} ...")
            new_marl_spec = algo.add_module(
                module_id=mid,
                module_spec=RLModuleSpec.from_module(mod0),
                # Test changing the mapping fn.
                new_agent_to_module_mapping_fn=new_mapping_fn,
                # Change the list of modules to train.
                new_should_module_be_updated=[f"p{i}", f"p{i-1}"],
            )
            new_module = algo.get_module(mid)
            self._assert_modules_added(
                algo=algo,
                marl_spec=new_marl_spec,
                mids=[0, i],
                trainable=[i, i - 1],
                mapped=[i, i - 1],
                not_mapped=[i - 2],
            )

            # Assert new policy is part of local worker (eval worker set does NOT
            # have a local worker, only the main EnvRunnerGroup does).
            multi_rl_module = algo.env_runner.module
            self.assertTrue(new_module is not mod0)
            for j in range(i + 1):
                self.assertTrue(f"p{j}" in multi_rl_module)
            self.assertTrue(len(multi_rl_module) == i + 1)
            algo.train()
            checkpoint = algo.save_to_path()

            # Test restoring from the checkpoint (which has more policies
            # than what's defined in the config dict).
            test = Algorithm.from_checkpoint(checkpoint)
            self._assert_modules_added(
                algo=test,
                marl_spec=None,
                mids=[0, i - 1, i],
                trainable=[i - 1, i],
                mapped=[i - 1, i],
                not_mapped=[i - 2],
            )
            # Make sure algorithm can continue training the restored policy.
            test.train()
            # Test creating an inference action with the added (and restored) RLModule.
            mod0 = test.get_module("p0")
            out = mod0.forward_inference(
                {
                    Columns.OBS: convert_to_tensor(
                        np.expand_dims(mod0.config.observation_space.sample(), 0),
                        framework=mod0.framework,
                    ),
                },
            )
            action_dist_inputs = out[Columns.ACTION_DIST_INPUTS]
            self.assertTrue(action_dist_inputs.shape == (1, 2))
            test.stop()

            # After having added 2 Modules, try to restore the Algorithm,
            # but only with 1 of the originally added Modules (plus the initial
            # p0).
            if i == 2:

                def new_mapping_fn(agent_id, episode, **kwargs):
                    return f"p{choice([0, 2])}"

                test2 = Algorithm.from_checkpoint(path=checkpoint)
                test2.remove_module(
                    module_id="p1",
                    new_agent_to_module_mapping_fn=new_mapping_fn,
                    new_should_module_be_updated=["p0"],
                )
                self._assert_modules_added(
                    algo=test2,
                    marl_spec=None,
                    mids=[0, 2],
                    trainable=[0],
                    mapped=[0, 2],
                    not_mapped=[1, 4, 5, 6],
                )
                # Make sure algorithm can continue training the restored policy.
                mod2 = test2.get_module("p2")
                test2.train()
                # Test creating an inference action with the added (and restored)
                # RLModule.
                out = mod2.forward_exploration(
                    {
                        Columns.OBS: convert_to_tensor(
                            np.expand_dims(mod0.config.observation_space.sample(), 0),
                            framework=mod0.framework,
                        ),
                    },
                )
                action_dist_inputs = out[Columns.ACTION_DIST_INPUTS]
                self.assertTrue(action_dist_inputs.shape == (1, 2))
                test2.stop()

        # Delete all added modules again from Algorithm.
        for i in range(2, 0, -1):
            mid = f"p{i}"
            marl_spec = algo.remove_module(
                mid,
                # Note that the complete signature of a policy_mapping_fn
                # is: `agent_id, episode, worker, **kwargs`.
                new_agent_to_module_mapping_fn=(
                    lambda agent_id, episode, i=i, **kwargs: f"p{i - 1}"
                ),
                # Update list of policies to train.
                new_should_module_be_updated=[f"p{i - 1}"],
            )
            self._assert_modules_added(
                algo=algo,
                marl_spec=marl_spec,
                mids=[0, i - 1],
                trainable=[i - 1],
                mapped=[i - 1],
                not_mapped=[i, i + 1],
            )

        algo.stop()

    @OldAPIStack
    def test_add_policy_and_remove_policy(self):
        config = (
            ppo.PPOConfig()
            .environment(
                env=MultiAgentCartPole,
                env_config={
                    "config": {
                        "num_agents": 4,
                    },
                },
            )
            .env_runners(num_cpus_per_env_runner=0.1)
            .training(
                train_batch_size=100,
                minibatch_size=50,
                num_epochs=1,
                model={
                    "fcnet_hiddens": [5],
                    "fcnet_activation": "linear",
                },
            )
            .multi_agent(
                # Start with a single policy.
                policies={"p0"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "p0",
                # And only two policies that can be stored in memory at a
                # time.
                policy_map_capacity=2,
            )
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_config=ppo.PPOConfig.overrides(num_cpus_per_env_runner=0.1),
            )
        )

        obs_space = gym.spaces.Box(-2.0, 2.0, (4,))
        act_space = gym.spaces.Discrete(2)

        # Pre-generate a policy instance to test adding these directly to an
        # existing algorithm.
        policy_obj = ppo.PPOTorchPolicy(obs_space, act_space, config.to_dict())

        # Construct the Algorithm with a single policy in it.
        algo = config.build()
        pol0 = algo.get_policy("p0")
        r = algo.train()
        self.assertTrue("p0" in r["info"][LEARNER_INFO])
        for i in range(1, 3):

            def new_mapping_fn(agent_id, episode, worker, i=i, **kwargs):
                return f"p{choice([i, i - 1])}"

            # Add a new policy either by class (and options) or by instance.
            pid = f"p{i}"
            print(f"Adding policy {pid} ...")
            # By (already instantiated) instance.
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
                    observation_space=obs_space,
                    action_space=act_space,
                    # Test changing the mapping fn.
                    policy_mapping_fn=new_mapping_fn,
                    # Change the list of policies to train.
                    policies_to_train=[f"p{i}", f"p{i-1}"],
                )

            # Make sure new policy is part of remote workers in the
            # worker set and the eval worker set.
            self.assertTrue(
                all(
                    algo.env_runner_group.foreach_worker(
                        func=lambda w, pid=pid: pid in w.policy_map
                    )
                )
            )
            self.assertTrue(
                all(
                    algo.eval_env_runner_group.foreach_worker(
                        func=lambda w, pid=pid: pid in w.policy_map
                    )
                )
            )

            # Assert new policy is part of local worker (eval worker set does NOT
            # have a local worker, only the main EnvRunnerGroup does).
            pol_map = algo.env_runner.policy_map
            self.assertTrue(new_pol is not pol0)
            for j in range(i + 1):
                self.assertTrue(f"p{j}" in pol_map)
            self.assertTrue(len(pol_map) == i + 1)
            algo.train()
            checkpoint = algo.save().checkpoint

            # Test restoring from the checkpoint (which has more policies
            # than what's defined in the config dict).
            test = ppo.PPO.from_checkpoint(checkpoint=checkpoint)

            # Make sure evaluation worker also got the restored, added policy.
            def _has_policies(w, pid=pid):
                return w.get_policy("p0") is not None and w.get_policy(pid) is not None

            self.assertTrue(
                all(test.eval_env_runner_group.foreach_worker(_has_policies))
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

                test2 = ppo.PPO.from_checkpoint(
                    path=checkpoint,
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
                    all(test2.eval_env_runner_group.foreach_worker(_has_policies))
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
                    lambda agent_id, episode, worker, i=i, **kwargs: f"p{i - 1}"
                ),
                # Update list of policies to train.
                policies_to_train=[f"p{i - 1}"],
            )
            # Make sure removed policy is no longer part of remote workers in the
            # worker set and the eval worker set.
            self.assertTrue(
                algo.env_runner_group.foreach_worker(
                    func=lambda w, pid=pid: pid not in w.policy_map
                )[0]
            )
            self.assertTrue(
                algo.eval_env_runner_group.foreach_worker(
                    func=lambda w, pid=pid: pid not in w.policy_map
                )[0]
            )
            # Assert removed policy is no longer part of local worker
            # (eval worker set does NOT have a local worker, only the main
            # EnvRunnerGroup does).
            pol_map = algo.env_runner.policy_map
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

        self.assertFalse(EVALUATION_RESULTS in r0)
        self.assertTrue(EVALUATION_RESULTS in r1)
        self.assertFalse(EVALUATION_RESULTS in r2)
        self.assertTrue(EVALUATION_RESULTS in r3)
        self.assertTrue(ENV_RUNNER_RESULTS in r1[EVALUATION_RESULTS])
        self.assertTrue(
            EPISODE_RETURN_MEAN in r1[EVALUATION_RESULTS][ENV_RUNNER_RESULTS]
        )
        self.assertNotEqual(r1[EVALUATION_RESULTS], r3[EVALUATION_RESULTS])

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
            )
            .reporting(min_sample_timesteps_per_iteration=100)
            .callbacks(callbacks_class=AssertEvalCallback)
        )
        algo = config.build()
        # Should only see eval results, when eval actually ran.
        r0 = algo.train()
        r1 = algo.train()
        r2 = algo.train()
        r3 = algo.train()
        algo.stop()

        # Eval results are not available at step 0.
        # But step 3 should still have it, even though no eval was
        # run during that step.
        self.assertTrue(EVALUATION_RESULTS not in r0)
        self.assertTrue(EVALUATION_RESULTS in r1)
        self.assertTrue(EVALUATION_RESULTS not in r2)
        self.assertTrue(EVALUATION_RESULTS in r3)

    def test_evaluation_wo_evaluation_env_runner_group(self):
        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation.
        config = (
            ppo.PPOConfig()
            .environment(env="CartPole-v1")
            .callbacks(callbacks_class=AssertEvalCallback)
        )

        # Setup algorithm w/o evaluation worker set and still call
        # evaluate() -> Expect error.
        algo_wo_env_on_local_worker = config.build()
        self.assertRaisesRegex(
            ValueError,
            "Can't evaluate on a local worker",
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
        assert (
            ENV_RUNNER_RESULTS in results
            and EPISODE_RETURN_MEAN in results[ENV_RUNNER_RESULTS]
        )
        algo_w_env_on_local_worker.stop()
        config.create_env_on_local_worker = False

    def test_space_inference_from_remote_workers(self):
        # Expect to not do space inference if the learner has an env.

        env = gym.make("CartPole-v1")

        config = (
            ppo.PPOConfig()
            .env_runners(
                num_env_runners=1, validate_env_runners_after_construction=False
            )
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
        """Tests the time taken by `validate_env_runners_after_construction=True`."""
        config = ppo.PPOConfig().environment(env="CartPole-v1")
        config.validate_env_runners_after_construction = True

        # Test, whether validating one worker takes just as long as validating
        # >> 1 workers.
        config.num_env_runners = 1
        t0 = time.time()
        algo = config.build()
        total_time_1 = time.time() - t0
        print(f"Validating w/ 1 worker: {total_time_1}sec")
        algo.stop()

        config.num_env_runners = 5
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
                evaluation_num_env_runners=1,
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
        config = ppo.PPOConfig().environment(env="CartPole-v1")
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

    def _assert_modules_added(
        self,
        *,
        algo,
        marl_spec,
        mids,
        trainable,
        mapped,
        not_mapped,
    ):
        # Make sure Learner has the correct `should_module_be_updated` list.
        self.assertEqual(
            set(algo.learner_group._learner.config.policies_to_train),
            {f"p{i}" for i in trainable},
        )
        # Make sure mids are all in marl_spec.
        if marl_spec is not None:
            self.assertTrue(all(f"p{m}" in marl_spec for m in mids))
        # Make sure module is part of remote EnvRunners in the
        # EnvRunnerGroup and the eval EnvRunnerGroup.
        self.assertTrue(
            all(
                algo.env_runner_group.foreach_worker(
                    lambda w, mids=mids: all(f"p{i}" in w.module for i in mids)
                )
            )
        )
        self.assertTrue(
            all(
                algo.eval_env_runner_group.foreach_worker(
                    lambda w, mids=mids: all(f"p{i}" in w.module for i in mids)
                )
            )
        )
        # Make sure that EnvRunners have received the correct mapping fn.
        mapped_pols = [
            algo.env_runner.config.policy_mapping_fn(0, None) for _ in range(100)
        ]
        self.assertTrue(all(f"p{i}" in mapped_pols for i in mapped))
        self.assertTrue(not any(f"p{i}" in mapped_pols for i in not_mapped))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
