from collections import defaultdict
import gymnasium as gym
import numpy as np
import time
import unittest

import ray
from ray.util.state import list_actors
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.algorithms.pg.pg_tf_policy import PGTF2Policy
from ray.rllib.algorithms.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.registry import register_env


@ray.remote
class Counter:
    """Remote counter service that survives restarts."""

    def __init__(self):
        self.reset()

    def _key(self, eval, worker_index, vector_index):
        return f"{eval}:{worker_index}:{vector_index}"

    def increment(self, eval, worker_index, vector_index):
        self.counter[self._key(eval, worker_index, vector_index)] += 1

    def get(self, eval, worker_index, vector_index):
        return self.counter[self._key(eval, worker_index, vector_index)]

    def reset(self):
        self.counter = defaultdict(int)


class FaultInjectEnv(gym.Env):
    """Env that fails upon calling `step()`, but only for some remote worker indices.

    The worker indices that should produce the failure (a ValueError) can be
    provided by a list (of ints) under the "bad_indices" key in the env's
    config.

    .. testcode::
        :skipif: True

        from ray.rllib.env.env_context import EnvContext
        # This env will fail for workers 1 and 2 (not for the local worker
        # or any others with an index != [1|2]).
        bad_env = FaultInjectEnv(
            EnvContext(
                {"bad_indices": [1, 2]},
                worker_index=1,
                num_workers=3,
             )
        )

        from ray.rllib.env.env_context import EnvContext
        # This env will fail only on the first evaluation worker, not on the first
        # regular rollout worker.
        bad_env = FaultInjectEnv(
            EnvContext(
                {"bad_indices": [1], "eval_only": True},
                worker_index=2,
                num_workers=5,
            )
        )
    """

    def __init__(self, config):
        # Use RandomEnv to control episode length if needed.
        self.env = RandomEnv(config)
        self._skip_env_checking = True
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space
        self.config = config
        # External counter service.
        if "counter" in config:
            self.counter = ray.get_actor(config["counter"])
        else:
            self.counter = None

        if (
            config.get("init_delay", 0) > 0.0
            and (
                not config.get("init_delay_indices", [])
                or self.config.worker_index in config.get("init_delay_indices", [])
            )
            and
            # constructor delay can only happen for recreated actors.
            self._get_count() > 0
        ):
            # Simulate an initialization delay.
            time.sleep(config.get("init_delay"))

    def _increment_count(self):
        if self.counter:
            eval = self.config.get("evaluation", False)
            worker_index = self.config.worker_index
            vector_index = self.config.vector_index
            ray.wait([self.counter.increment.remote(eval, worker_index, vector_index)])

    def _get_count(self):
        if self.counter:
            eval = self.config.get("evaluation", False)
            worker_index = self.config.worker_index
            vector_index = self.config.vector_index
            return ray.get(self.counter.get.remote(eval, worker_index, vector_index))
        return -1

    def _maybe_raise_error(self):
        # Do not raise simulated error if this worker is not bad.
        if self.config.worker_index not in self.config.get("bad_indices", []):
            return

        if self.counter:
            count = self._get_count()
            if self.config.get(
                "failure_start_count", -1
            ) >= 0 and count < self.config.get("failure_start_count"):
                return

            if self.config.get(
                "failure_stop_count", -1
            ) >= 0 and count >= self.config.get("failure_stop_count"):
                return

        raise ValueError(
            "This is a simulated error from "
            f"{'eval-' if self.config.get('evaluation', False) else ''}"
            f"worker-idx={self.config.worker_index}!"
        )

    def reset(self, *, seed=None, options=None):
        self._increment_count()
        self._maybe_raise_error()
        return self.env.reset()

    def step(self, action):
        self._increment_count()
        self._maybe_raise_error()

        if self.config.get("step_delay", 0) > 0.0 and (
            not self.config.get("init_delay_indices", [])
            or self.config.worker_index in self.config.get("step_delay_indices", [])
        ):
            # Simulate a step delay.
            time.sleep(self.config.get("step_delay"))

        return self.env.step(action)

    def action_space_sample(self):
        return self.env.action_space.sample()


class ForwardHealthCheckToEnvWorker(RolloutWorker):
    """Configure RolloutWorker to error in specific condition is hard.

    So we take a short-cut, and simply forward ping() to env.sample().
    """

    def ping(self) -> str:
        # See if Env wants to throw error.
        _ = self.env.step(self.env.action_space_sample())
        # If there is no error raised from sample(), we simply reply pong.
        return super().ping()


def wait_for_restore(num_restarting_allowed=0):
    """Wait for Ray actor fault tolerence to restore all failed workers.

    Args:
        num_restarting_allowed: Number of actors that are allowed to be
            in "RESTARTING" state. This is because some actors may
            hang in __init__().
    """
    while True:
        states = [
            a["state"]
            for a in list_actors(
                filters=[("class_name", "=", "ForwardHealthCheckToEnvWorker")]
            )
        ]
        finished = True
        for s in states:
            # Wait till all actors are either "ALIVE" (restored),
            # or "DEAD" (cancelled. these actors are from other
            # finished test cases) or "RESTARTING" (being restored).
            if s not in ["ALIVE", "DEAD", "RESTARTING"]:
                finished = False
                break

        restarting = [s for s in states if s == "RESTARTING"]
        if len(restarting) > num_restarting_allowed:
            finished = False

        print("waiting ... ", states)
        if finished:
            break
        # Otherwise, wait a bit.
        time.sleep(0.5)


class AddPolicyCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()

    def on_algorithm_init(self, *, algorithm, **kwargs):
        # Add a custom policy to algorithm.
        algorithm.add_policy(
            policy_id="test_policy",
            policy_cls=(
                PGTorchPolicy
                if algorithm.config.framework_str == "torch"
                else PGTF2Policy
            ),
            observation_space=gym.spaces.Box(low=0, high=1, shape=(8,)),
            action_space=gym.spaces.Discrete(2),
            config={},
            policy_state=None,
            evaluation_workers=True,
        )


class TestWorkerFailures(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

        register_env("fault_env", lambda c: FaultInjectEnv(c))
        register_env(
            "multi_agent_fault_env", lambda c: make_multi_agent(FaultInjectEnv)(c)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def _do_test_fault_ignore(self, config: AlgorithmConfig, fail_eval: bool = False):
        # Test fault handling
        config.num_rollout_workers = 2
        config.ignore_worker_failures = True
        config.recreate_failed_workers = False
        config.env = "fault_env"
        # Make worker idx=1 fail. Other workers will be ok.
        config.env_config = {
            "bad_indices": [1],
        }
        if fail_eval:
            config.evaluation_num_workers = 2
            config.evaluation_interval = 1
            config.evaluation_config = {
                "ignore_worker_failures": True,
                "recreate_failed_workers": False,
                "env_config": {
                    # Make worker idx=1 fail. Other workers will be ok.
                    "bad_indices": [1],
                    "evaluation": True,
                },
            }

        print(config)

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            algo = config.build()
            algo.train()

            # One of the rollout workers failed.
            self.assertEqual(algo.workers.num_healthy_remote_workers(), 1)
            if fail_eval:
                # One of the eval workers failed.
                self.assertEqual(
                    algo.evaluation_workers.num_healthy_remote_workers(), 1
                )

            algo.stop()

    def _do_test_fault_fatal(self, config, fail_eval=False):
        # Test raises real error when out of workers.
        config.num_rollout_workers = 2
        config.env = "fault_env"
        # Make both worker idx=1 and 2 fail.
        config.env_config = {"bad_indices": [1, 2]}
        if fail_eval:
            config.evaluation_num_workers = 2
            config.evaluation_interval = 1
            config.evaluation_config = {
                # Make eval worker (index 1) fail.
                "env_config": {
                    "bad_indices": [1],
                    "evaluation": True,
                },
            }

        for _ in framework_iterator(config, frameworks=("torch", "tf")):
            a = config.build()
            self.assertRaises(Exception, lambda: a.train())
            a.stop()

    def _do_test_fault_fatal_but_recreate(self, config, multi_agent=False):
        # Counter that will survive restarts.
        COUNTER_NAME = (
            f"_do_test_fault_fatal_but_recreate{'_ma' if multi_agent else ''}"
        )
        counter = Counter.options(name=COUNTER_NAME).remote()

        # Test raises real error when out of workers.
        config.num_rollout_workers = 1
        config.evaluation_num_workers = 1
        config.evaluation_interval = 1
        config.env = "fault_env" if not multi_agent else "multi_agent_fault_env"
        config.evaluation_config = AlgorithmConfig.overrides(
            recreate_failed_workers=True,
            # 0 delay for testing purposes.
            delay_between_worker_restarts_s=0,
            # Make eval worker (index 1) fail.
            env_config={
                "bad_indices": [1],
                "failure_start_count": 3,
                "failure_stop_count": 4,
                "counter": COUNTER_NAME,
            },
            **(
                dict(
                    policy_mapping_fn=(
                        lambda aid, episode, worker, **kwargs: (
                            # Allows this test to query this
                            # different-from-training-workers policy mapping fn.
                            "This is the eval mapping fn"
                            if episode is None
                            else "main"
                            if episode.episode_id % 2 == aid
                            else "p{}".format(np.random.choice([0, 1]))
                        )
                    )
                )
                if multi_agent
                else {}
            ),
        )

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaction counter.
            ray.wait([counter.reset.remote()])

            a = config.build()

            # This should also work several times.
            for _ in range(2):
                a.train()
                wait_for_restore()
                a.train()

                self.assertEqual(a.workers.num_healthy_remote_workers(), 1)
                self.assertEqual(a.evaluation_workers.num_healthy_remote_workers(), 1)
                if multi_agent:
                    # Make a dummy call to the eval worker's policy_mapping_fn and
                    # make sure the restored eval worker received the correct one from
                    # the eval config (not the main workers' one).
                    test = a.evaluation_workers.foreach_worker(
                        lambda w: w.policy_mapping_fn(0, None, None)
                    )
                    self.assertEqual(test[0], "This is the eval mapping fn")
            a.stop()

    def test_fatal(self):
        # Test the case where all workers fail (w/o recovery).
        self._do_test_fault_fatal(PGConfig().training(optimizer={}))

    def test_async_samples(self):
        self._do_test_fault_ignore(
            ImpalaConfig()
            .rollouts(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .resources(num_gpus=0)
        )

    def test_sync_replay(self):
        self._do_test_fault_ignore(
            DQNConfig()
            .rollouts(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .reporting(min_sample_timesteps_per_iteration=1)
        )

    def test_multi_gpu(self):
        self._do_test_fault_ignore(
            PPOConfig()
            .rollouts(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
            )
            .training(
                train_batch_size=10,
                sgd_minibatch_size=1,
                num_sgd_iter=1,
            )
        )

    def test_sync_samples(self):
        self._do_test_fault_ignore(
            PPOConfig()
            .rollouts(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .training(optimizer={})
        )

    def test_eval_workers_failing_ignore(self):
        # Test the case where one eval worker fails, but we chose to ignore.
        self._do_test_fault_ignore(
            PPOConfig()
            .rollouts(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .training(model={"fcnet_hiddens": [4]}),
            fail_eval=True,
        )

    def test_recreate_eval_workers_parallel_to_training_w_actor_manager(self):
        # Test the case where all eval workers fail, but we chose to recover.
        config = (
            PGConfig()
            .rollouts(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .evaluation(
                evaluation_num_workers=1,
                enable_async_evaluation=True,
                evaluation_parallel_to_training=True,
                evaluation_duration="auto",
            )
            .training(model={"fcnet_hiddens": [4]})
        )

        self._do_test_fault_fatal_but_recreate(config)

    def test_recreate_eval_workers_parallel_to_training_w_actor_manager_and_multi_agent(
        self,
    ):
        # Test the case where all eval workers fail on a multi-agent env with
        # different `policy_mapping_fn` in eval- vs train workers, but we chose
        # to recover.
        config = (
            PGConfig()
            .rollouts(env_runner_cls=ForwardHealthCheckToEnvWorker)
            .multi_agent(
                policies={"main", "p0", "p1"},
                policy_mapping_fn=(
                    lambda aid, episode, worker, **kwargs: (
                        "main"
                        if episode.episode_id % 2 == aid
                        else "p{}".format(np.random.choice([0, 1]))
                    )
                ),
            )
            .evaluation(
                evaluation_num_workers=1,
                enable_async_evaluation=True,
                evaluation_parallel_to_training=True,
                evaluation_duration="auto",
            )
            .training(model={"fcnet_hiddens": [4]})
        )

        self._do_test_fault_fatal_but_recreate(config, multi_agent=True)

    def test_eval_workers_failing_fatal(self):
        # Test the case where all eval workers fail (w/o recovery).
        self._do_test_fault_fatal(
            PPOConfig().training(model={"fcnet_hiddens": [4]}),
            fail_eval=True,
        )

    def test_workers_fatal_but_recover(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_workers_fatal_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            PGConfig()
            .rollouts(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_rollout_workers=2,
                rollout_fragment_length=16,
            )
            .training(
                train_batch_size=32,
                model={"fcnet_hiddens": [4]},
            )
            .environment(
                env="fault_env",
                env_config={
                    # Make both worker idx=1 and 2 fail.
                    "bad_indices": [1, 2],
                    "failure_start_count": 3,
                    "failure_stop_count": 4,
                    "counter": COUNTER_NAME,
                },
            )
            .fault_tolerance(
                recreate_failed_workers=True,  # But recover.
                # 0 delay for testing purposes.
                delay_between_worker_restarts_s=0,
            )
        )

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = config.build()

            # Before training, 2 healthy workers.
            self.assertEqual(a.workers.num_healthy_remote_workers(), 2)
            # Nothing is restarted.
            self.assertEqual(a.workers.num_remote_worker_restarts(), 0)

            a.train()
            wait_for_restore()
            # One more iteration. Workers will be recovered during this round.
            a.train()

            # After training, still 2 healthy workers.
            self.assertEqual(a.workers.num_healthy_remote_workers(), 2)
            # Both workers are restarted.
            self.assertEqual(a.workers.num_remote_worker_restarts(), 2)

    def test_policies_are_restored_on_recovered_worker(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_policies_are_restored_on_recovered_worker"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            PGConfig()
            .rollouts(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_rollout_workers=2,
                rollout_fragment_length=16,
            )
            .training(
                train_batch_size=32,
                model={"fcnet_hiddens": [4]},
            )
            .environment(
                env="multi_agent_fault_env",
                env_config={
                    # Make both worker idx=1 and 2 fail.
                    "bad_indices": [1, 2],
                    "failure_start_count": 3,
                    "failure_stop_count": 4,
                    "counter": COUNTER_NAME,
                },
            )
            .evaluation(
                evaluation_num_workers=1,
                evaluation_interval=1,
                evaluation_config=PGConfig.overrides(
                    recreate_failed_workers=True,
                    # Restart the entire eval worker.
                    restart_failed_sub_environments=False,
                    env_config={
                        "evaluation": True,
                        # Make eval worker (index 1) fail.
                        "bad_indices": [1],
                        "failure_start_count": 3,
                        "failure_stop_count": 4,
                        "counter": COUNTER_NAME,
                    },
                ),
            )
            .callbacks(AddPolicyCallback)
            .fault_tolerance(
                recreate_failed_workers=True,  # But recover.
                # Throwing error in constructor is a bad idea.
                # 0 delay for testing purposes.
                delay_between_worker_restarts_s=0,
            )
        )

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaction counter.
            ray.wait([counter.reset.remote()])

            a = config.build()

            # Should have the custom policy.
            self.assertIsNotNone(a.get_policy("test_policy"))

            # Before train loop, workers are fresh and not recreated.
            self.assertEqual(a.workers.num_healthy_remote_workers(), 2)
            self.assertEqual(a.workers.num_remote_worker_restarts(), 0)
            self.assertEqual(a.evaluation_workers.num_healthy_remote_workers(), 1)
            self.assertEqual(a.evaluation_workers.num_remote_worker_restarts(), 0)

            a.train()
            wait_for_restore()
            # One more iteration. Workers will be recovered during this round.
            a.train()

            # Everything still healthy. And all workers are restarted.
            self.assertEqual(a.workers.num_healthy_remote_workers(), 2)
            self.assertEqual(a.workers.num_remote_worker_restarts(), 2)
            self.assertEqual(a.evaluation_workers.num_healthy_remote_workers(), 1)
            self.assertEqual(a.evaluation_workers.num_remote_worker_restarts(), 1)

            # Let's verify that our custom policy exists on both recovered workers.
            def has_test_policy(w):
                return "test_policy" in w.policy_map

            # Rollout worker has test policy.
            self.assertTrue(
                all(a.workers.foreach_worker(has_test_policy, local_worker=False))
            )
            # Eval worker has test policy.
            self.assertTrue(
                all(
                    a.evaluation_workers.foreach_worker(
                        has_test_policy, local_worker=False
                    )
                )
            )

    def test_eval_workers_fault_but_recover(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_eval_workers_fault_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            PGConfig()
            .rollouts(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_rollout_workers=2,
                rollout_fragment_length=16,
            )
            .training(
                train_batch_size=32,
                model={"fcnet_hiddens": [4]},
            )
            .environment(env="fault_env")
            .evaluation(
                evaluation_num_workers=2,
                evaluation_interval=1,
                evaluation_config=PGConfig.overrides(
                    env_config={
                        "evaluation": True,
                        "p_terminated": 0.0,
                        "max_episode_len": 20,
                        # Make both eval workers fail.
                        "bad_indices": [1, 2],
                        # Env throws error between steps 10 and 12.
                        "failure_start_count": 3,
                        "failure_stop_count": 4,
                        "counter": COUNTER_NAME,
                    },
                ),
            )
            .fault_tolerance(
                recreate_failed_workers=True,  # And recover
                # 0 delay for testing purposes.
                delay_between_worker_restarts_s=0,
            )
        )

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = config.build()

            # Before train loop, workers are fresh and not recreated.
            self.assertEqual(a.evaluation_workers.num_healthy_remote_workers(), 2)
            self.assertEqual(a.evaluation_workers.num_remote_worker_restarts(), 0)

            a.train()
            wait_for_restore()
            a.train()

            # Everything still healthy. And all workers are restarted.
            self.assertEqual(a.evaluation_workers.num_healthy_remote_workers(), 2)
            self.assertEqual(a.evaluation_workers.num_remote_worker_restarts(), 2)

    def test_worker_recover_with_hanging_workers(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_eval_workers_fault_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = (
            # Must use off-policy algorithm since we are gonna have hanging workers.
            ImpalaConfig()
            .resources(
                num_gpus=0,
            )
            .rollouts(
                env_runner_cls=ForwardHealthCheckToEnvWorker,
                num_rollout_workers=3,
                rollout_fragment_length=16,
            )
            .training(
                train_batch_size=32,
                model={"fcnet_hiddens": [4]},
            )
            .reporting(
                # Make sure each iteration doesn't take too long.
                min_time_s_per_iteration=0.5,
                # Make sure metrics reporting doesn't hang for too long
                # since we are gonna have a hanging worker.
                metrics_episode_collection_timeout_s=1,
            )
            .environment(
                env="fault_env",
                env_config={
                    "evaluation": True,
                    "p_terminated": 0.0,
                    "max_episode_len": 20,
                    # Worker 1 and 2 will fail in step().
                    "bad_indices": [1, 2],
                    # Env throws error between steps 3 and 4.
                    "failure_start_count": 3,
                    "failure_stop_count": 4,
                    "counter": COUNTER_NAME,
                    # Worker 2 will hang for long time during init after restart.
                    "init_delay": 3600,
                    "init_delay_indices": [2],
                    # Worker 3 will hang in env.step().
                    "step_delay": 3600,
                    "step_delay_indices": [3],
                },
            )
            .fault_tolerance(
                recreate_failed_workers=True,  # And recover
                worker_health_probe_timeout_s=0.01,
                worker_restore_timeout_s=5,
                delay_between_worker_restarts_s=0,  # For testing, no delay.
            )
        )

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = config.build()

            # Before train loop, workers are fresh and not recreated.
            self.assertEqual(a.workers.num_healthy_remote_workers(), 3)
            self.assertEqual(a.workers.num_remote_worker_restarts(), 0)

            a.train()
            wait_for_restore(num_restarting_allowed=1)
            # Most importantly, training progressed fine.
            a.train()

            # 2 healthy remote workers left, although worker 3 is stuck in rollout.
            self.assertEqual(a.workers.num_healthy_remote_workers(), 2)
            # Only 1 successful restore, since worker 2 is stuck in indefinite init
            # and can not be properly restored.
            self.assertEqual(a.workers.num_remote_worker_restarts(), 1)

    def test_eval_workers_on_infinite_episodes(self):
        """Tests whether eval workers warn appropriately after some episode timeout."""
        # Create infinitely running episodes, but with horizon setting (RLlib will
        # auto-terminate the episode). However, in the eval workers, don't set a
        # horizon -> Expect warning and no proper evaluation results.
        config = (
            PPOConfig()
            .environment(env=RandomEnv, env_config={"p_terminated": 0.0})
            .reporting(metrics_episode_collection_timeout_s=5.0)
            .evaluation(
                evaluation_num_workers=2,
                evaluation_interval=1,
                evaluation_sample_timeout_s=5.0,
            )
        )
        algo = config.build()
        results = algo.train()
        self.assertTrue(np.isnan(results["evaluation"]["episode_reward_mean"]))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
