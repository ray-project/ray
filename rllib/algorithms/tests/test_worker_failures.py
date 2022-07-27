from collections import defaultdict
import gym
import numpy as np
import time
import unittest

import ray
from ray.rllib.algorithms.pg import PG, PGConfig
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.policy.policy import PolicySpec
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

    Examples:
        >>> from ray.rllib.env.env_context import EnvContext
        >>> # This env will fail for workers 1 and 2 (not for the local worker
        >>> # or any others with an index != [1|2]).
        >>> bad_env = FaultInjectEnv(
        ...     EnvContext(
        ...         {"bad_indices": [1, 2]},
        ...         worker_index=1,
        ...         num_workers=3,
        ..      )
        ... )

        >>> from ray.rllib.env.env_context import EnvContext
        >>> # This env will fail only on the first evaluation worker, not on the first
        >>> # regular rollout worker.
        >>> bad_env = FaultInjectEnv(
        ...     EnvContext(
        ...         {"bad_indices": [1], "eval_only": True},
        ...         worker_index=2,
        ...         num_workers=5,
        ...     )
        ... )
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

        if config.get("init_delay", 0) > 0.0:
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

        # Do not raise simulated error if recreated worker can not fail,
        # and this is a recreated worker.
        if (
            not self.config.get("recreated_worker_can_fail", False)
            and self.config.recreated_worker
        ):
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

    def reset(self):
        self._increment_count()
        self._maybe_raise_error()
        return self.env.reset()

    def step(self, action):
        self._increment_count()
        self._maybe_raise_error()
        return self.env.step(action)


def is_recreated(w):
    return w.apply.remote(
        lambda w: w.recreated_worker or w.env_context.recreated_worker
    )


class TestWorkerFailure(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

        register_env("fault_env", lambda c: FaultInjectEnv(c))
        register_env(
            "multi-agent-fault_env", lambda c: make_multi_agent(FaultInjectEnv)(c)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def _do_test_fault_ignore(self, algo: str, config: dict, fail_eval: bool = False):
        algo_cls = get_algorithm_class(algo)

        # Test fault handling
        config["num_workers"] = 2
        config["ignore_worker_failures"] = True
        # Make worker idx=1 fail. Other workers will be ok.
        config["env_config"] = {"bad_indices": [1]}
        if fail_eval:
            config["evaluation_num_workers"] = 2
            config["evaluation_interval"] = 1
            config["evaluation_config"] = {
                "ignore_worker_failures": True,
                "env_config": {
                    # Make worker idx=1 fail. Other workers will be ok.
                    "bad_indices": [1],
                    "evaluation": True,
                },
            }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            algo = algo_cls(config=config, env="fault_env")
            result = algo.train()

            # Both rollout workers are healthy.
            self.assertTrue(result["num_healthy_workers"] == 1)
            if fail_eval:
                # One of the eval workers failed.
                self.assertTrue(result["evaluation"]["num_healthy_workers"] == 1)

            algo.stop()

    def _do_test_fault_fatal(self, alg, config, fail_eval=False):
        agent_cls = get_algorithm_class(alg)

        # Test raises real error when out of workers.
        config["num_workers"] = 2
        config["ignore_worker_failures"] = False
        # Make both worker idx=1 and 2 fail.
        config["env_config"] = {"bad_indices": [1, 2]}
        if fail_eval:
            config["evaluation_num_workers"] = 2
            config["evaluation_interval"] = 1
            config["evaluation_config"] = {
                "ignore_worker_failures": False,
                # Make eval worker (index 1) fail.
                "env_config": {
                    "bad_indices": [1],
                    "evaluation": True,
                },
            }

        for _ in framework_iterator(config, frameworks=("torch", "tf")):
            a = agent_cls(config=config, env="fault_env")
            self.assertRaises(Exception, lambda: a.train())
            a.stop()

    def test_fatal(self):
        # Test the case where all workers fail (w/o recovery).
        self._do_test_fault_fatal("PG", {"optimizer": {}})

    def test_async_grads(self):
        self._do_test_fault_ignore("A3C", {"optimizer": {"grads_per_step": 1}})

    def test_async_replay(self):
        self._do_test_fault_ignore(
            "APEX",
            {
                "num_gpus": 0,
                "min_sample_timesteps_per_iteration": 1000,
                "min_time_s_per_iteration": 1,
                "explore": False,
                "learning_starts": 1000,
                "target_network_update_freq": 100,
                "optimizer": {
                    "num_replay_buffer_shards": 1,
                },
            },
        )

    def test_async_samples(self):
        self._do_test_fault_ignore("IMPALA", {"num_gpus": 0})

    def test_sync_replay(self):
        self._do_test_fault_ignore("DQN", {"min_sample_timesteps_per_iteration": 1})

    def test_multi_g_p_u(self):
        self._do_test_fault_ignore(
            "PPO",
            {
                "num_sgd_iter": 1,
                "train_batch_size": 10,
                "rollout_fragment_length": 10,
                "sgd_minibatch_size": 1,
            },
        )

    def test_sync_samples(self):
        self._do_test_fault_ignore("PG", {"optimizer": {}})

    def test_async_sampling_option(self):
        self._do_test_fault_ignore("PG", {"optimizer": {}, "sample_async": True})

    def test_eval_workers_failing_ignore(self):
        # Test the case where one eval worker fails, but we chose to ignore.
        self._do_test_fault_ignore(
            "PG",
            config={"model": {"fcnet_hiddens": [4]}},
            fail_eval=True,
        )

    def test_eval_workers_failing_fatal(self):
        # Test the case where all eval workers fail (w/o recovery).
        self._do_test_fault_fatal(
            "PG",
            config={"model": {"fcnet_hiddens": [4]}},
            fail_eval=True,
        )

    def test_workers_fatal_but_recover(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_workers_fatal_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = {
            "num_workers": 2,
            # Worker fault tolerance.
            "ignore_worker_failures": False,  # Do not ignore
            "recreate_failed_workers": True,  # But recover.
            "model": {"fcnet_hiddens": [4]},
            "env_config": {
                # Make both worker idx=1 and 2 fail.
                "bad_indices": [1, 2],
                # Env throws error between steps 100 and 102.
                "failure_start_count": 100,
                "failure_stop_count": 102,
                "counter": COUNTER_NAME,
            },
        }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = PG(config=config, env="fault_env")

            # Before train loop, workers are fresh and not recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )

            result = a.train()

            self.assertEqual(result["num_healthy_workers"], 2)
            # Workers are re-created.
            self.assertEqual(result["num_recreated_workers"], 2)
            self.assertTrue(
                all(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )

    def test_eval_workers_fault_but_recover(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_eval_workers_fault_but_recover"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = {
            "num_workers": 2,
            # Worker fault tolerance.
            "ignore_worker_failures": True,  # Ignore failure.
            "recreate_failed_workers": True,  # And recover.
            "model": {"fcnet_hiddens": [4]},
            # 2 eval workers.
            "evaluation_num_workers": 2,
            "evaluation_interval": 1,
            "evaluation_config": {
                "env_config": {
                    "evaluation": True,
                    "p_done": 0.0,
                    "max_episode_len": 20,
                    # Make both eval workers fail.
                    "bad_indices": [1, 2],
                    # Env throws error between steps 10 and 12.
                    "failure_start_count": 10,
                    "failure_stop_count": 12,
                    "counter": COUNTER_NAME,
                }
            },
        }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = PG(config=config, env="fault_env")

            # Before train loop, workers are fresh and not recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )
            # Eval workers are also fresh and not recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [
                            is_recreated(worker)
                            for worker in a.evaluation_workers.remote_workers()
                        ]
                    )
                )
            )

            result = a.train()

            self.assertEqual(result["num_healthy_workers"], 2)
            # Nothing happens to worker. They are still not re-created.
            self.assertEqual(result["num_recreated_workers"], 0)
            self.assertTrue(
                not any(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )

            self.assertEqual(result["evaluation"]["num_healthy_workers"], 2)
            # But all the eval workers are re-created.
            self.assertEqual(result["evaluation"]["num_recreated_workers"], 2)
            self.assertTrue(
                all(
                    ray.get(
                        [
                            is_recreated(worker)
                            for worker in a.evaluation_workers.remote_workers()
                        ]
                    )
                )
            )

    def test_eval_workers_fault_but_restore_env(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_eval_workers_fault_but_restore_env"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = {
            "num_workers": 2,
            # Worker fault tolerance.
            "ignore_worker_failures": True,
            "recreate_failed_workers": True,
            "model": {"fcnet_hiddens": [4]},
            "env_config": {
                # Make both worker idx=1 and 2 fail.
                "bad_indices": [1, 2],
                # Env throws error before step 2.
                "failure_stop_count": 2,
                "counter": COUNTER_NAME,
            },
            # 2 eval workers.
            "evaluation_num_workers": 2,
            "evaluation_interval": 1,
            "evaluation_config": {
                "ignore_worker_failures": True,
                "recreate_failed_workers": True,
                # Now instead of recreating failed workers,
                # we want to recreate the failed sub env instead.
                "restart_failed_sub_environments": True,
                "env_config": {
                    "evaluation": True,
                    # Make eval worker (index 1) fail.
                    "bad_indices": [1],
                },
            },
        }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = PG(config=config, env="fault_env")

            # Before train loop, workers are fresh and not recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )

            result = a.train()

            self.assertTrue(result["num_healthy_workers"] == 2)
            # Workers are re-created.
            self.assertEqual(result["num_recreated_workers"], 2)
            self.assertTrue(
                all(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )

            self.assertTrue(result["evaluation"]["num_healthy_workers"] == 2)
            # However eval worker is not, since we want to restart
            # individual env.
            self.assertEqual(result["evaluation"]["num_recreated_workers"], 0)
            self.assertTrue(
                not any(
                    ray.get(
                        [
                            is_recreated(worker)
                            for worker in a.evaluation_workers.remote_workers()
                        ]
                    )
                )
            )

            # This should also work several times.
            result = a.train()

            self.assertTrue(result["num_healthy_workers"] == 2)
            self.assertTrue(result["evaluation"]["num_healthy_workers"] == 2)

            a.stop()

    def test_multi_agent_env_eval_workers_fault_but_restore_env(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_multi_agent_env_eval_workers_fault_but_restore_env"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = {
            "num_workers": 2,
            "model": {"fcnet_hiddens": [4]},
            # Workers do not fault and no fault tolerance.
            "env_config": {},
            "multiagent": {
                "policies": {
                    "main_agent": PolicySpec(),
                },
                "policies_to_train": ["main_agent"],
                "policy_mapping_fn": lambda _: "main_agent",
            },
            # 2 eval workers.
            "evaluation_num_workers": 2,
            "evaluation_interval": 1,
            "evaluation_config": {
                # Now instead of recreating failed workers,
                # we want to recreate the failed sub env instead.
                "restart_failed_sub_environments": True,
                "env_config": {
                    "evaluation": True,
                    "p_done": 0.0,
                    "max_episode_len": 20,
                    # Make eval worker (index 1) fail.
                    "bad_indices": [1],
                    "counter": COUNTER_NAME,
                    "failure_start_count": 10,
                    "failure_stop_count": 12,
                },
            },
            "disable_env_checking": True,
        }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = PG(config=config, env="multi-agent-fault_env")

            result = a.train()

            self.assertTrue(result["num_healthy_workers"] == 2)
            self.assertEqual(result["num_faulty_episodes"], 0)
            self.assertTrue(result["evaluation"]["num_healthy_workers"] == 2)
            self.assertEqual(result["evaluation"]["num_recreated_workers"], 0)
            # There should be a faulty episode.
            self.assertEqual(result["evaluation"]["num_faulty_episodes"], 2)

            # This should also work several times.
            result = a.train()

            self.assertTrue(result["num_healthy_workers"] == 2)
            self.assertEqual(result["num_faulty_episodes"], 0)
            self.assertTrue(result["evaluation"]["num_healthy_workers"] == 2)
            self.assertEqual(result["evaluation"]["num_recreated_workers"], 0)
            # There shouldn't be any faulty episode anymore.
            self.assertEqual(result["evaluation"]["num_faulty_episodes"], 0)

            a.stop()

    def test_long_failure_period_restore_env(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_long_failure_period_restore_env"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = {
            "num_workers": 1,
            "create_env_on_driver": False,
            # Worker fault tolerance.
            "recreate_failed_workers": True,  # Restore failed workers.
            "restart_failed_sub_environments": True,  # And create failed envs.
            "model": {"fcnet_hiddens": [4]},
            "env_config": {
                "p_done": 0.0,
                "max_episode_len": 100,
                "bad_indices": [1],
                # Env throws error between steps 50 and 150.
                "failure_start_count": 30,
                "failure_stop_count": 80,
                "counter": COUNTER_NAME,
            },
            # 2 eval workers.
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {
                "env_config": {
                    "evaluation": True,
                }
            },
        }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = PG(config=config, env="fault_env")

            # Before train loop, workers are fresh and not recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )
            # Eval workers are also fresh and not recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [
                            is_recreated(worker)
                            for worker in a.evaluation_workers.remote_workers()
                        ]
                    )
                )
            )

            result = a.train()

            # Should see a lot of faulty episodes.
            self.assertGreaterEqual(result["num_faulty_episodes"], 50)
            self.assertGreaterEqual(result["evaluation"]["num_faulty_episodes"], 50)

            self.assertTrue(result["num_healthy_workers"] == 1)
            # All workers are still not restored, since env are restored.
            self.assertTrue(
                not any(
                    ray.get(
                        [is_recreated(worker) for worker in a.workers.remote_workers()]
                    )
                )
            )

            self.assertTrue(result["evaluation"]["num_healthy_workers"] == 1)
            # All eval workers are still not restored, since env are recreated.
            self.assertTrue(
                not any(
                    ray.get(
                        [
                            is_recreated(worker)
                            for worker in a.evaluation_workers.remote_workers()
                        ]
                    )
                )
            )

    def test_env_wait_time_workers_restore_env(self):
        # Counter that will survive restarts.
        COUNTER_NAME = "test_env_wait_time_workers_restore_env"
        counter = Counter.options(name=COUNTER_NAME).remote()

        config = {
            "num_workers": 1,
            # Worker fault tolerance.
            "ignore_worker_failures": False,  # Do not ignore
            "recreate_failed_workers": True,  # But recover.
            "restart_failed_sub_environments": True,
            "model": {"fcnet_hiddens": [4]},
            "rollout_fragment_length": 10,
            "train_batch_size": 10,
            "env_config": {
                "p_done": 0.0,
                "max_episode_len": 10,
                "init_delay": 10,  # 10 sec init delay.
                # Make both worker idx=1 and 2 fail.
                "bad_indices": [1],
                # Env throws error between steps 100 and 102.
                "failure_start_count": 7,
                "failure_stop_count": 8,
                "counter": COUNTER_NAME,
            },
            # Use EMA PerfStat.
            # Really large coeff to show the difference in env_wait_time_ms.
            # Pretty much consider the last 2 data points.
            "sampler_perf_stats_ema_coef": 0.5,
            # Important, don't smooth over all the episodes,
            # otherwise we don't see latency spike.
            "metrics_num_episodes_for_smoothing": 1,
        }

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            # Reset interaciton counter.
            ray.wait([counter.reset.remote()])

            a = PG(config=config, env="fault_env")

            # Had to restore env during this iteration.
            result = a.train()
            self.assertEqual(result["num_faulty_episodes"], 1)
            time_with_restore = result["sampler_perf"]["mean_env_wait_ms"]

            # Doesn't have to restore env during this iteration.
            result = a.train()
            # Still only 1 faulty episode.
            self.assertEqual(result["num_faulty_episodes"], 0)
            time_without_restore = result["sampler_perf"]["mean_env_wait_ms"]

            # wait time with restore is at least 2 times wait time without restore.
            self.assertGreater(time_with_restore, 2 * time_without_restore)

    def test_eval_workers_on_infinite_episodes(self):
        """Tests whether eval workers warn appropriately after some episode timeout."""
        # Create infinitely running episodes, but with horizon setting (RLlib will
        # auto-terminate the episode). However, in the eval workers, don't set a
        # horizon -> Expect warning and no proper evaluation results.
        config = (
            PGConfig()
            .rollouts(num_rollout_workers=2, horizon=100)
            .reporting(metrics_episode_collection_timeout_s=5.0)
            .environment(env=RandomEnv, env_config={"p_done": 0.0})
            .evaluation(
                evaluation_num_workers=2,
                evaluation_interval=1,
                evaluation_sample_timeout_s=5.0,
                evaluation_config={
                    "horizon": None,
                },
            )
        )
        algo = config.build()
        results = algo.train()
        self.assertTrue(np.isnan(results["evaluation"]["episode_reward_mean"]))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
