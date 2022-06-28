import unittest

import gym

import ray
from ray.rllib import _register_all
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune.registry import register_env


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
        ...    EnvContext({"bad_indices": [1, 2]},
        ...               worker_index=1, num_workers=3))
    """

    def __init__(self, config):
        self.env = gym.make("CartPole-v0")
        self._skip_env_checking = True
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space
        self.config = config

    def reset(self):
        return self.env.reset()

    def step(self, action):
        # Only fail on the original workers with the specified indices.
        # Once on a recreated worker, don't fail anymore.
        if (
            self.config.worker_index in self.config["bad_indices"]
            and not self.config.recreated_worker
        ):
            raise ValueError(
                "This is a simulated error from "
                f"worker-idx={self.config.worker_index}."
            )
        return self.env.step(action)


class TestWorkerFailure(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def do_test(self, alg: str, config: dict, fn=None):
        fn = fn or self._do_test_fault_ignore
        try:
            fn(alg, config)
        finally:
            _register_all()  # re-register the evicted objects

    def _do_test_fault_ignore(self, algo: str, config: dict):
        register_env("fault_env", lambda c: FaultInjectEnv(c))
        algo_cls = get_algorithm_class(algo)

        # Test fault handling
        config["num_workers"] = 2
        config["ignore_worker_failures"] = True
        # Make worker idx=1 fail. Other workers will be ok.
        config["env_config"] = {"bad_indices": [1]}

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            algo = algo_cls(config=config, env="fault_env")
            result = algo.train()
            self.assertTrue(result["num_healthy_workers"], 1)
            algo.stop()

    def _do_test_fault_fatal(self, alg, config):
        register_env("fault_env", lambda c: FaultInjectEnv(c))
        agent_cls = get_algorithm_class(alg)

        # Test raises real error when out of workers
        config["num_workers"] = 2
        config["ignore_worker_failures"] = True
        # Make both worker idx=1 and 2 fail.
        config["env_config"] = {"bad_indices": [1, 2]}

        for _ in framework_iterator(config, frameworks=("torch", "tf")):
            a = agent_cls(config=config, env="fault_env")
            self.assertRaises(Exception, lambda: a.train())
            a.stop()

    def _do_test_fault_fatal_but_recreate(self, alg, config):
        register_env("fault_env", lambda c: FaultInjectEnv(c))
        agent_cls = get_algorithm_class(alg)

        # Test raises real error when out of workers
        config["num_workers"] = 2
        config["recreate_failed_workers"] = True
        # Make both worker idx=1 and 2 fail.
        config["env_config"] = {"bad_indices": [1, 2]}

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            a = agent_cls(config=config, env="fault_env")
            # Expect this to go well and all faulty workers are recovered.
            self.assertTrue(
                not any(
                    ray.get(
                        worker.apply.remote(
                            lambda w: w.recreated_worker
                            or w.env_context.recreated_worker
                        )
                    )
                    for worker in a.workers.remote_workers()
                )
            )
            result = a.train()
            self.assertTrue(result["num_healthy_workers"], 2)
            self.assertTrue(
                all(
                    ray.get(
                        worker.apply.remote(
                            lambda w: w.recreated_worker
                            and w.env_context.recreated_worker
                        )
                    )
                    for worker in a.workers.remote_workers()
                )
            )
            # This should also work several times.
            result = a.train()
            self.assertTrue(result["num_healthy_workers"], 2)
            a.stop()

    def test_fatal(self):
        # Test the case where all workers fail (w/o recovery).
        self.do_test("PG", {"optimizer": {}}, fn=self._do_test_fault_fatal)

    def test_fatal_but_recover(self):
        # Test the case where all workers fail, but we chose to recover.
        self.do_test("PG", {"optimizer": {}}, fn=self._do_test_fault_fatal_but_recreate)

    def test_async_grads(self):
        self.do_test("A3C", {"optimizer": {"grads_per_step": 1}})

    def test_async_replay(self):
        self.do_test(
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
        self.do_test("IMPALA", {"num_gpus": 0})

    def test_sync_replay(self):
        self.do_test("DQN", {"min_sample_timesteps_per_iteration": 1})

    def test_multi_g_p_u(self):
        self.do_test(
            "PPO",
            {
                "num_sgd_iter": 1,
                "train_batch_size": 10,
                "rollout_fragment_length": 10,
                "sgd_minibatch_size": 1,
            },
        )

    def test_sync_samples(self):
        self.do_test("PG", {"optimizer": {}})

    def test_async_sampling_option(self):
        self.do_test("PG", {"optimizer": {}, "sample_async": True})


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
