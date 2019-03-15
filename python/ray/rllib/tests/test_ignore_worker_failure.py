from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import unittest

import ray
from ray.rllib import _register_all
from ray.rllib.agents.registry import get_agent_class
from ray.tune.registry import register_env


class FaultInjectEnv(gym.Env):
    def __init__(self, config):
        self.env = gym.make("CartPole-v0")
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space
        self.config = config

    def reset(self):
        return self.env.reset()

    def step(self, action):
        if self.config.worker_index in self.config["bad_indices"]:
            raise ValueError("This is a simulated error from {}".format(
                self.config.worker_index))
        return self.env.step(action)


class IgnoresWorkerFailure(unittest.TestCase):
    def doTest(self, alg, config, fn=None):
        fn = fn or self._doTestFaultRecover
        try:
            ray.init(num_cpus=6)
            fn(alg, config)
        finally:
            ray.shutdown()
            _register_all()  # re-register the evicted objects

    def _doTestFaultRecover(self, alg, config):
        register_env("fault_env", lambda c: FaultInjectEnv(c))
        agent_cls = get_agent_class(alg)

        # Test fault handling
        config["num_workers"] = 2
        config["ignore_worker_failures"] = True
        config["env_config"] = {"bad_indices": [1]}
        a = agent_cls(config=config, env="fault_env")
        result = a.train()
        self.assertTrue(result["num_healthy_workers"], 1)
        a.stop()

    def _doTestFaultFatal(self, alg, config):
        register_env("fault_env", lambda c: FaultInjectEnv(c))
        agent_cls = get_agent_class(alg)

        # Test raises real error when out of workers
        config["num_workers"] = 2
        config["ignore_worker_failures"] = True
        config["env_config"] = {"bad_indices": [1, 2]}
        a = agent_cls(config=config, env="fault_env")
        self.assertRaises(Exception, lambda: a.train())
        a.stop()

    def testFatal(self):
        # test the case where all workers fail
        self.doTest("PG", {"optimizer": {}}, fn=self._doTestFaultFatal)

    def testAsyncGrads(self):
        self.doTest("A3C", {"optimizer": {"grads_per_step": 1}})

    def testAsyncReplay(self):
        self.doTest(
            "APEX", {
                "timesteps_per_iteration": 1000,
                "num_gpus": 0,
                "min_iter_time_s": 1,
                "learning_starts": 1000,
                "target_network_update_freq": 100,
                "optimizer": {
                    "num_replay_buffer_shards": 1,
                },
            })

    def testAsyncSamples(self):
        self.doTest("IMPALA", {"num_gpus": 0})

    def testSyncReplay(self):
        self.doTest("DQN", {"timesteps_per_iteration": 1})

    def testMultiGPU(self):
        self.doTest(
            "PPO", {
                "num_sgd_iter": 1,
                "train_batch_size": 10,
                "sample_batch_size": 10,
                "sgd_minibatch_size": 1,
            })

    def testSyncSamples(self):
        self.doTest("PG", {"optimizer": {}})

    def testAsyncSamplingOption(self):
        self.doTest("PG", {"optimizer": {}, "sample_async": True})


if __name__ == "__main__":
    unittest.main(verbosity=2)
