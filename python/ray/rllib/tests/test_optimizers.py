from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import tensorflow as tf
import time
import unittest

import ray
from ray.rllib.agents.ppo import PPOAgent
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.rllib.evaluation import SampleBatch
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.optimizers import AsyncGradientsOptimizer, AsyncSamplesOptimizer
from ray.rllib.tests.mock_evaluator import _MockEvaluator


class AsyncOptimizerTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testBasic(self):
        ray.init(num_cpus=4)
        local = _MockEvaluator()
        remotes = ray.remote(_MockEvaluator)
        remote_evaluators = [remotes.remote() for i in range(5)]
        test_optimizer = AsyncGradientsOptimizer(local, remote_evaluators,
                                                 {"grads_per_step": 10})
        test_optimizer.step()
        self.assertTrue(all(local.get_weights() == 0))


class PPOCollectTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testPPOSampleWaste(self):
        ray.init(num_cpus=4)

        # Check we at least collect the initial wave of samples
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "train_batch_size": 128,
                "num_workers": 3,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 600)
        ppo.stop()

        # Check we collect at least the specified amount of samples
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "train_batch_size": 900,
                "num_workers": 3,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 1000)
        ppo.stop()

        # Check in vectorized mode
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "num_envs_per_worker": 2,
                "train_batch_size": 900,
                "num_workers": 3,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 1200)
        ppo.stop()

        # Check legacy mode
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "train_batch_size": 128,
                "num_workers": 3,
                "straggler_mitigation": True,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 200)
        ppo.stop()


class SampleBatchTest(unittest.TestCase):
    def testConcat(self):
        b1 = SampleBatch({"a": np.array([1, 2, 3]), "b": np.array([4, 5, 6])})
        b2 = SampleBatch({"a": np.array([1]), "b": np.array([4])})
        b3 = SampleBatch({"a": np.array([1]), "b": np.array([5])})
        b12 = b1.concat(b2)
        self.assertEqual(b12["a"].tolist(), [1, 2, 3, 1])
        self.assertEqual(b12["b"].tolist(), [4, 5, 6, 4])
        b = SampleBatch.concat_samples([b1, b2, b3])
        self.assertEqual(b["a"].tolist(), [1, 2, 3, 1, 1])
        self.assertEqual(b["b"].tolist(), [4, 5, 6, 4, 5])


class AsyncSamplesOptimizerTest(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=8)

    def testSimple(self):
        local, remotes = self._make_evs()
        optimizer = AsyncSamplesOptimizer(local, remotes, {})
        self._wait_for(optimizer, 1000, 1000)

    def testMultiGPU(self):
        local, remotes = self._make_evs()
        optimizer = AsyncSamplesOptimizer(local, remotes, {
            "num_gpus": 2,
            "_fake_gpus": True
        })
        self._wait_for(optimizer, 1000, 1000)

    def testMultiGPUParallelLoad(self):
        local, remotes = self._make_evs()
        optimizer = AsyncSamplesOptimizer(local, remotes, {
            "num_gpus": 2,
            "num_data_loader_buffers": 2,
            "_fake_gpus": True
        })
        self._wait_for(optimizer, 1000, 1000)

    def testMultiplePasses(self):
        local, remotes = self._make_evs()
        optimizer = AsyncSamplesOptimizer(
            local, remotes, {
                "minibatch_buffer_size": 10,
                "num_sgd_iter": 10,
                "sample_batch_size": 10,
                "train_batch_size": 50,
            })
        self._wait_for(optimizer, 1000, 10000)
        self.assertLess(optimizer.stats()["num_steps_sampled"], 5000)
        self.assertGreater(optimizer.stats()["num_steps_trained"], 8000)

    def testReplay(self):
        local, remotes = self._make_evs()
        optimizer = AsyncSamplesOptimizer(
            local, remotes, {
                "replay_buffer_num_slots": 100,
                "replay_proportion": 10,
                "sample_batch_size": 10,
                "train_batch_size": 10,
            })
        self._wait_for(optimizer, 1000, 1000)
        self.assertLess(optimizer.stats()["num_steps_sampled"], 5000)
        self.assertGreater(optimizer.stats()["num_steps_replayed"], 8000)
        self.assertGreater(optimizer.stats()["num_steps_trained"], 8000)

    def testReplayAndMultiplePasses(self):
        local, remotes = self._make_evs()
        optimizer = AsyncSamplesOptimizer(
            local, remotes, {
                "minibatch_buffer_size": 10,
                "num_sgd_iter": 10,
                "replay_buffer_num_slots": 100,
                "replay_proportion": 10,
                "sample_batch_size": 10,
                "train_batch_size": 10,
            })
        self._wait_for(optimizer, 1000, 1000)
        self.assertLess(optimizer.stats()["num_steps_sampled"], 5000)
        self.assertGreater(optimizer.stats()["num_steps_replayed"], 8000)
        self.assertGreater(optimizer.stats()["num_steps_trained"], 40000)

    def testRejectBadConfigs(self):
        local, remotes = self._make_evs()
        self.assertRaises(
            ValueError, lambda: AsyncSamplesOptimizer(
                local, remotes,
                {"num_data_loader_buffers": 2, "minibatch_buffer_size": 4}))
        optimizer = AsyncSamplesOptimizer(
            local, remotes, {
                "num_gpus": 2,
                "train_batch_size": 100,
                "sample_batch_size": 50,
                "_fake_gpus": True
            })
        self._wait_for(optimizer, 1000, 1000)
        optimizer = AsyncSamplesOptimizer(
            local, remotes, {
                "num_gpus": 2,
                "train_batch_size": 100,
                "sample_batch_size": 25,
                "_fake_gpus": True
            })
        self._wait_for(optimizer, 1000, 1000)
        optimizer = AsyncSamplesOptimizer(
            local, remotes, {
                "num_gpus": 2,
                "train_batch_size": 100,
                "sample_batch_size": 74,
                "_fake_gpus": True
            })
        self._wait_for(optimizer, 1000, 1000)

    def _make_evs(self):
        def make_sess():
            return tf.Session(config=tf.ConfigProto(device_count={"CPU": 2}))

        local = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=PPOPolicyGraph,
            tf_session_creator=make_sess)
        remotes = [
            PolicyEvaluator.as_remote().remote(
                env_creator=lambda _: gym.make("CartPole-v0"),
                policy_graph=PPOPolicyGraph,
                tf_session_creator=make_sess)
        ]
        return local, remotes

    def _wait_for(self, optimizer, num_steps_sampled, num_steps_trained):
        start = time.time()
        while time.time() - start < 30:
            optimizer.step()
            if optimizer.num_steps_sampled > num_steps_sampled and \
                    optimizer.num_steps_trained > num_steps_trained:
                print("OK", optimizer.stats())
                return
        raise AssertionError("TIMED OUT", optimizer.stats())


if __name__ == "__main__":
    unittest.main(verbosity=2)
