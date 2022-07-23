from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.utils.numpy import convert_to_numpy
import ray
import numpy as np
import os
import unittest


class TestFQE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        checkpoint_dir = "/tmp/cartpole/"
        num_episodes = 100

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(num_rollout_workers=0)
        )
        cls.algo = config.build()

        cls.random_path = os.path.join(checkpoint_dir, "checkpoint", "random")
        reader = JsonReader(os.path.join(checkpoint_dir, "data", "random"))
        cls.random_batch = concat_samples([reader.next() for _ in range(num_episodes)])
        cls.random_reward = 14.8

        cls.mixed_path = os.path.join(checkpoint_dir, "checkpoint", "mixed")
        reader = JsonReader(os.path.join(checkpoint_dir, "data", "mixed"))
        cls.mixed_batch = concat_samples([reader.next() for _ in range(num_episodes)])
        cls.mixed_reward = 73.8

        cls.optimal_path = os.path.join(checkpoint_dir, "checkpoint", "optimal")
        reader = JsonReader(os.path.join(checkpoint_dir, "data", "optimal"))
        cls.optimal_batch = concat_samples([reader.next() for _ in range(num_episodes)])
        cls.optimal_reward = 86.6

    @classmethod
    def tearDownClass(cls):
        cls.algo.stop()
        ray.shutdown()

    def _check_fqe(self, checkpoint: str, batch: SampleBatch, mean_ret: float):
        self.algo.load_checkpoint(checkpoint)

        fqe = FQETorchModel(
            policy=self.algo.get_policy(),
            gamma=self.algo.config["gamma"],
            n_iters=160,
            minibatch_size=128,
            tau=1,
        )
        losses = fqe.train(batch)
        estimates = fqe.estimate_v(batch)
        estimates = convert_to_numpy(estimates)
        # Change to check()
        print(np.mean(estimates), mean_ret, np.mean(losses))

    def test_random(self):
        self._check_fqe(self.random_path, self.random_batch, self.random_reward)

    def test_mixed(self):
        self._check_fqe(self.mixed_path, self.mixed_batch, self.mixed_reward)

    def test_optimal(self):
        self._check_fqe(self.optimal_path, self.optimal_batch, self.optimal_reward)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
