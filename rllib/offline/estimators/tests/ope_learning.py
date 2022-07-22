from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.numpy import convert_to_numpy
import ray
import numpy as np
import shutil
import tempfile
import unittest


class TestOPELearning(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        cls.checkpoint_dir = tempfile.mkdtemp()
        cls.num_episodes = 100

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(num_rollout_workers=8, batch_mode="complete_episodes")
        )
        algo = config.build()
        cls.random_path = algo.save_checkpoint(cls.checkpoint_dir)
        cls.random_batch, cls.random_reward = cls._collect_batch(algo, cls.num_episodes)

        results = algo.train()
        while results["episode_reward_mean"] < 120:
            results = algo.train()
        cls.mixed_path = algo.save_checkpoint(cls.checkpoint_dir)
        cls.mixed_batch, cls.mixed_reward = cls._collect_batch(algo, cls.num_episodes)

        while results["episode_reward_mean"] < 200:
            results = algo.train()
        cls.optimal_path = algo.save_checkpoint(cls.checkpoint_dir)
        cls.optimal_batch, cls.optimal_reward = cls._collect_batch(
            algo, cls.num_episodes
        )
        algo.stop()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.checkpoint_dir)
        ray.shutdown()

    @staticmethod
    def _collect_batch(algo, num_episodes):
        batches = []
        mean_ret = []
        gamma = algo.config["gamma"]

        for _ in range(num_episodes, algo.config["num_workers"]):
            episodes = synchronous_parallel_sample(
                worker_set=algo.workers, concat=False
            )
            batches.extend(episodes)
            for episode in episodes:
                ep_ret = 0
                for r in episode[SampleBatch.REWARDS][::-1]:
                    ep_ret = r + gamma * ep_ret
                mean_ret.append(ep_ret)

        batch = concat_samples(batches)
        mean_ret = np.mean(mean_ret)
        return batch, mean_ret

    @staticmethod
    def _check_fqe(checkpoint: str, batch: SampleBatch, mean_ret: float):
        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(num_rollout_workers=0)
        )
        algo = config.build()
        algo.load_checkpoint(checkpoint)

        fqe = FQETorchModel(
            policy=algo.get_policy(),
            gamma=algo.config["gamma"],
            n_iters=160,
            minibatch_size=128,
            tau=1,
        )
        losses = fqe.train(batch)
        estimates = fqe.estimate_v(batch)
        estimates = convert_to_numpy(estimates)
        # Change to check()
        print(np.mean(estimates), mean_ret, np.mean(losses))
        algo.stop()

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
