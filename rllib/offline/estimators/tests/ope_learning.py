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


def generate_data(checkpoint_dir: str, mean_reward: float, num_episodes: int):
    config = (
        DQNConfig()
        .environment(env="CartPole-v0")
        .framework("torch")
        .rollouts(num_rollout_workers=4, batch_mode="complete_episodes")
    )
    algo = config.build()
    results = algo.train()
    while results["episode_reward_mean"] < mean_reward:
        results = algo.train()
    checkpoint = algo.save_checkpoint(checkpoint_dir)

    batches = []
    eps = 0
    while eps < num_episodes:
        batch = synchronous_parallel_sample(worker_set=algo.workers)
        batches.append(batch)
        eps += len(set(batch[SampleBatch.EPS_ID]))
    batch = concat_samples(batches)
    algo.stop()

    return checkpoint, batch


def test_fqe(checkpoint: str, batch: SampleBatch, mean_reward: float):
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
    algo.stop()
    estimates = convert_to_numpy(estimates)
    print(np.mean(estimates), mean_reward, np.mean(losses))


class TestOPELearning(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        cls.checkpoint_dir = tempfile.mkdtemp(prefix="fqe/")

        cls.random_reward = 10
        cls.mixed_reward = 120
        cls.optimal_reward = 200
        cls.num_episodes = 100

        cls.random_path, cls.random_data = generate_data(
            cls.checkpoint_dir,
            cls.random_reward,
            cls.num_episodes,
        )
        print("Generated random batch")

        # cls.mixed_path, cls.mixed_data = generate_data(
        #     cls.checkpoint_dir,
        #     cls.mixed_reward,
        #     cls.num_episodes,
        # )
        # print("Generated mixed batch")

        # cls.optimal_path, cls.optimal_data = generate_data(
        #     cls.checkpoint_dir,
        #     cls.checkpoint_dir + "optimal",
        #     cls.optimal_reward,
        #     cls.num_episodes,
        # )
        # print("Generated optimal batch")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.checkpoint_dir)
        ray.shutdown()

    def test_random(self):
        test_fqe(self.random_path, self.random_data, self.random_reward)

    # def test_mixed(self):
    #     test_fqe(self.mixed_path, self.mixed_data, self.mixed_reward)

    # def test_optimal(self):
    #     test_fqe(self.optimal_path, self.optimal_data, self.optimal_reward)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
