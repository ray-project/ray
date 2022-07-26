import os
import unittest
from typing import Dict

import numpy as np
from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline import JsonReader
from ray.rllib.offline.estimators.doubly_robust import DoublyRobust
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.framework import try_import_torch

import ray

_, nn = try_import_torch()


class TestDR(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        checkpoint_dir = "/tmp/cartpole/"
        num_episodes = 20
        cls.gamma = 0.99

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(num_rollout_workers=0)
        )
        cls.algo = config.build()
        cls.q_model_config = {
            "n_iters": 160,
            "minibatch_size": 32,
            "tau": 1.0,
        }

        cls.random_path = os.path.join(checkpoint_dir, "checkpoint", "random")
        (
            cls.random_batch,
            cls.random_reward,
            cls.random_std,
        ) = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "random"),
            cls.gamma,
            num_episodes,
        )
        print(
            f"Collected random batch of {cls.random_batch.count} steps "
            f"with return {cls.random_reward} stddev {cls.random_std}"
        )

        cls.mixed_path = os.path.join(checkpoint_dir, "checkpoint", "mixed")
        cls.mixed_batch, cls.mixed_reward, cls.mixed_std = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "mixed"),
            cls.gamma,
            num_episodes,
        )
        print(
            f"Collected mixed batch of {cls.mixed_batch.count} steps "
            f"with return {cls.mixed_reward} stddev {cls.mixed_std}"
        )

        cls.expert_path = os.path.join(checkpoint_dir, "checkpoint", "expert")
        (
            cls.expert_batch,
            cls.expert_reward,
            cls.expert_std,
        ) = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "expert"),
            cls.gamma,
            num_episodes,
        )
        print(
            f"Collected expert batch of {cls.expert_batch.count} steps "
            f"with return {cls.expert_reward} stddev {cls.expert_std}"
        )

    @classmethod
    def tearDownClass(cls):
        cls.algo.stop()
        ray.shutdown()

    @staticmethod
    def get_batch_and_mean_ret(data_path: str, gamma: float, num_episodes: int):
        reader = JsonReader(data_path)
        ep_ret = []
        batches = []
        n_eps = 0
        while n_eps < num_episodes:
            batch = reader.next()
            batches.append(batch)
            for episode in batch.split_by_episode():
                ret = 0
                for r in episode[SampleBatch.REWARDS][::-1]:
                    ret = r + gamma * ret
                ep_ret.append(ret)
                n_eps += 1
        return concat_samples(batches), np.mean(ep_ret), np.std(ep_ret)

    @staticmethod
    def check(
        algo: Algorithm,
        checkpoint: str,
        batch: SampleBatch,
        mean_ret: float,
        std_ret: float,
        q_model_config: Dict,
    ):
        algo.load_checkpoint(checkpoint)

        q_model_config = q_model_config.copy()
        estimator = DoublyRobust(
            policy=algo.get_policy(),
            gamma=algo.config["gamma"],
            q_model_config=q_model_config,
        )
        loss = estimator.train(batch)["loss"]
        estimates = estimator.estimate(batch)
        est_mean = estimates["v_target"]
        est_std = estimates["v_target_std"]
        print(
            f"{est_mean:.2f}, {est_std:.2f}, {mean_ret:.2f}, {std_ret:.2f}, {loss:.2f}"
        )
        # Assert that the two mean +- stddev intervals overlap
        assert (
            est_mean - est_std <= mean_ret + std_ret
            and mean_ret - std_ret <= est_mean + est_std
        ), f"DR estimate {est_mean:.2f} with stddev {est_std:.2f} does not "
        f"converge to true estimate {mean_ret:.2f} with stddev {std_ret:.2f}!"

    def test_random_random_data(self):
        print("Test random policy on random dataset")
        self.check(
            self.algo,
            self.random_path,
            self.random_batch,
            self.random_reward,
            self.random_std,
            self.q_model_config,
        )

    def test_random_mixed_data(self):
        print("Test random policy on mixed dataset")
        self.check(
            self.algo,
            self.random_path,
            self.mixed_batch,
            self.random_reward,
            self.random_std,
            self.q_model_config,
        )

    def test_random_expert_data(self):
        print("Test random policy on expert dataset")
        self.check(
            self.algo,
            self.random_path,
            self.expert_batch,
            self.random_reward,
            self.random_std,
            self.q_model_config,
        )

    def test_mixed_random_data(self):
        print("Test mixed policy on random dataset")
        self.check(
            self.algo,
            self.mixed_path,
            self.random_batch,
            self.mixed_reward,
            self.mixed_std,
            self.q_model_config,
        )

    def test_mixed_mixed_data(self):
        print("Test mixed policy on mixed dataset")
        self.check(
            self.algo,
            self.mixed_path,
            self.mixed_batch,
            self.mixed_reward,
            self.mixed_std,
            self.q_model_config,
        )

    def test_mixed_expert_data(self):
        print("Test mixed policy on expert dataset")
        self.check(
            self.algo,
            self.mixed_path,
            self.expert_batch,
            self.mixed_reward,
            self.mixed_std,
            self.q_model_config,
        )

    def test_expert_random_data(self):
        print("Test expert policy on random dataset")
        self.check(
            self.algo,
            self.expert_path,
            self.random_batch,
            self.expert_reward,
            self.expert_std,
            self.q_model_config,
        )

    def test_expert_mixed_data(self):
        print("Test expert policy on mixed dataset")
        self.check(
            self.algo,
            self.expert_path,
            self.mixed_batch,
            self.expert_reward,
            self.expert_std,
            self.q_model_config,
        )

    def test_expert_expert_data(self):
        print("Test expert policy on expert dataset")
        self.check(
            self.algo,
            self.expert_path,
            self.expert_batch,
            self.expert_reward,
            self.expert_std,
            self.q_model_config,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
