import os
import shutil
import unittest
from typing import Dict

import numpy as np
from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.offline import JsonReader, JsonWriter
from ray.rllib.offline.estimators.doubly_robust import DoublyRobust
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.numpy import convert_to_numpy

import ray


class TestDR(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        regenerate_data = False
        num_workers = 4 if regenerate_data else 0
        checkpoint_dir = "/tmp/cartpole/"
        num_episodes = 100
        cls.gamma = 0.99

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(
                num_rollout_workers=num_workers,
                batch_mode="complete_episodes",
            )
            .exploration(exploration_config={"type": "SoftQ", "temperature": 0.1})
        )
        cls.algo = config.build()
        cls.q_model_config = {"n_iters": 600, "minibatch_size": 128, "tau": 1.0}

        if regenerate_data:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)
            os.makedirs(checkpoint_dir, exist_ok=True)

        if regenerate_data:
            cls.generate_data(cls.algo, checkpoint_dir, "random", 20, num_episodes)
            print("Generated random dataset")
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

        if regenerate_data:
            cls.generate_data(cls.algo, checkpoint_dir, "mixed", 120, num_episodes)
            print("Generated mixed dataset")
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

        if regenerate_data:
            cls.generate_data(cls.algo, checkpoint_dir, "expert", 200, num_episodes)
            print("Generated expert dataset")
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
    def generate_data(
        algo: Algorithm,
        checkpoint_dir: str,
        name: str,
        stop_reward: float,
        num_episodes: int,
    ):
        """Generates training data and writes it to an offline dataset

        Train the algorithm until `episode_reward_mean` reaches the given `stop_reward`,
        then save a checkpoint to `checkpoint_dir`/checkpoint/`name`
        and a dataset with `n_episodes` episodes to `checkpoint_dir`/data/`name`.
        """
        results = algo.train()
        while results["episode_reward_mean"] < stop_reward:
            results = algo.train()

        checkpoint = algo.save_checkpoint(checkpoint_dir)
        checkpoint_path = os.path.join(checkpoint_dir, "checkpoint", name)
        os.renames(checkpoint, checkpoint_path)

        output_path = os.path.join(checkpoint_dir, "data", name)
        writer = JsonWriter(output_path)
        for _ in range(num_episodes // algo.config["num_workers"] + 1):
            # This is how many episodes we get per iteration
            batch = synchronous_parallel_sample(worker_set=algo.workers)
            log_likelihoods = algo.get_policy().compute_log_likelihoods(
                actions=batch[SampleBatch.ACTIONS],
                obs_batch=batch[SampleBatch.OBS],
                actions_normalized=False,
            )
            log_likelihoods = convert_to_numpy(log_likelihoods)
            batch[SampleBatch.ACTION_PROB] = np.exp(log_likelihoods)
            writer.write(batch)

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
