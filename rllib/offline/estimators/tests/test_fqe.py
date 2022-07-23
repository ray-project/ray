import copy
import os
import shutil
import unittest
from typing import Dict

import numpy as np
import torch
from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.offline import JsonReader, JsonWriter
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check

import ray


class TestFQE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        regenerate_data = False
        num_workers = 4 if regenerate_data else 0
        checkpoint_dir = "/tmp/cartpole/"
        num_episodes = 1000
        cls.gamma = 0.99

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(
                num_rollout_workers=num_workers,
                batch_mode="complete_episodes",
            )
        )
        cls.algo = config.build()
        cls.fqe_config = {"n_iters": 160, "minibatch_size": 256, "tau": 1.0}

        if regenerate_data:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)
            os.makedirs(checkpoint_dir, exist_ok=True)

        if regenerate_data:
            cls.generate_data(cls.algo, checkpoint_dir, "random", 20, num_episodes)
            print("Generated random dataset")
        cls.random_path = os.path.join(checkpoint_dir, "checkpoint", "random")
        cls.random_batch, cls.random_reward = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "random"),
            cls.gamma,
            num_episodes,
        )
        print(
            f"Random batch length {cls.random_batch.count} return {cls.random_reward}"
        )

        if regenerate_data:
            cls.generate_data(cls.algo, checkpoint_dir, "mixed", 120, num_episodes)
            print("Generated mixed dataset")
        cls.mixed_path = os.path.join(checkpoint_dir, "checkpoint", "mixed")
        cls.mixed_batch, cls.mixed_reward = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "mixed"),
            cls.gamma,
            num_episodes,
        )
        print(f"Mixed batch length {cls.mixed_batch.count} return {cls.mixed_reward}")

        if regenerate_data:
            cls.generate_data(cls.algo, checkpoint_dir, "expert", 200, num_episodes)
            print("Generated expert dataset")
        cls.expert_path = os.path.join(checkpoint_dir, "checkpoint", "expert")
        cls.expert_batch, cls.expert_reward = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "expert"),
            cls.gamma,
            num_episodes,
        )
        print(
            f"expert batch length {cls.expert_batch.count} return {cls.expert_reward}"
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
        return concat_samples(batches), np.mean(ep_ret)

    @staticmethod
    def check_fqe(
        algo: Algorithm,
        checkpoint: str,
        batch: SampleBatch,
        mean_ret: float,
        fqe_config: Dict,
    ):
        algo.load_checkpoint(checkpoint)

        fqe = FQETorchModel(
            policy=algo.get_policy(),
            gamma=algo.config["gamma"],
            **fqe_config,
        )
        losses = fqe.train(batch)
        estimates = fqe.estimate_v(batch)
        estimates = convert_to_numpy(estimates)
        # Change to check()
        print(np.mean(estimates), np.std(estimates), mean_ret, np.mean(losses))

    def test_fqe_compilation_and_stopping(self):
        # Test FQETorchModel for:
        # (1) Check that it does not modify the underlying batch during training
        # (2) Check that the stopping criteria from FQE are working correctly
        # (3) Check that using fqe._compute_action_probs equals brute force
        # iterating over all actions with policy.compute_log_likelihoods
        fqe = FQETorchModel(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        tmp_batch = copy.deepcopy(self.random_batch)
        losses = fqe.train(self.random_batch)

        # Make sure FQETorchModel.train() does not modify the batch
        check(tmp_batch, self.random_batch)

        # Make sure FQE stopping criteria are respected
        assert (
            len(losses) == fqe.n_iters or losses[-1] < fqe.delta
        ), f"FQE.train() terminated early in {len(losses)} steps with final loss"
        f"{losses[-1]} for n_iters: {fqe.n_iters} and delta: {fqe.delta}"

        # Test fqe._compute_action_probs against "brute force" method
        # of computing log_prob for each possible action individually
        # using policy.compute_log_likelihoods
        obs = torch.tensor(self.random_batch["obs"], device=fqe.device)
        action_probs = fqe._compute_action_probs(obs)
        action_probs = convert_to_numpy(action_probs)

        tmp_probs = []
        for act in range(fqe.policy.action_space.n):
            tmp_actions = np.zeros_like(self.random_batch["actions"]) + act
            log_probs = fqe.policy.compute_log_likelihoods(
                actions=tmp_actions,
                obs_batch=self.random_batch["obs"],
            )
            tmp_probs.append(torch.exp(log_probs))
        tmp_probs = torch.stack(tmp_probs).transpose(0, 1)
        tmp_probs = convert_to_numpy(tmp_probs)
        check(action_probs, tmp_probs, decimals=3)

    def test_random(self):
        self.check_fqe(
            self.algo,
            self.random_path,
            self.random_batch,
            self.random_reward,
            self.fqe_config,
        )

    def test_mixed(self):
        self.check_fqe(
            self.algo,
            self.mixed_path,
            self.mixed_batch,
            self.mixed_reward,
            self.fqe_config,
        )

    def test_expert(self):
        self.check_fqe(
            self.algo,
            self.expert_path,
            self.expert_batch,
            self.expert_reward,
            self.fqe_config,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
