from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.offline import JsonReader, JsonWriter
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check
import ray
import torch
import copy
import numpy as np
import shutil
import os
import unittest


class TestFQE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        checkpoint_dir = "/tmp/cartpole/"
        num_episodes = 100
        recollect_data = True
        cls.gamma = 0.99

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .framework("torch")
            .rollouts(num_rollout_workers=8, batch_mode="complete_episodes")
        )
        cls.algo = config.build()

        if recollect_data:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)
            os.makedirs(checkpoint_dir, exist_ok=True)

            cls.collect_data(cls.algo, checkpoint_dir, "random", 20, num_episodes)
            cls.collect_data(cls.algo, checkpoint_dir, "mixed", 120, num_episodes)
            cls.collect_data(cls.algo, checkpoint_dir, "optimal", 200, num_episodes)

        cls.random_path = os.path.join(checkpoint_dir, "checkpoint", "random")
        cls.random_batch, cls.random_reward = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "random"),
            cls.gamma,
            num_episodes,
        )
        # TODO (Rohan138): Add error message
        assert cls.random_reward < 20

        cls.mixed_path = os.path.join(checkpoint_dir, "checkpoint", "mixed")
        cls.mixed_batch, cls.mixed_reward = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "mixed"),
            cls.gamma,
            num_episodes,
        )

        cls.optimal_path = os.path.join(checkpoint_dir, "checkpoint", "optimal")
        cls.optimal_batch, cls.optimal_reward = cls.get_batch_and_mean_ret(
            os.path.join(checkpoint_dir, "data", "optimal"),
            cls.gamma,
            num_episodes,
        )
        # TODO (Rohan138): Add error message
        assert cls.optimal_reward > 85

    @classmethod
    def tearDownClass(cls):
        cls.algo.stop()
        ray.shutdown()

    @staticmethod
    def collect_data(
        algo: Algorithm,
        checkpoint_dir: str,
        name: str,
        stop_reward: float,
        num_episodes: int,
    ):
        results = algo.train()
        while results["episode_reward_mean"] < stop_reward:
            results = algo.train()

        checkpoint = algo.save_checkpoint(checkpoint_dir)
        checkpoint_path = os.path.join(checkpoint_dir, "checkpoint", name)
        os.renames(checkpoint, checkpoint_path)

        output_path = os.path.join(checkpoint_dir, "data", name)
        writer = JsonWriter(output_path)
        n_episodes = 0
        while n_episodes < num_episodes:
            episodes = synchronous_parallel_sample(
                worker_set=algo.workers, concat=False
            )
            for episode in episodes:
                writer.write(episode)

    @staticmethod
    def get_batch_and_mean_ret(data_path: str, gamma: float, num_episodes: int):
        reader = JsonReader(data_path)
        ep_ret = []
        batches = []
        n_eps = 0
        while n_eps < num_episodes:
            episode = batches.append(reader.next())
            ret = 0
            for r in episode[SampleBatch.REWARDS][::-1]:
                ret = r + gamma * ret
            ep_ret.append(ret)
        return concat_samples(batches), np.mean(ep_ret)

    @staticmethod
    def check_fqe(
        algo: Algorithm, checkpoint: str, batch: SampleBatch, mean_ret: float
    ):
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

    def test_fqe_model(self):
        # Test FQETorchModel for:
        # (1) Check that it does not modify the underlying batch during training
        # (2) Check that the stopping criteria from FQE are working correctly
        # (3) Check that using fqe._compute_action_probs equals brute force
        # iterating over all actions with policy.compute_log_likelihoods
        fqe = FQETorchModel(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        tmp_batch = copy.deepcopy(self.batch)
        losses = fqe.train(self.batch)

        # Make sure FQETorchModel.train() does not modify self.batch
        check(tmp_batch, self.batch)

        # Make sure FQE stopping criteria are respected
        assert (
            len(losses) == fqe.n_iters or losses[-1] < fqe.delta
        ), f"FQE.train() terminated early in {len(losses)} steps with final loss"
        f"{losses[-1]} for n_iters: {fqe.n_iters} and delta: {fqe.delta}"

        # Test fqe._compute_action_probs against "brute force" method
        # of computing log_prob for each possible action individually
        # using policy.compute_log_likelihoods
        obs = torch.tensor(self.batch["obs"], device=fqe.device)
        action_probs = fqe._compute_action_probs(obs)
        action_probs = convert_to_numpy(action_probs)

        tmp_probs = []
        for act in range(fqe.policy.action_space.n):
            tmp_actions = np.zeros_like(self.batch["actions"]) + act
            log_probs = fqe.policy.compute_log_likelihoods(
                actions=tmp_actions,
                obs_batch=self.batch["obs"],
            )
            tmp_probs.append(torch.exp(log_probs))
        tmp_probs = torch.stack(tmp_probs).transpose(0, 1)
        tmp_probs = convert_to_numpy(tmp_probs)
        check(action_probs, tmp_probs, decimals=3)

    def test_random(self):
        self.check_fqe(self.random_path, self.random_batch, self.random_reward)

    def test_mixed(self):
        self.check_fqe(self.mixed_path, self.mixed_batch, self.mixed_reward)

    def test_optimal(self):
        self.check_fqe(self.optimal_path, self.optimal_batch, self.optimal_reward)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
