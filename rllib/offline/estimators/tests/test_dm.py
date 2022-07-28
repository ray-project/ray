import unittest
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.algorithms import AlgorithmConfig

import numpy as np
from ray.rllib.offline.estimators import DirectMethod
from ray.rllib.offline.estimators.tests.gridworld import GridWorldPolicy, GridWorldEnv
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.framework import try_import_torch

import ray

_, nn = try_import_torch()


class TestDM(unittest.TestCase):
    """Learning tests for the Direct Method Estimator"""

    @classmethod
    def setUpClass(cls):
        ray.init()
        # Epsilon-greedy exploration values
        random_eps = 0.8
        mixed_eps = 0.5
        expert_eps = 0.2
        cls.num_episodes = 32
        cls.gamma = 0.99
        num_workers = 8

        # Config settings for FQE model used in DM estimator
        cls.q_model_config = {
            "n_iters": 600,
            "minibatch_size": 32,
            "tau": 1.0,
            "model": {
                "fcnet_hiddens": [],
                "activation": "linear",
            },
            "lr": 0.01,
        }

        env = GridWorldEnv()
        cls.random_policy = GridWorldPolicy(
            observation_space=env.observation_space,
            action_space=env.action_space,
            config={"epsilon": random_eps},
        )
        cls.mixed_policy = GridWorldPolicy(
            observation_space=env.observation_space,
            action_space=env.action_space,
            config={"epsilon": mixed_eps},
        )
        cls.expert_policy = GridWorldPolicy(
            observation_space=env.observation_space,
            action_space=env.action_space,
            config={"epsilon": expert_eps},
        )

        config = (
            AlgorithmConfig()
            .rollouts(batch_mode="complete_episodes")
            .environment(disable_env_checking=True)
            .experimental(_disable_preprocessor_api=True)
            .to_dict()
        )

        cls.workers = WorkerSet(
            env_creator=lambda env_config: GridWorldEnv(),
            policy_class=GridWorldPolicy,
            trainer_config=config,
            num_workers=num_workers,
        )

        (
            cls.random_batch,
            cls.random_reward,
            cls.random_std,
        ) = cls.get_batch_and_mean_ret(random_eps)
        print(
            f"Collected random batch of {cls.random_batch.count} steps "
            f"with return {cls.random_reward} stddev {cls.random_std}"
        )

        (
            cls.mixed_batch,
            cls.mixed_reward,
            cls.mixed_std,
        ) = cls.get_batch_and_mean_ret(mixed_eps)
        print(
            f"Collected mixed batch of {cls.mixed_batch.count} steps "
            f"with return {cls.mixed_reward} stddev {cls.mixed_std}"
        )

        (
            cls.expert_batch,
            cls.expert_reward,
            cls.expert_std,
        ) = cls.get_batch_and_mean_ret(expert_eps)
        print(
            f"Collected expert batch of {cls.expert_batch.count} steps "
            f"with return {cls.expert_reward} stddev {cls.expert_std}"
        )
        cls.workers.stop()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    @classmethod
    def get_batch_and_mean_ret(cls, epsilon: float):
        cls.workers.foreach_policy(
            func=lambda policy, _: policy.update_epsilon(epsilon)
        )
        ep_ret = []
        batches = []
        n_eps = 0
        while n_eps < cls.num_episodes:
            batch = synchronous_parallel_sample(worker_set=cls.workers)
            for episode in batch.split_by_episode():
                ret = 0
                for r in episode[SampleBatch.REWARDS][::-1]:
                    ret = r + cls.gamma * ret
                ep_ret.append(ret)
                n_eps += 1
            batches.append(batch)
        return concat_samples(batches), np.mean(ep_ret), np.std(ep_ret)

    @classmethod
    def check_estimate(
        cls,
        policy: Policy,
        batch: SampleBatch,
        mean_ret: float,
        std_ret: float,
    ):

        estimator = DirectMethod(
            policy=policy,
            gamma=cls.gamma,
            q_model_config=cls.q_model_config,
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
        ), (
            f"DM estimate {est_mean:.2f} with stddev {est_std:.2f} does not "
            f"converge to true estimate {mean_ret:.2f} with stddev {std_ret:.2f}!"
        )

    def test_random_random_data(self):
        print("Test random policy on random dataset")
        self.check_estimate(
            self.random_policy,
            self.random_batch,
            self.random_reward,
            self.random_std,
        )

    def test_random_mixed_data(self):
        print("Test random policy on mixed dataset")
        self.check_estimate(
            self.random_policy,
            self.mixed_batch,
            self.random_reward,
            self.random_std,
        )

    @pytest.mark.skip("Skipped out due to flakiness; makes sense since expert episodes"
    "are shorter than random ones, increasing the variance of the estimate")
    def test_random_policy_expert_data(self):
        print("Test random policy on expert dataset")
        self.check_estimate(
            self.random_policy,
            self.expert_batch,
            self.random_reward,
            self.random_std,
        )

    def test_mixed_policy_random_data(self):
        print("Test mixed policy on random dataset")
        self.check_estimate(
            self.mixed_policy,
            self.random_batch,
            self.mixed_reward,
            self.mixed_std,
        )

    def test_mixed_policy_mixed_data(self):
        print("Test mixed policy on mixed dataset")
        self.check_estimate(
            self.mixed_policy,
            self.mixed_batch,
            self.mixed_reward,
            self.mixed_std,
        )

    def test_mixed_policy_expert_data(self):
        print("Test mixed policy on expert dataset")
        self.check_estimate(
            self.mixed_policy,
            self.expert_batch,
            self.mixed_reward,
            self.mixed_std,
        )

    def test_expert_policy_random_data(self):
        print("Test expert policy on random dataset")
        self.check_estimate(
            self.expert_policy,
            self.random_batch,
            self.expert_reward,
            self.expert_std,
        )

    def test_expert_policy_mixed_data(self):
        print("Test expert policy on mixed dataset")
        self.check_estimate(
            self.expert_policy,
            self.mixed_batch,
            self.expert_reward,
            self.expert_std,
        )

    def test_expert_policy_expert_data(self):
        print("Test expert policy on expert dataset")
        self.check_estimate(
            self.expert_policy,
            self.expert_batch,
            self.expert_reward,
            self.expert_std,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
