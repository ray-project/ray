import unittest

import ray
from ray.rllib.offline.estimators import DoublyRobust
from ray.rllib.offline.estimators.tests.utils import (
    get_cliff_walking_wall_policy_and_data,
    check_estimate,
)

SEED = 0


class TestDRLearning(unittest.TestCase):
    """Learning tests for the DoublyRobust estimator.

    Generates three GridWorldWallPolicy policies and batches with  epsilon = 0.2, 0.5,
    and 0.8 respectively using `get_cliff_walking_wall_policy_and_data`.

    Tests that the estimators converge on all eight combinations of evaluation policy
    and behavior batch using `check_estimates`, except random policy-expert batch.

    Note: We do not test OPE with the "random" policy (epsilon=0.8)
    and "expert" (epsilon=0.2) batch because of the large policy-data mismatch. The
    expert batch is unlikely to contain the longer trajectories that would be observed
    under the random policy, thus the OPE estimate is flaky and inaccurate.
    """

    @classmethod
    def setUpClass(cls):
        ray.init()
        # Epsilon-greedy exploration values
        random_eps = 0.8
        mixed_eps = 0.5
        expert_eps = 0.2
        num_episodes = 64
        cls.gamma = 0.99

        # Config settings for FQE model
        cls.q_model_config = {
            "n_iters": 800,
            "minibatch_size": 64,
            "polyak_coef": 1.0,
            "model": {
                "fcnet_hiddens": [32, 32, 32],
                "activation": "relu",
            },
            "lr": 1e-3,
        }

        (
            cls.random_policy,
            cls.random_batch,
            cls.random_reward,
            cls.random_std,
        ) = get_cliff_walking_wall_policy_and_data(
            num_episodes, cls.gamma, random_eps, seed=SEED
        )
        print(
            f"Collected random batch of {cls.random_batch.count} steps "
            f"with return {cls.random_reward} stddev {cls.random_std}"
        )

        (
            cls.mixed_policy,
            cls.mixed_batch,
            cls.mixed_reward,
            cls.mixed_std,
        ) = get_cliff_walking_wall_policy_and_data(
            num_episodes, cls.gamma, mixed_eps, seed=SEED
        )
        print(
            f"Collected mixed batch of {cls.mixed_batch.count} steps "
            f"with return {cls.mixed_reward} stddev {cls.mixed_std}"
        )

        (
            cls.expert_policy,
            cls.expert_batch,
            cls.expert_reward,
            cls.expert_std,
        ) = get_cliff_walking_wall_policy_and_data(
            num_episodes, cls.gamma, expert_eps, seed=SEED
        )
        print(
            f"Collected expert batch of {cls.expert_batch.count} steps "
            f"with return {cls.expert_reward} stddev {cls.expert_std}"
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dr_random_policy_random_data(self):
        print("Test DoublyRobust on random policy on random dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.random_policy,
            batch=self.random_batch,
            mean_ret=self.random_reward,
            std_ret=self.random_std,
            seed=SEED,
        )

    def test_dr_random_policy_mixed_data(self):
        print("Test DoublyRobust on random policy on mixed dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.random_policy,
            batch=self.mixed_batch,
            mean_ret=self.random_reward,
            std_ret=self.random_std,
            seed=SEED,
        )

    def test_dr_mixed_policy_random_data(self):
        print("Test DoublyRobust on mixed policy on random dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.mixed_policy,
            batch=self.random_batch,
            mean_ret=self.mixed_reward,
            std_ret=self.mixed_std,
            seed=SEED,
        )

    def test_dr_mixed_policy_mixed_data(self):
        print("Test DoublyRobust on mixed policy on mixed dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.mixed_policy,
            batch=self.mixed_batch,
            mean_ret=self.mixed_reward,
            std_ret=self.mixed_std,
            seed=SEED,
        )

    def test_dr_mixed_policy_expert_data(self):
        print("Test DoublyRobust on mixed policy on expert dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.mixed_policy,
            batch=self.expert_batch,
            mean_ret=self.mixed_reward,
            std_ret=self.mixed_std,
            seed=SEED,
        )

    def test_dr_expert_policy_random_data(self):
        print("Test DoublyRobust on expert policy on random dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.expert_policy,
            batch=self.random_batch,
            mean_ret=self.expert_reward,
            std_ret=self.expert_std,
            seed=SEED,
        )

    def test_dr_expert_policy_mixed_data(self):
        print("Test DoublyRobust on expert policy on mixed dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.expert_policy,
            batch=self.mixed_batch,
            mean_ret=self.expert_reward,
            std_ret=self.expert_std,
            seed=SEED,
        )

    def test_dr_expert_policy_expert_data(self):
        print("Test DoublyRobust on expert policy on expert dataset")
        check_estimate(
            estimator_cls=DoublyRobust,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.expert_policy,
            batch=self.expert_batch,
            mean_ret=self.expert_reward,
            std_ret=self.expert_std,
            seed=SEED,
        )


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
