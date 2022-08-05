import copy
import os
import unittest
from pathlib import Path
from typing import Type, Union, Dict, Tuple

import numpy as np
from ray.data import read_json
from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.examples.env.cliff_walking_wall_env import CliffWalkingWallEnv
from ray.rllib.examples.policy.cliff_walking_wall_policy import CliffWalkingWallPolicy
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.offline.dataset_reader import DatasetReader
from ray.rllib.offline.estimators import (
    DirectMethod,
    DoublyRobust,
    ImportanceSampling,
    WeightedImportanceSampling,
)
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check

import ray

torch, _ = try_import_torch()


class TestOPE(unittest.TestCase):
    """Compilation tests for using OPE both standalone and in an RLlib Algorithm"""

    @classmethod
    def setUpClass(cls):
        ray.init()
        rllib_dir = Path(__file__).parent.parent.parent.parent
        train_data = os.path.join(rllib_dir, "tests/data/cartpole/small.json")

        env_name = "CartPole-v0"
        cls.gamma = 0.99
        n_episodes = 3
        cls.q_model_config = {"n_iters": 160}

        config = (
            DQNConfig()
            .environment(env=env_name)
            .rollouts(batch_mode="complete_episodes")
            .framework("torch")
            .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", 0)))
            .offline_data(
                input_="dataset", input_config={"format": "json", "paths": train_data}
            )
            .evaluation(
                evaluation_interval=1,
                evaluation_duration=n_episodes,
                evaluation_num_workers=1,
                evaluation_duration_unit="episodes",
                off_policy_estimation_methods={
                    "is": {"type": ImportanceSampling},
                    "wis": {"type": WeightedImportanceSampling},
                    "dm_fqe": {"type": DirectMethod},
                    "dr_fqe": {"type": DoublyRobust},
                },
            )
        )
        cls.algo = config.build()

        # Read n_episodes of data, assuming that one line is one episode
        reader = DatasetReader(read_json(train_data))
        batches = [reader.next() for _ in range(n_episodes)]
        cls.batch = concat_samples(batches)
        cls.n_episodes = len(cls.batch.split_by_episode())
        print("Episodes:", cls.n_episodes, "Steps:", cls.batch.count)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ope_standalone(self):
        # Test all OPE methods standalone
        estimator_outputs = {
            "v_behavior",
            "v_behavior_std",
            "v_target",
            "v_target_std",
            "v_gain",
            "v_gain_std",
        }
        estimator = ImportanceSampling(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.assertEqual(estimates.keys(), estimator_outputs)

        estimator = WeightedImportanceSampling(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.assertEqual(estimates.keys(), estimator_outputs)

        estimator = DirectMethod(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_config=self.q_model_config,
        )
        losses = estimator.train(self.batch)
        assert losses, "DM estimator did not return mean loss"
        estimates = estimator.estimate(self.batch)
        self.assertEqual(estimates.keys(), estimator_outputs)

        estimator = DoublyRobust(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_config=self.q_model_config,
        )
        losses = estimator.train(self.batch)
        assert losses, "DM estimator did not return mean loss"
        estimates = estimator.estimate(self.batch)
        self.assertEqual(estimates.keys(), estimator_outputs)

    def test_ope_in_algo(self):
        # Test OPE in DQN, during training as well as by calling evaluate()
        results = self.algo.train()
        ope_results = results["evaluation"]["off_policy_estimator"]
        # Check that key exists AND is not {}
        self.assertEqual(set(ope_results.keys()), {"is", "wis", "dm_fqe", "dr_fqe"})

        # Check algo.evaluate() manually as well
        results = self.algo.evaluate()
        ope_results = results["evaluation"]["off_policy_estimator"]
        self.assertEqual(set(ope_results.keys()), {"is", "wis", "dm_fqe", "dr_fqe"})


class TestFQE(unittest.TestCase):
    """Compilation and learning tests for the Fitted-Q Evaluation model"""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()
        env = CliffWalkingWallEnv()
        cls.policy = CliffWalkingWallPolicy(
            observation_space=env.observation_space,
            action_space=env.action_space,
            config={},
        )
        cls.gamma = 0.99
        # Collect single episode under optimal policy
        obs_batch = []
        new_obs = []
        actions = []
        action_prob = []
        rewards = []
        dones = []
        obs = env.reset()
        done = False
        while not done:
            obs_batch.append(obs)
            act, _, extra = cls.policy.compute_single_action(obs)
            actions.append(act)
            action_prob.append(extra["action_prob"])
            obs, rew, done, _ = env.step(act)
            new_obs.append(obs)
            rewards.append(rew)
            dones.append(done)
        cls.batch = SampleBatch(
            obs=obs_batch,
            actions=actions,
            action_prob=action_prob,
            rewards=rewards,
            dones=dones,
            new_obs=new_obs,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_fqe_compilation_and_stopping(self):
        """Compilation tests for FQETorchModel.

        (1) Check that it does not modify the underlying batch during training
        (2) Check that the stopping criteria from FQE are working correctly
        (3) Check that using fqe._compute_action_probs equals brute force
        iterating over all actions with policy.compute_log_likelihoods
        """
        fqe = FQETorchModel(
            policy=self.policy,
            gamma=self.gamma,
        )
        tmp_batch = copy.deepcopy(self.batch)
        losses = fqe.train(self.batch)

        # Make sure FQETorchModel.train() does not modify the batch
        check(tmp_batch, self.batch)

        # Make sure FQE stopping criteria are respected
        assert len(losses) == fqe.n_iters or losses[-1] < fqe.min_loss_threshold, (
            f"FQE.train() terminated early in {len(losses)} steps with final loss"
            f"{losses[-1]} for n_iters: {fqe.n_iters} and "
            f"min_loss_threshold: {fqe.min_loss_threshold}"
        )

        # Test fqe._compute_action_probs against "brute force" method
        # of computing log_prob for each possible action individually
        # using policy.compute_log_likelihoods
        obs = torch.tensor(self.batch["obs"], device=fqe.device)
        action_probs = fqe._compute_action_probs(obs)
        action_probs = convert_to_numpy(action_probs)

        tmp_probs = []
        for act in range(fqe.policy.action_space.n):
            tmp_actions = np.zeros_like(self.batch["actions"]) + act
            log_probs = self.policy.compute_log_likelihoods(
                actions=tmp_actions,
                obs_batch=self.batch["obs"],
            )
            tmp_probs.append(np.exp(log_probs))
        tmp_probs = np.stack(tmp_probs).T
        check(action_probs, tmp_probs, decimals=3)

    def test_fqe_optimal_convergence(self):
        """Test that FQE converges to the true Q-values for an optimal trajectory

        self.batch is deterministic since it is collected under a CliffWalkingWallPolicy
        with epsilon = 0.0; check that FQE converges to the true Q-values for self.batch
        """

        # If self.batch["rewards"] =
        #   [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10],
        # and gamma = 0.99, the discounted returns i.e. optimal Q-values are as follows:

        q_values = np.zeros(len(self.batch["rewards"]), dtype=float)
        q_values[-1] = self.batch["rewards"][-1]
        for t in range(len(self.batch["rewards"]) - 2, -1, -1):
            q_values[t] = self.batch["rewards"][t] + self.gamma * q_values[t + 1]

        print(q_values)

        q_model_config = {
            "polyak_coef": 1.0,
            "model": {
                "fcnet_hiddens": [],
                "activation": "linear",
            },
            "lr": 0.01,
            "n_iters": 5000,
        }

        fqe = FQETorchModel(
            policy=self.policy,
            gamma=self.gamma,
            **q_model_config,
        )
        losses = fqe.train(self.batch)
        print(losses[-10:])
        estimates = fqe.estimate_v(self.batch)
        print(estimates)
        check(estimates, q_values, decimals=1)


def get_cliff_walking_wall_policy_and_data(
    num_episodes: int,
    gamma: float,
    epsilon: float,
) -> Tuple[Policy, SampleBatch, float, float]:
    """Collect a cliff_walking_wall policy and data with epsilon-greedy exploration.

    Args:
        num_episodes: Minimum number of episodes to collect
        gamma: discount factor
        epsilon: epsilon-greedy exploration value

    Returns:
        A Tuple consisting of:
          - A CliffWalkingWallPolicy with exploration parameter epsilon
          - A SampleBatch of at least `num_episodes` CliffWalkingWall episodes
          collected using epsilon-greedy exploration
          - The mean of the discounted return over the collected episodes
          - The stddev of the discounted return over the collected episodes

    """
    config = (
        AlgorithmConfig()
        .rollouts(batch_mode="complete_episodes")
        .environment(disable_env_checking=True)
        .experimental(_disable_preprocessor_api=True)
    )
    config = config.to_dict()
    config["epsilon"] = epsilon

    env = CliffWalkingWallEnv()
    policy = CliffWalkingWallPolicy(
        env.observation_space, env.action_space, {"epsilon": epsilon}
    )
    workers = WorkerSet(
        env_creator=lambda env_config: CliffWalkingWallEnv(),
        policy_class=CliffWalkingWallPolicy,
        trainer_config=config,
        num_workers=4,
    )
    ep_ret = []
    batches = []
    n_eps = 0
    while n_eps < num_episodes:
        batch = synchronous_parallel_sample(worker_set=workers)
        for episode in batch.split_by_episode():
            ret = 0
            for r in episode[SampleBatch.REWARDS][::-1]:
                ret = r + gamma * ret
            ep_ret.append(ret)
            n_eps += 1
        batches.append(batch)
    workers.stop()
    return policy, concat_samples(batches), np.mean(ep_ret), np.std(ep_ret)


def check_estimate(
    *,
    estimator_cls: Type[Union[DirectMethod, DoublyRobust]],
    gamma: float,
    q_model_config: Dict,
    policy: Policy,
    batch: SampleBatch,
    mean_ret: float,
    std_ret: float,
) -> None:
    """Compute off-policy estimates and compare them to the true discounted return.

    Args:
        estimator_cls: Off-Policy Estimator class to be used
        gamma: discount factor
        q_model_config: Optional config settings for the estimator's Q-model
        policy: The target policy we compute estimates for
        batch: The behavior data we use for off-policy estimation
        mean_ret: The mean discounted episode return over the batch
        std_ret: The standard deviation corresponding to mean_ret

    Raises:
        AssertionError if the estimated mean episode return computed by
        the off-policy estimator does not fall within one standard deviation of
        the values specified above i.e. [mean_ret - std_ret, mean_ret + std_ret]
    """
    estimator = estimator_cls(
        policy=policy,
        gamma=gamma,
        q_model_config=q_model_config,
    )
    loss = estimator.train(batch)["loss"]
    estimates = estimator.estimate(batch)
    est_mean = estimates["v_target"]
    est_std = estimates["v_target_std"]
    print(f"{est_mean:.2f}, {est_std:.2f}, {mean_ret:.2f}, {std_ret:.2f}, {loss:.2f}")
    # Assert that the two mean +- stddev intervals overlap
    assert mean_ret - std_ret <= est_mean <= mean_ret + std_ret, (
        f"DirectMethod estimate {est_mean:.2f} with stddev "
        f"{est_std:.2f} does not converge to true discounted return "
        f"{mean_ret:.2f} with stddev {std_ret:.2f}!"
    )


class TestOPELearning(unittest.TestCase):
    """Learning tests for the DirectMethod and DoublyRobust estimators.

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
                "fcnet_hiddens": [],
                "activation": "linear",
            },
            "lr": 0.01,
        }

        (
            cls.random_policy,
            cls.random_batch,
            cls.random_reward,
            cls.random_std,
        ) = get_cliff_walking_wall_policy_and_data(num_episodes, cls.gamma, random_eps)
        print(
            f"Collected random batch of {cls.random_batch.count} steps "
            f"with return {cls.random_reward} stddev {cls.random_std}"
        )

        (
            cls.mixed_policy,
            cls.mixed_batch,
            cls.mixed_reward,
            cls.mixed_std,
        ) = get_cliff_walking_wall_policy_and_data(num_episodes, cls.gamma, mixed_eps)
        print(
            f"Collected mixed batch of {cls.mixed_batch.count} steps "
            f"with return {cls.mixed_reward} stddev {cls.mixed_std}"
        )

        (
            cls.expert_policy,
            cls.expert_batch,
            cls.expert_reward,
            cls.expert_std,
        ) = get_cliff_walking_wall_policy_and_data(num_episodes, cls.gamma, expert_eps)
        print(
            f"Collected expert batch of {cls.expert_batch.count} steps "
            f"with return {cls.expert_reward} stddev {cls.expert_std}"
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dm_random_policy_random_data(self):
        print("Test DirectMethod on random policy on random dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.random_policy,
            batch=self.random_batch,
            mean_ret=self.random_reward,
            std_ret=self.random_std,
        )

    def test_dm_random_policy_mixed_data(self):
        print("Test DirectMethod on random policy on mixed dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.random_policy,
            batch=self.mixed_batch,
            mean_ret=self.random_reward,
            std_ret=self.random_std,
        )

    def test_dm_mixed_policy_random_data(self):
        print("Test DirectMethod on mixed policy on random dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.mixed_policy,
            batch=self.random_batch,
            mean_ret=self.mixed_reward,
            std_ret=self.mixed_std,
        )

    def test_dm_mixed_policy_mixed_data(self):
        print("Test DirectMethod on mixed policy on mixed dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.mixed_policy,
            batch=self.mixed_batch,
            mean_ret=self.mixed_reward,
            std_ret=self.mixed_std,
        )

    def test_dm_mixed_policy_expert_data(self):
        print("Test DirectMethod on mixed policy on expert dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.mixed_policy,
            batch=self.expert_batch,
            mean_ret=self.mixed_reward,
            std_ret=self.mixed_std,
        )

    def test_dm_expert_policy_random_data(self):
        print("Test DirectMethod on expert policy on random dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.expert_policy,
            batch=self.random_batch,
            mean_ret=self.expert_reward,
            std_ret=self.expert_std,
        )

    def test_dm_expert_policy_mixed_data(self):
        print("Test DirectMethod on expert policy on mixed dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.expert_policy,
            batch=self.mixed_batch,
            mean_ret=self.expert_reward,
            std_ret=self.expert_std,
        )

    def test_dm_expert_policy_expert_data(self):
        print("Test DirectMethod on expert policy on expert dataset")
        check_estimate(
            estimator_cls=DirectMethod,
            gamma=self.gamma,
            q_model_config=self.q_model_config,
            policy=self.expert_policy,
            batch=self.expert_batch,
            mean_ret=self.expert_reward,
            std_ret=self.expert_std,
        )

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
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
