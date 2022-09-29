import copy
import os
import unittest
from pathlib import Path

import numpy as np
from ray.data import read_json
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.examples.env.cliff_walking_wall_env import CliffWalkingWallEnv
from ray.rllib.examples.policy.cliff_walking_wall_policy import CliffWalkingWallPolicy
from ray.rllib.offline.dataset_reader import DatasetReader
from ray.rllib.offline.estimators import (
    DirectMethod,
    DoublyRobust,
    ImportanceSampling,
    WeightedImportanceSampling,
)
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check

import ray

torch, _ = try_import_torch()

ESTIMATOR_OUTPUTS = {
    "v_behavior",
    "v_behavior_std",
    "v_target",
    "v_target_std",
    "v_gain",
    "v_delta",
}


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
                    "is": {"type": ImportanceSampling, "epsilon_greedy": 0.1},
                    "wis": {"type": WeightedImportanceSampling, "epsilon_greedy": 0.1},
                    "dm_fqe": {"type": DirectMethod, "epsilon_greedy": 0.1},
                    "dr_fqe": {"type": DoublyRobust, "epsilon_greedy": 0.1},
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

    def test_is_and_wis_standalone(self):
        ope_classes = [
            ImportanceSampling,
            WeightedImportanceSampling,
        ]

        for class_module in ope_classes:
            estimator = class_module(
                policy=self.algo.get_policy(),
                gamma=self.gamma,
            )
            estimates = estimator.estimate(self.batch)
            self.assertEqual(set(estimates.keys()), ESTIMATOR_OUTPUTS)
            check(estimates["v_gain"], estimates["v_target"] / estimates["v_behavior"])

    def test_dm_and_dr_standalone(self):
        ope_classes = [
            DirectMethod,
            DoublyRobust,
        ]

        for class_module in ope_classes:
            estimator = class_module(
                policy=self.algo.get_policy(),
                gamma=self.gamma,
                q_model_config=self.q_model_config,
            )
            losses = estimator.train(self.batch)
            assert losses, f"{class_module.__name__} estimator did not return mean loss"
            estimates = estimator.estimate(self.batch)
            self.assertEqual(set(estimates.keys()), ESTIMATOR_OUTPUTS)
            check(estimates["v_gain"], estimates["v_target"] / estimates["v_behavior"])

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
