import copy
import os
import unittest
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

import gymnasium as gym
import numpy as np
import pandas as pd

import ray
from ray.data import read_json
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.examples._old_api_stack.policy.cliff_walking_wall_policy import (
    CliffWalkingWallPolicy,
)
from ray.rllib.examples.envs.classes.cliff_walking_wall_env import CliffWalkingWallEnv
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

if TYPE_CHECKING:
    from ray.rllib.policy import Policy


torch, _ = try_import_torch()

ESTIMATOR_OUTPUTS = {
    "v_behavior",
    "v_behavior_std",
    "v_target",
    "v_target_std",
    "v_gain",
    "v_delta",
}


def compute_expected_is_or_wis_estimator(
    df: pd.DataFrame, policy: "Policy", num_actions: int, is_wis: bool = False
) -> Tuple[float, float]:
    """Computes the expected IS or WIS estimator for the given policy and data.

    The policy is assumed to be deterministic over some discrete action space. i.e. the
    output of a policy has probablity 1.0 over the action it chooses.

    Args:
        df: The data to compute the estimator for.
        policy: The policy to compute the estimator for.
        num_actions: The number of actions in the action space.
        is_wis: Whether to compute the IS or WIS estimator.

    Returns:
        A tuple of the estimator value and the standard error of the estimator.
    """
    sample_batch = {SampleBatch.OBS: np.vstack(df[SampleBatch.OBS].values)}

    actions, _, extra_outs = policy.compute_actions_from_input_dict(
        sample_batch, explore=False
    )

    logged_actions = df[SampleBatch.ACTIONS].astype(int)
    ips_gain = (
        num_actions
        * sum(df[SampleBatch.REWARDS] * (1.0 * (actions == logged_actions).values))
        / df[SampleBatch.REWARDS].sum()
    )
    avg_ips_weight = (
        num_actions * sum((1.0 * (actions == logged_actions).values)) / len(actions)
    )

    if is_wis:
        gain = float(ips_gain / avg_ips_weight)
    else:
        gain = float(ips_gain)

    ips_gain_vec = (
        num_actions
        * df[SampleBatch.REWARDS]
        * (1.0 * (actions == logged_actions)).values
        / df[SampleBatch.REWARDS].mean()
    )

    if is_wis:
        se = float(
            np.std(ips_gain_vec / avg_ips_weight)
            / np.sqrt(len(ips_gain_vec / avg_ips_weight))
        )
    else:
        se = float(np.std(ips_gain_vec) / np.sqrt(len(ips_gain_vec)))

    return gain, se


class TestOPE(unittest.TestCase):
    """Compilation tests for using OPE both standalone and in an RLlib Algorithm"""

    @classmethod
    def setUpClass(cls):
        ray.init()
        seed = 42
        np.random.seed(seed)

        rllib_dir = Path(__file__).parent.parent.parent.parent
        train_data = os.path.join(rllib_dir, "tests/data/cartpole/small.json")

        env_name = "CartPole-v1"
        cls.gamma = 0.99
        n_episodes = 3
        cls.q_model_config = {"n_iters": 160}

        cls.config_dqn_on_cartpole = (
            DQNConfig()
            .environment(env=env_name)
            .framework("torch")
            .env_runners(batch_mode="complete_episodes")
            .offline_data(
                input_="dataset",
                input_config={"format": "json", "paths": train_data},
            )
            .evaluation(
                evaluation_interval=1,
                evaluation_duration=n_episodes,
                evaluation_num_env_runners=1,
                evaluation_duration_unit="episodes",
                off_policy_estimation_methods={
                    "is": {"type": ImportanceSampling, "epsilon_greedy": 0.1},
                    "wis": {"type": WeightedImportanceSampling, "epsilon_greedy": 0.1},
                    "dm_fqe": {"type": DirectMethod, "epsilon_greedy": 0.1},
                    "dr_fqe": {"type": DoublyRobust, "epsilon_greedy": 0.1},
                },
            )
            .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", 0)))
        )

        num_env_runners = 4
        dsize = num_env_runners * 1024
        feature_dim = 64
        action_dim = 8

        data = {
            SampleBatch.OBS: np.random.randn(dsize, 1, feature_dim),
            SampleBatch.ACTIONS: np.random.randint(0, action_dim, dsize).reshape(-1, 1),
            SampleBatch.REWARDS: np.random.rand(dsize).reshape(-1, 1),
            SampleBatch.ACTION_PROB: 1 / action_dim * np.ones((dsize, 1)),
        }
        cls.train_df = pd.DataFrame({k: list(v) for k, v in data.items()})
        cls.train_df["type"] = "SampleBatch"

        train_ds = ray.data.from_pandas(cls.train_df).repartition(num_env_runners)

        cls.dqn_on_fake_ds = (
            DQNConfig()
            .environment(
                observation_space=gym.spaces.Box(-1, 1, (feature_dim,)),
                action_space=gym.spaces.Discrete(action_dim),
            )
            .env_runners(num_env_runners=num_env_runners)
            .framework("torch")
            # .env_runners(num_env_runners=num_env_runners)
            .offline_data(
                input_="dataset",
                input_config={"loader_fn": lambda: train_ds},
            )
            .evaluation(
                evaluation_num_env_runners=num_env_runners,
                ope_split_batch_by_episode=False,
            )
            # make the policy deterministic
            .training(categorical_distribution_temperature=1e-20)
            .debugging(seed=seed)
        )

        # Read n episodes of data, assuming that one line is one episode.
        reader = DatasetReader(read_json(train_data))
        batches = [reader.next() for _ in range(n_episodes)]
        cls.batch = concat_samples(batches)
        cls.n_episodes = len(cls.batch.split_by_episode())
        print("Episodes:", cls.n_episodes, "Steps:", cls.batch.count)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()


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
        terminateds = []
        truncateds = []

        obs, info = env.reset()

        terminated = truncated = False
        while not terminated and not truncated:
            obs_batch.append(obs)
            act, _, extra = cls.policy.compute_single_action(obs)
            actions.append(act)
            action_prob.append(extra["action_prob"])
            obs, rew, terminated, truncated, _ = env.step(act)
            new_obs.append(obs)
            rewards.append(rew)
            terminateds.append(terminated)
            truncateds.append(truncated)

        cls.batch = SampleBatch(
            obs=obs_batch,
            actions=actions,
            action_prob=action_prob,
            rewards=rewards,
            terminateds=terminateds,
            truncateds=truncateds,
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
            "model_config": {
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
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
