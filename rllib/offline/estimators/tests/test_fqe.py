import unittest
from ray.rllib.offline.estimators.tests.utils import GridWorldEnv, GridWorldPolicy
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.numpy import convert_to_numpy
import torch
import copy
import numpy as np


class TestFQE(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        env = GridWorldEnv()
        cls.policy = GridWorldPolicy(
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

    def test_fqe_compilation_and_stopping(self):
        # Test FQETorchModel for:
        # (1) Check that it does not modify the underlying batch during training
        # (2) Check that the stopping criteria from FQE are working correctly
        # (3) Check that using fqe._compute_action_probs equals brute force
        # iterating over all actions with policy.compute_log_likelihoods
        fqe = FQETorchModel(
            policy=self.policy,
            gamma=self.gamma,
        )
        tmp_batch = copy.deepcopy(self.batch)
        losses = fqe.train(self.batch)

        # Make sure FQETorchModel.train() does not modify the batch
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
            log_probs = self.policy.compute_log_likelihoods(
                actions=tmp_actions,
                obs_batch=self.batch["obs"],
            )
            tmp_probs.append(np.exp(log_probs))
        tmp_probs = np.stack(tmp_probs).T
        check(action_probs, tmp_probs, decimals=3)

    def test_fqe_optimal_convergence(self):
        q_model_config = {
            "tau": 1.0,
            "model": {
                "fcnet_hiddens": [],
                "activation": "linear",
            },
            "lr": 0.01,
            "n_iters": 5000,
            "delta": 1e-3,
        }

        fqe = FQETorchModel(
            policy=self.policy,
            gamma=self.gamma,
            **q_model_config,
        )
        losses = fqe.train(self.batch)
        print(losses[-10:])
        assert losses[-1] < fqe.delta, "FQE loss did not converge!"
        estimates = fqe.estimate_v(self.batch)
        print(estimates)
        q_vals = [
            -2.50,
            -1.51,
            -0.52,
            0.49,
            1.50,
            2.53,
            3.56,
            4.61,
            5.67,
            6.73,
            7.81,
            8.90,
            10.00,
        ]
        check(
            estimates,
            q_vals,
            decimals=1,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
