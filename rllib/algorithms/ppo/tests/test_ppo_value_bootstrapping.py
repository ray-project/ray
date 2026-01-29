import unittest

import numpy as np
import torch

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.connectors.learner import (
    AddColumnsFromEpisodesToTrainBatch,
    AddOneTsToEpisodesAndTruncate,
    BatchIndividualItems,
    LearnerConnectorPipeline,
)
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary


def simulate_vt_calculation(vfps, rewards, terminateds, truncateds, gamma, lambda_):
    # Formatting
    episodes = []
    for vfp, r, term, trunc in zip(vfps, rewards, terminateds, truncateds):
        episodes.append(
            SingleAgentEpisode(
                observations=[0] * len(vfp),  # Include observation after last action
                actions=[0] * len(r),
                rewards=r,
                terminated=term,
                truncated=trunc,
                len_lookback_buffer=0,
            )
        )
    # Call AddOneTsToEpisodesAndTruncate
    pipe = LearnerConnectorPipeline(
        connectors=[
            AddOneTsToEpisodesAndTruncate(),
            AddColumnsFromEpisodesToTrainBatch(),
            BatchIndividualItems(),
        ]
    )
    batch = pipe(
        episodes=episodes,
        batch={},
        rl_module=None,
        explore=False,
        shared_data={},
    )
    episode_lens = [len(e) for e in episodes]
    # Add the last episode's terminated/truncated flags to `terminateds` and `truncateds`
    vfps = [v for vfpl in vfps for v in vfpl]
    # Compute the value targets
    return compute_value_targets(
        values=vfps,
        rewards=unpad_data_if_necessary(
            episode_lens,
            np.array(batch[Columns.REWARDS]),
        ),
        terminateds=unpad_data_if_necessary(
            episode_lens,
            np.array(batch[Columns.TERMINATEDS]),
        ),
        truncateds=unpad_data_if_necessary(
            episode_lens,
            np.array(batch[Columns.TRUNCATEDS]),
        ),
        gamma=gamma,
        lambda_=lambda_,
    )


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_value_computation(self):
        correct = [0.9405, 1.0, None, 0.9405, 1.0, None]
        two_term = simulate_vt_calculation(
            [[0.0, 0.95, 0.95], [0.0, 0.95, 0.95]],  # Value head outputs
            [[0.0, 1.0], [0.0, 1.0]],  # Environment rewards
            [True, True],  # Terminated flags
            [False, False],  # Truncated flags
            gamma=0.99,
            lambda_=0.0,
        )
        for pred, gt in zip(two_term, correct):
            if gt is not None:
                self.assertAlmostEqual(pred, gt)
        # Test case where an episode is truncated (state value should be included)
        correct = [0.9405, 1.0, None, 0.9405, 1.9405, None]
        term_trunc = simulate_vt_calculation(
            [[0.0, 0.95, 0.95], [0.0, 0.95, 0.95]],  # Value head outputs
            [[0.0, 1.0], [0.0, 1.0]],  # Environment rewards
            [True, False],  # Terminated flags
            [False, True],  # Truncated flags
            gamma=0.99,
            lambda_=0.0,
        )
        for pred, gt in zip(term_trunc, correct):
            if gt is not None:
                self.assertAlmostEqual(pred, gt)

    def test_ppo_value_bootstrapping(self):
        """Test whether PPO's value bootstrapping works properly."""

        # Build a PPOConfig object with the `SingleAgentEnvRunner` class.
        config = (
            ppo.PPOConfig()
            .debugging(seed=0)
            .environment(  # A very simple environment with a terminal reward
                "FrozenLake-v1",
                env_config={
                    "desc": [
                        "HG",
                        "FF",
                        "SH",
                        "FH",
                    ],
                    "is_slippery": False,
                    "max_episode_steps": 3,
                },
            )
            .env_runners(
                num_env_runners=0,
                # Flatten discrete observations (into one-hot vectors).
                env_to_module_connector=lambda env, spaces, device: FlattenObservations(),
            )
            .training(
                num_epochs=10,
                lr=2e-4,
                lambda_=0.0,  # Zero means pure value bootstrapping
                gamma=0.9,
                train_batch_size=256,
            )
        )

        num_iterations = 20

        algo = config.build()

        for i in range(num_iterations):
            r_mean = algo.train()[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
            print(r_mean)

        # Test value predictions
        critic = algo.learner_group._learner._module[DEFAULT_POLICY_ID]
        state_values = {}

        for state in [3, 2, 4, 6]:
            obs = torch.zeros((8,)).float()
            obs[state] += 1
            batch = {Columns.OBS: obs.unsqueeze(0)}
            with torch.no_grad():
                value = critic.compute_values(batch).item()
            print(f"State {state}: {value:.02f}")
            state_values[state] = value

        algo.stop()
        # Value bootstrapping should learn this simple environment reliably
        self.assertGreater(r_mean, 0.9)
        # The value function
        self.assertGreater(state_values[3], 0.9)  # Immediately terminates with reward 1
        self.assertGreater(
            state_values[2], 0.8
        )  # One step from terminating with reward 1
        self.assertGreater(
            state_values[4], 0.7
        )  # Two steps from terminating with reward 1
        self.assertLess(state_values[6], 0.7)  # Cannot reach the goal from this state


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
