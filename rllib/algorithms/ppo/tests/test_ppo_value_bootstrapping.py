import unittest

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.core.columns import Columns
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN
import torch

class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ppo_value_bootstrapping(self):
        """Test whether PPO's value bootstrapping works properly."""

        # Build a PPOConfig object with the `SingleAgentEnvRunner` class.
        config = (
            ppo.PPOConfig()
            .debugging(seed=1)
            .environment( # A very simple environment with a terminal reward
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
                lambda_=0., # Zero means pure value bootstrapping
                gamma=0.9,
                train_batch_size=128,
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
 
        for state in [3,2,4,6]:
            obs = torch.zeros((8,)).float()
            obs[state]+=1
            batch = {Columns.OBS: obs.unsqueeze(0)}
            with torch.no_grad():
              value = critic.compute_values(batch).item()
            print(f'State {state}: {value:.02f}')
            state_values[state] = value

        algo.stop()
        # Value bootstrapping should learn this simple environment reliably
        self.assertGreater(r_mean, 0.9)
        # The value function
        self.assertGreater(state_values[3], 0.9) # Immediately terminates with reward 1
        self.assertGreater(state_values[2], 0.8) # One step from terminating with reward 1
        self.assertGreater(state_values[4], 0.7) # Two steps from terminating with reward 1
        self.assertLess(state_values[6], 0.7) # Cannot reach the goal from this state

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
