from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import unittest

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms import ppo
from ray.rllib.examples.envs.classes.random_env import RandomEnv


class TestLSTMs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_lstm_w_prev_action_and_prev_reward(self):
        """Tests LSTM prev-a/r input insertions using complex actions."""
        config = (
            ppo.PPOConfig()
            .environment(
                RandomEnv,
                env_config={
                    "action_space": Dict(
                        {
                            "a": Box(-1.0, 1.0, ()),
                            "b": Box(-1.0, 1.0, (2,)),
                            "c": Tuple(
                                [
                                    Discrete(2),
                                    MultiDiscrete([2, 3]),
                                    Box(-1.0, 1.0, (3,)),
                                ]
                            ),
                        }
                    ),
                },
            )
            .training(
                # Need to set this to True to enable complex (prev.) actions
                # as inputs to the LSTM.
                model={
                    "fcnet_hiddens": [10],
                    "use_lstm": True,
                    "lstm_cell_size": 16,
                    "lstm_use_prev_action": True,
                    "lstm_use_prev_reward": True,
                },
                num_epochs=1,
                train_batch_size=200,
                minibatch_size=50,
            )
            .env_runners(
                rollout_fragment_length=100,
                num_env_runners=1,
            )
            .experimental(
                _disable_action_flattening=True,
            )
        )

        tune.Tuner(
            "PPO",
            param_space=config.to_dict(),
            run_config=air.RunConfig(stop={TRAINING_ITERATION: 1}, verbose=1),
        ).fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
