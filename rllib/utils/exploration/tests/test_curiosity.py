import gym
import gym_minigrid
import ray
import sys
import unittest

import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.test_utils import framework_iterator
from ray.tune import register_env


def env_maker(config):
    name = config.get("name", "MiniGrid-Empty-5x5-v0")
    env = gym.make(name)
    # Convert discrete inputs (OBJECT_IDX, COLOR_IDX, STATE) to pixels
    # (otherwise, a Conv2D will not be able to learn anything).
    env = gym_minigrid.wrappers.RGBImgPartialObsWrapper(env)
    # Only use image portion of observation (discard goal and direction).
    env = gym_minigrid.wrappers.ImgObsWrapper(env)
    return env


register_env("mini-grid", env_maker)
CONV_FILTERS = [[16, [11, 11], 3], [32, [9, 9], 3], [64, [5, 5], 3]]


class TestCuriosity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_curiosity_on_large_frozen_lake(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # A very large frozen-lake that's hard for a random policy to solve
        # due to 0.0 feedback.
        config["env"] = "FrozenLake-v0"
        config["env_config"] = {
            "desc": [
                "SFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFG",
            ],
            "is_slippery": False
        }
        # Limit horizon to make it really hard for non-curious agent to reach
        # the goal state.
        config["horizon"] = 40
        config["num_workers"] = 0  # local only
        config["train_batch_size"] = 512
        config["num_sgd_iter"] = 10

        num_iterations = 30
        for _ in framework_iterator(config, frameworks="torch"):
            # W/ Curiosity.
            config["exploration_config"] = {
                "type": "Curiosity",
                "feature_dim": 128,
                "eta": 0.05,
                "sub_exploration": {
                    "type": "StochasticSampling",
                }
            }
            trainer = ppo.PPOTrainer(config=config)
            rewards_w = 0.0
            for _ in range(num_iterations):
                result = trainer.train()
                rewards_w += result["episode_reward_mean"]
                print(result)
            rewards_w /= num_iterations
            trainer.stop()

            # W/o Curiosity.
            config["exploration_config"] = {
                "type": "StochasticSampling",
            }
            trainer = ppo.PPOTrainer(config=config)
            rewards_wo = 0.0
            for _ in range(num_iterations):
                result = trainer.train()
                rewards_wo += result["episode_reward_mean"]
                print(result)
            rewards_wo /= num_iterations
            trainer.stop()

            self.assertTrue(rewards_wo == 0.0)
            self.assertGreater(rewards_w, 0.15)

    def test_curiosity_on_4_room_partially_observable_pixels_domain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # config["env_config"] = {"name": "MiniGrid-FourRooms-v0"}
        config["env_config"] = {"name": "MiniGrid-DoorKey-5x5-v0"}
        config["num_envs_per_worker"] = 10
        config["horizon"] = 100  # Make it hard to reach goald just by chance.
        config["model"]["conv_filters"] = CONV_FILTERS
        config["model"]["use_lstm"] = True
        config["model"]["lstm_cell_size"] = 256
        config["model"]["lstm_use_prev_action_reward"] = True

        config["train_batch_size"] = 512
        config["num_sgd_iter"] = 10

        config["exploration_config"] = {
            "type": "Curiosity",
            # For the feature NN, use a non-LSTM conv2d net (same as the one
            # in the policy model.
            "eta": 0.05,
            "feature_net_config": {
                "conv_filters": CONV_FILTERS,
            },
            "sub_exploration": {
                "type": "StochasticSampling",
            }
        }

        num_iterations = 100
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config=config, env="mini-grid")
            for _ in range(num_iterations):
                result = trainer.train()
                print(result)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
