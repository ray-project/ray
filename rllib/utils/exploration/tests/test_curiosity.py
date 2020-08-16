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
    #env = gym_minigrid.wrappers.FlatObsWrapper(env)
    env = gym_minigrid.wrappers.RGBImgPartialObsWrapper(env)
    # Only use image portion of observation (discard goal and direction).
    env = gym_minigrid.wrappers.ImgObsWrapper(env)
    return env


register_env("4-room", env_maker)
CONV_FILTERS = [[16, [11, 11], 3], [32, [9, 9], 3], [64, [5, 5], 3]]


class TestCuriosity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_curiosity_on_4_room_domain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # 4-room env as frozen-lake (with holes instead of walls).
        config["env"] = "FrozenLake-v0"
        config["env_config"] = {"desc": [
            "FFFFFFFFHFFFFFFFF",
            "FSFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFFFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "HHHHFHHHHHHHHFHHH",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFGFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFFFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
            "FFFFFFFFHFFFFFFFF",
        ]}
        config["num_workers"] = 0  # local only

        num_iterations = 10
        for _ in framework_iterator(config, frameworks="torch"):
            # W/ Curiosity.
            config["exploration_config"] = {
                "type": "Curiosity",
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
            trainer.stop()

            self.assertGreater(rewards_w, rewards_wo)

    def test_curiosity_on_4_room_partially_observable_pixels_domain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # config["env_config"] = {"name": "MiniGrid-FourRooms-v0"}
        config["num_workers"] = 0  # local only
        config["model"]["conv_filters"] = CONV_FILTERS
        config["model"]["use_lstm"] = True
        config["model"]["lstm_cell_size"] = 256
        config["model"]["lstm_use_prev_action_reward"] = True
        config["exploration_config"] = {
            "type": "Curiosity",
            # For the feature NN, use a non-LSTM conv2d net (same as the one
            # in the policy model.
            "feature_net_config": {
                "conv_filters": CONV_FILTERS,
            },
            "sub_exploration": {
                "type": "StochasticSampling",
            }
        }

        num_iterations = 1
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config=config, env="4-room")
            for _ in range(num_iterations):
                result = trainer.train()
                print(result)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
