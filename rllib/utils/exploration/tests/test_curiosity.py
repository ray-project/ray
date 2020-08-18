import gym
import gym_minigrid
import numpy as np
import ray
import sys
import unittest

import ray.rllib.agents.ppo as ppo
from ray.rllib.utils.test_utils import framework_iterator
from ray.rllib.utils.numpy import one_hot
from ray.tune import register_env


class OneHotWrapper(gym.core.ObservationWrapper):
    def __init__(self, env):
        super().__init__(env)
        self.observation_space = gym.spaces.Box(
            # 11=objects; 6=colors; 3=states
            0.0, 1.0, shape=(49 * (11 + 6 + 3), ), dtype=np.float32)
        self.init_x = None
        self.init_y = None
        self.x_positions = []
        self.y_positions = []

    def observation(self, obs):
        # Debug output: max-x/y positions to watch exploration progress.
        if self.step_count == 0:
            if self.x_positions:
                #max_x_diff = max(abs(np.array(self.x_positions) - self.init_x))
                #max_y_diff = max(abs(np.array(self.y_positions) - self.init_y))
                max_diff = max(np.sqrt((np.array(self.x_positions) - self.init_x) ** 2 + (
                            np.array(self.y_positions) - self.init_y) ** 2))
                print("After reset: max delta-x/y={}".format(max_diff))
                self.x_positions = []
                self.y_positions = []
            self.init_x = self.agent_pos[0]
            self.init_y = self.agent_pos[1]

        self.x_positions.append(self.agent_pos[0])
        self.y_positions.append(self.agent_pos[1])

        # One-hot the last dim into 11, 6, 3 one-hot vectors, then flatten.
        objects = one_hot(obs[:, :, 0], depth=11)
        colors = one_hot(obs[:, :, 1], depth=6)
        states = one_hot(obs[:, :, 2], depth=3)
        all_ = np.concatenate([objects, colors, states], -1)
        ret = np.reshape(all_, (-1, ))
        return ret


def env_maker(config):
    name = config.get("name", "MiniGrid-Empty-5x5-v0")
    env = gym.make(name)
    # Convert discrete inputs (OBJECT_IDX, COLOR_IDX, STATE) to pixels
    # (otherwise, a Conv2D will not be able to learn anything).
    #env = gym_minigrid.wrappers.RGBImgPartialObsWrapper(env)
    # Only use image portion of observation (discard goal and direction).
    env = gym_minigrid.wrappers.ImgObsWrapper(env)
    env = OneHotWrapper(env)
    return env


register_env("mini-grid", env_maker)
CONV_FILTERS = [[16, [11, 11], 3], [32, [9, 9], 3], [64, [5, 5], 3]]


class TestCuriosity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        #TODO
        ray.init(local_mode=True)

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
            self.assertGreater(rewards_w, 0.1)

    def test_curiosity_on_key_lock_partially_observable_domain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # config["env_config"] = {"name": "MiniGrid-FourRooms-v0"}
        config["env_config"] = {"name": "MiniGrid-DoorKey-16x16-v0"}
        config["num_envs_per_worker"] = 10
        config["horizon"] = 100  # Make it hard to reach goal just by chance.
        #config["model"]["conv_filters"] = CONV_FILTERS
        config["model"]["use_lstm"] = True
        config["model"]["lstm_cell_size"] = 256
        config["model"]["lstm_use_prev_action_reward"] = True
        config["model"]["max_seq_len"] = 100

        # config["evaluation_interval"] = 1
        config["train_batch_size"] = 1024
        config["num_sgd_iter"] = 10

        config["num_gpus"] = 1

        config["exploration_config"] = {
            "type": "Curiosity",
            # For the feature NN, use a non-LSTM conv2d net (same as the one
            # in the policy model.
            "eta": 0.05,
            "feature_net_config": {
                "fcnet_hiddens": [256, 256],
                "fcnet_activation": "relu",
                #"conv_filters": CONV_FILTERS,
            },
            "sub_exploration": {
                "type": "StochasticSampling",
            }
        }

        max_num_iterations = 1000
        for _ in framework_iterator(config, frameworks="torch"):
            # Curiosity should be able to solve this.
            trainer = ppo.PPOTrainer(config=config, env="mini-grid")
            num_iterations_to_success = 0
            for num_iterations_to_success in range(max_num_iterations):
                result = trainer.train()
                print(result)
                reward = result["episode_reward_mean"]
                if reward > 0.7:
                    print("Learnt after {} iters!".format(
                        num_iterations_to_success))
                    break
            trainer.stop()

            # Try w/o Curiosity.
            config["exploration_config"] = {
                "type": "StochasticSampling",
            }
            trainer = ppo.PPOTrainer(config=config, env="mini-grid")
            # Give agent w/o curiosity 2x as much time.
            for _ in range(num_iterations_to_success * 2):
                result = trainer.train()
                print(result)
                reward = result["episode_reward_mean"]
                if reward > 0.7:
                    raise ValueError("Not expected to learn w/o curiosity!")
            trainer.stop()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
