from collections import deque
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
    def __init__(self, env, vector_index, framestack):
        super().__init__(env)
        self.framestack = framestack
        # 49=7x7 field of vision; 11=object types; 6=colors; 3=state types.
        # +4: Direction.
        self.single_frame_dim = 49 * (11 + 6 + 3) + 4
        self.init_x = None
        self.init_y = None
        self.x_positions = []
        self.y_positions = []
        self.x_y_delta_buffer = deque(maxlen=100)
        self.vector_index = vector_index
        self.frame_buffer = deque(maxlen=self.framestack)
        for _ in range(self.framestack):
            self.frame_buffer.append(np.zeros((self.single_frame_dim, )))

        self.observation_space = gym.spaces.Box(
            0.0, 1.0, shape=(self.single_frame_dim * self.framestack, ),
            dtype=np.float32)

    def observation(self, obs):
        # Debug output: max-x/y positions to watch exploration progress.
        if self.step_count == 0:
            for _ in range(self.framestack):
                self.frame_buffer.append(np.zeros((self.single_frame_dim,)))
            if self.vector_index == 1:
                if self.x_positions:
                    max_diff = max(
                       np.sqrt((np.array(self.x_positions) - self.init_x) ** 2 + (
                                np.array(self.y_positions) - self.init_y) ** 2))
                    self.x_y_delta_buffer.append(max_diff)
                    print("100 average delta-x/y={}".format(np.mean(self.x_y_delta_buffer)))
                    self.x_positions = []
                    self.y_positions = []
                self.init_x = self.agent_pos[0]
                self.init_y = self.agent_pos[1]

        # Are we carrying the key?
        # if self.carrying is not None:
        #    print("Carrying KEY!!")

        self.x_positions.append(self.agent_pos[0])
        self.y_positions.append(self.agent_pos[1])

        # One-hot the last dim into 11, 6, 3 one-hot vectors, then flatten.
        objects = one_hot(obs[:, :, 0], depth=11)
        colors = one_hot(obs[:, :, 1], depth=6)
        states = one_hot(obs[:, :, 2], depth=3)
        # Is the door we see open?
        # for x in range(7):
        #    for y in range(7):
        #        if objects[x, y, 4] == 1.0 and states[x, y, 0] == 1.0:
        #            print("Door OPEN!!")

        all_ = np.concatenate([objects, colors, states], -1)
        ret = np.reshape(all_, (-1, ))
        direction = one_hot(np.array(self.agent_dir), depth=4).astype(np.float32)
        single_frame = np.concatenate([ret, direction])
        self.frame_buffer.append(single_frame)
        return np.concatenate(self.frame_buffer)


def env_maker(config):
    name = config.get("name", "MiniGrid-Empty-5x5-v0")
    framestack = config.get("framestack", 4)
    env = gym.make(name)
    # Only use image portion of observation (discard goal and direction).
    env = gym_minigrid.wrappers.ImgObsWrapper(env)
    env = OneHotWrapper(env, config.vector_index, framestack=framestack)
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
            self.assertGreater(rewards_w, 0.1)

    def test_curiosity_on_partially_observable_domain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        config["env_config"] = {
            "name": "MiniGrid-Empty-16x16-v0",
            "framestack": 1,  # seems to work even w/o framestacking
        }
        config["horizon"] = 40  # Make it impossible to reach goal by chance.
        config["num_envs_per_worker"] = 10
        config["model"]["fcnet_hiddens"] = [512]
        config["model"]["use_lstm"] = True
        config["model"]["lstm_cell_size"] = 256
        config["model"]["lstm_use_prev_action_reward"] = True
        config["num_sgd_iter"] = 6

        config["exploration_config"] = {
            "type": "Curiosity",
            # For the feature NN, use a non-LSTM fcnet (same as the one
            # in the policy model).
            "eta": 0.1,
            "lr": 0.0005,  # 0.0003 or 0.0005 seem to work fine as well.
            "feature_net_config": {
                "fcnet_hiddens": [256, 256],
                "fcnet_activation": "relu",
            },
            "sub_exploration": {
                "type": "StochasticSampling",
            }
        }

        reward_threshold = 0.1
        max_num_iterations = 25
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config=config, env="mini-grid")
            num_iterations_to_success = max_num_iterations
            for num_iterations_to_success in range(max_num_iterations):
                result = trainer.train()
                print(result)
                if result["episode_reward_mean"] > reward_threshold:
                    print("Learnt after {} iters!".format(
                        num_iterations_to_success))
                    break
            trainer.stop()
            self.assertTrue(num_iterations_to_success < max_num_iterations - 1)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
