import gymnasium as gym
import numpy as np
from pettingzoo.butterfly import pistonball_v6
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0
import unittest

import ray
from ray.rllib.algorithms.pg import pg
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv

# from ray.rllib.examples.env.random_env import RandomEnv


# Function that outputs the environment you wish to register.
def env_creator():
    env = pistonball_v6.env()
    env = dtype_v0(env, dtype=np.float32)
    env = color_reduction_v0(env, mode="R")
    env = normalize_obs_v0(env)
    return env


gym.register("cartpole", lambda: gym.make("CartPole-v1"))

gym.register("pistonball", lambda: PettingZooEnv(env_creator()))


class TestRemoteWorkerEnvSetting(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_remote_worker_env(self):
        config = pg.PGConfig().rollouts(remote_worker_envs=True, num_envs_per_worker=4)

        # Simple string env definition (gym.make(...)).
        config.environment("CartPole-v1")
        algo = config.build()
        print(algo.train())
        algo.stop()

        # Using tune.register.
        config.environment("cartpole")
        algo = config.build()
        print(algo.train())
        algo.stop()

        # Using class directly.
        # This doesn't work anymore as of gym==0.23
        # config.environment(RandomEnv)
        # algo = config.build()
        # print(algo.train())
        # algo.stop()

        # Using class directly: Sub-class of gym.Env,
        # which implements its own API.
        # This doesn't work anymore as of gym==0.23
        # config.environment(NonVectorizedEnvToBeVectorizedIntoRemoteBaseEnv)
        # algo = config.build()
        # print(algo.train())
        # algo.stop()

    def test_remote_worker_env_multi_agent(self):
        config = pg.PGConfig().rollouts(remote_worker_envs=True, num_envs_per_worker=4)

        # Full classpath provided.
        config.environment("ray.rllib.examples.env.random_env.RandomMultiAgentEnv")
        algo = config.build()
        print(algo.train())
        algo.stop()

        # Using tune.register.
        config.environment("pistonball")
        algo = config.build()
        print(algo.train())
        algo.stop()

        # Using class directly.
        # This doesn't work anymore as of gym==0.23.
        # config.environment(RandomMultiAgentEnv)
        # algo = config.build()
        # print(algo.train())
        # algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
