import gym
import numpy as np
from pettingzoo.butterfly import pistonball_v4
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0
import unittest

import ray
from ray.rllib.agents.pg import pg
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from ray.rllib.examples.env.random_env import RandomEnv, RandomMultiAgentEnv
from ray.rllib.examples.remote_vector_env_with_custom_api import \
    NonVectorizedEnvToBeVectorizedIntoRemoteVectorEnv
from ray import tune


# Function that outputs the environment you wish to register.
def env_creator(config):
    env = pistonball_v4.env(local_ratio=config.get("local_ratio", 0.2))
    env = dtype_v0(env, dtype=np.float32)
    env = color_reduction_v0(env, mode="R")
    env = normalize_obs_v0(env)
    return env


tune.register_env("cartpole", lambda env_ctx: gym.make("CartPole-v0"))

tune.register_env("pistonball",
                  lambda config: PettingZooEnv(env_creator(config)))


class TestRemoteWorkerEnvSetting(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_remote_worker_env(self):
        config = pg.DEFAULT_CONFIG.copy()
        config["remote_worker_envs"] = True
        config["num_envs_per_worker"] = 4

        # Simple string env definition (gym.make(...)).
        config["env"] = "CartPole-v0"
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()

        # Using tune.register.
        config["env"] = "cartpole"
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()

        # Using class directly.
        config["env"] = RandomEnv
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()

        # Using class directly: Sub-class of gym.Env,
        # which implements its own API.
        config["env"] = NonVectorizedEnvToBeVectorizedIntoRemoteVectorEnv
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()

    def test_remote_worker_env_multi_agent(self):
        config = pg.DEFAULT_CONFIG.copy()
        config["remote_worker_envs"] = True
        config["num_envs_per_worker"] = 4

        # Full classpath provided.
        config["env"] = \
            "ray.rllib.examples.env.random_env.RandomMultiAgentEnv"
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()

        # Using tune.register.
        config["env"] = "pistonball"
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()

        # Using class directly.
        config["env"] = RandomMultiAgentEnv
        trainer = pg.PGTrainer(config=config)
        print(trainer.train())
        trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
