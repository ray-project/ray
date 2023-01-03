import unittest
import logging

import gymnasium as gym
import numpy as np
import ray
from ray import air, tune
from ray.rllib.env.base_env import convert_to_base_env
from ray.rllib.env.wrappers.check_nan_wrapper import CheckNaNWrapper

class TestNaNEnv(gym.Env):
    
    def __init__(self, render_mode=None, check_inf=False):
        self.check_inf = check_inf
        self.action_space = gym.spaces.Discrete(10)
        self.observation_space = gym.spaces.Box(-np.inf, np.inf, shape=(10,), dtype=np.float32)
        self.counter = 0
        self.max_episode_steps=1000

    def step(self, action):
        self.counter += 1
        obs = np.random.rand(10)
        reward = np.sum(obs) * action
        if np.random.rand() > 0.0 and self.counter > 20:
            if self.check_inf:
                obs[2] = np.inf 
            else:
                obs[2] = np.nan
            print("*****************************************")
            print("=========================================")
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(f"Now its {obs[2]}.")
        terminated = self.counter == 100                   
        return obs.astype(np.float32), reward, terminated, False, {}
    
    def reset(self, *, seed=None, options=None):

        obs = np.random.rand(10)
        self.counter = 0

        return obs.astype(np.float32), {}

class TestCheckNaNWrapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_check_nan_wrapper(self):
        """Checks, if the Wrapper warns about NaNs."""
        env = TestNaNEnv()
        base_env = convert_to_base_env(env)
        check_nan_env_config = {
            "raise_exception": False,
            "check_inf": True,
            "warn_once": False,
        }
        wrapped_env = CheckNaNWrapper(base_env, check_nan_env_config)

        obs, infos = wrapped_env.try_reset()

        def run(iters):        
            for i in range(iters):            
                action = np.random.randint(0, 10)
                action_to_send = {0: {"agent0": action}}            
                wrapped_env.send_actions(action_to_send)
                (
                    obs, 
                    rewards, 
                    terminateds, 
                    truncateds, 
                    infos, 
                    offpolicy_actions
                ) = wrapped_env.poll()
        
        self.assertLogs("check_nan_wrapper", level="WARNING")

    def test_check_nan_wrapper_inf(self):
        """Checks if the Wrapper als warns about Inf values."""
        env = TestNaNEnv(check_inf=True)
        base_env = convert_to_base_env(env)
        check_nan_env_config = {
            "raise_exception": False,
            "check_inf": True,
            "warn_once": False,
        }
        wrapped_env = CheckNaNWrapper(base_env, check_nan_env_config)

        obs, infos = wrapped_env.try_reset()

        def run(iters):        
            for i in range(iters):            
                action = np.random.randint(0, 10)
                action_to_send = {0: {"agent0": action}}            
                wrapped_env.send_actions(action_to_send)
                (
                    obs, 
                    rewards, 
                    terminateds, 
                    truncateds, 
                    infos, 
                    offpolicy_actions
                ) = wrapped_env.poll()
        
        self.assertLogs("check_nan_wrapper", level="WARNING")

    def test_check_nan_wrapper_exception(self):
        """Checks, if the Wrapper raises an exception."""
        env = TestNaNEnv()
        base_env = convert_to_base_env(env)
        check_nan_env_config = {
            "raise_exception": True,
            "check_inf": True,
            "warn_once": False,
        }
        wrapped_env = CheckNaNWrapper(base_env, check_nan_env_config)

        obs, infos = wrapped_env.try_reset()

        def run(iters):        
            for i in range(iters):            
                action = np.random.randint(0, 10)
                action_to_send = {0: {"agent0": action}}            
                wrapped_env.send_actions(action_to_send)
                (
                    obs, 
                    rewards, 
                    terminateds, 
                    truncateds, 
                    infos, 
                    offpolicy_actions
                ) = wrapped_env.poll()

        self.assertRaises(ValueError, run, iters=100)
    
    def test_check_nan_wrapper_with_tune(self):
        """Regression test.
        
        Checks if the Wrapper runs smoothly with RLlib algorithms.
        """
        tune.Tuner(
            "PPO",
            run_config=air.RunConfig(stop={"num_env_steps_sampled": 200},),
            param_space={
                "env": TestNaNEnv,
                "check_nan_env": True,
                "check_nan_env_config": {
                    "raise_exception": False,
                    "warn_once": False,
                    "check_inf": True,
                },
                "disable_env_checking": True,
                "num_gpus": 0,
                "num_workers": 1,
                "lr": 0.01,
                "framework": "tf2",
            },
        ).fit()

        self.assertLogs("check_nan_env", level="WARNING")

if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))