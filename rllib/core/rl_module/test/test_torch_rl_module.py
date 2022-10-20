import unittest
import gym
from ray.rllib.core.rl_module.examples.simple_ppo_rl_module import (
    SimplePPOModule,
    PPOModuleConfig,
    FCConfig,
)

import torch

def to_numpy(tensor):
    return tensor.detach().cpu().numpy()

def to_tensor(array, device=None):
    if device:
        return torch.from_numpy(array).float().to(device)
    return torch.from_numpy(array).float()

class TestRLModule(unittest.TestCase):
    
    @staticmethod
    def _get_shared_encoder_config(env):
        return PPOModuleConfig(
            observation_space=env.observation_space,
            action_space=env.action_space,
            encoder_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            ),
            pi_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            ),
            vf_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            ),
        )
    
    @staticmethod
    def _get_separate_encoder_config(env):
        return PPOModuleConfig(
            observation_space=env.observation_space,
            action_space=env.action_space,
            pi_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            ),
            vf_config=FCConfig(
                hidden_layers=[32],
                activation="ReLU",
            )
        )



    # def test_simple_ppo_module_compilation(self):

    #     for env_name in ["CartPole-v0", "Pendulum-v1"]:
    #         env = gym.make(env_name)
    #         obs_dim = env.observation_space.shape[0]
    #         action_dim = (
    #             env.action_space.n 
    #             if isinstance(env.action_space, gym.spaces.Discrete) 
    #             else env.action_space.shape[0]
    #         )

    #         # 
    #         config_separate_encoder = self._get_separate_encoder_config(env)
    #         module = SimplePPOModule(config_separate_encoder)

    #         self.assertIsNone(module.encoder)
    #         self.assertEqual(module.pi.layers[0].in_features, obs_dim)
    #         self.assertEqual(module.vf.layers[0].in_features, obs_dim)
    #         if isinstance(env.action_space, gym.spaces.Discrete):
    #             self.assertEqual(module.pi.layers[-1].out_features, action_dim)
    #         else:
    #             self.assertEqual(module.pi.layers[-1].out_features, action_dim * 2)
    #         self.assertEqual(module.vf.layers[-1].out_features, 1)

    #         # with shared encoder
    #         config_shared_encoder = self._get_shared_encoder_config(env)
    #         module = SimplePPOModule(config_shared_encoder)

    #         self.assertIsNotNone(module.encoder)
    #         self.assertEqual(module.encoder.layers[0].in_features, obs_dim)
    #         self.assertEqual(module.pi.layers[0].in_features, module.encoder.output_dim)
    #         self.assertEqual(module.vf.layers[0].in_features, module.encoder.output_dim)
    #         if isinstance(env.action_space, gym.spaces.Discrete):
    #             self.assertEqual(module.pi.layers[-1].out_features, action_dim)
    #         else:
    #             self.assertEqual(module.pi.layers[-1].out_features, action_dim * 2)
    #         self.assertEqual(module.vf.layers[-1].out_features, 1)
    
    def test_forward_exploration(self):
        
        for env_name in ["CartPole-v0"]: #, "Pendulum-v1"]:
            env = gym.make(env_name)
            
            config_shared_encoder = self._get_shared_encoder_config(env)
            module = SimplePPOModule(config_shared_encoder)

            obs = env.reset()
            tstep = 0
            while tstep < 10:
                fwd_out = module.forward_exploration({"obs": to_tensor(obs)[None]})
                action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                obs, reward, done, info = env.step(action)
                print(f"obs: {obs}, action: {action}, reward: {reward}, done: {done}, info: {info}")
                tstep += 1
            




if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
