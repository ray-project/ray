import unittest
import gym
from ray.rllib.core.examples.simple_ppo_rl_module import (
    SimplePPOModule,
    PPOModuleConfig,
    FCConfig,
)

import torch
from ray.rllib.utils.test_utils import check
import tree
import numpy as np

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
            ),
        )

    def test_simple_ppo_module_compilation(self):

        for env_name in ["CartPole-v0", "Pendulum-v1"]:
            env = gym.make(env_name)
            obs_dim = env.observation_space.shape[0]
            action_dim = (
                env.action_space.n
                if isinstance(env.action_space, gym.spaces.Discrete)
                else env.action_space.shape[0]
            )

            #
            config_separate_encoder = self._get_separate_encoder_config(env)
            module = SimplePPOModule(config_separate_encoder)

            self.assertIsNone(module.encoder)
            self.assertEqual(module.pi.layers[0].in_features, obs_dim)
            self.assertEqual(module.vf.layers[0].in_features, obs_dim)
            if isinstance(env.action_space, gym.spaces.Discrete):
                self.assertEqual(module.pi.layers[-1].out_features, action_dim)
            else:
                self.assertEqual(module.pi.layers[-1].out_features, action_dim * 2)
            self.assertEqual(module.vf.layers[-1].out_features, 1)

            # with shared encoder
            config_shared_encoder = self._get_shared_encoder_config(env)
            module = SimplePPOModule(config_shared_encoder)

            self.assertIsNotNone(module.encoder)
            self.assertEqual(module.encoder.layers[0].in_features, obs_dim)
            self.assertEqual(module.pi.layers[0].in_features, module.encoder.output_dim)
            self.assertEqual(module.vf.layers[0].in_features, module.encoder.output_dim)
            if isinstance(env.action_space, gym.spaces.Discrete):
                self.assertEqual(module.pi.layers[-1].out_features, action_dim)
            else:
                self.assertEqual(module.pi.layers[-1].out_features, action_dim * 2)
            self.assertEqual(module.vf.layers[-1].out_features, 1)

    def test_rollouts_with_rlmodule(self):

        for env_name in ["CartPole-v0", "Pendulum-v1"]:
            for fwd_fn in ["forward_exploration", "forward_inference"]:
                for shared_encoder in [False, True]:
                    print(f"[ENV={env_name}] | [FWD={fwd_fn}] | [SHARED={shared_encoder}]")
                    env = gym.make(env_name)

                    if shared_encoder:
                        config = self._get_shared_encoder_config(env)
                    else:
                        config = self._get_separate_encoder_config(env)
                    module = SimplePPOModule(config)

                    obs = env.reset()
                    tstep = 0
                    while tstep < 10:

                        if fwd_fn == "forward_exploration":
                            fwd_out = module.forward_exploration({"obs": to_tensor(obs)[None]})
                            action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                        elif fwd_fn == "forward_inference":
                            # check if I sample twice, I get the same action
                            fwd_out = module.forward_inference({"obs": to_tensor(obs)[None]})
                            action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                            action2 = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                            check(action, action2)

                        obs, reward, done, info = env.step(action)
                        print(
                            f"obs: {obs}, action: {action}, reward: {reward}, done: {done}, info: {info}"
                        )
                        tstep += 1


    def test_forward_train(self):
        for env_name in ["CartPole-v0", "Pendulum-v1"]:
            for shared_encoder in [False, True]:
                print("-" * 80)
                print(f"[ENV={env_name}] | [SHARED={shared_encoder}]")
                env = gym.make(env_name)

                if shared_encoder:
                    config = self._get_shared_encoder_config(env)
                else:
                    config = self._get_separate_encoder_config(env)
                
                module = SimplePPOModule(config)
                module.to("cpu")
                module.train()

                # collect a batch of data
                batch = []
                obs = env.reset()
                tstep = 0
                while tstep < 10:
                    fwd_out = module.forward_exploration({"obs": to_tensor(obs)[None]})
                    action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                    obs, reward, done, _ = env.step(action)
                    batch.append({
                        "obs": obs,
                        "action": action[None] if action.ndim == 0 else action,
                        "reward": np.array(reward),
                        "done": np.array(done),
                    })
                    tstep += 1
                
                # convert the list of dicts to dict of lists
                batch = tree.map_structure(lambda *x: list(x), *batch)
                # convert dict of lists to dict of tensors
                batch = {k: to_tensor(np.array(v)) for k, v in batch.items()}

                # forward train
                fwd_out = module.forward_train(batch)

                # this is not exactly a ppo loss, just something to show that the 
                # forward train works
                adv = batch["reward"] - fwd_out["vf"]
                actor_loss = -(fwd_out["logp"] * adv).mean()
                critic_loss = (adv ** 2).mean()
                loss = actor_loss + critic_loss

                loss.backward()

                # check that all neural net parameters have gradients
                for param in module.parameters():
                    self.assertIsNotNone(param.grad)





if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
