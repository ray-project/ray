import unittest
import gym
import torch
import tree
import numpy as np


from ray.rllib.core.examples.simple_ppo_rl_module import (
    SimplePPOModule,
    get_separate_encoder_config,
    get_shared_encoder_config,
    get_ppo_loss,
)
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.test_utils import check


def to_numpy(tensor):
    return tensor.detach().cpu().numpy()


def to_tensor(array, device=None):
    if device:
        return torch.from_numpy(array).float().to(device)
    return torch.from_numpy(array).float()


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            env = gym.make(env_name)
            obs_dim = env.observation_space.shape[0]
            action_dim = (
                env.action_space.n
                if isinstance(env.action_space, gym.spaces.Discrete)
                else env.action_space.shape[0]
            )

            config_separate_encoder = get_separate_encoder_config(env)
            module = SimplePPOModule(config_separate_encoder)

            self.assertIsInstance(module, TorchRLModule)
            self.assertIsNone(module.encoder)
            self.assertEqual(module.pi.layers[0].in_features, obs_dim)
            self.assertEqual(module.vf.layers[0].in_features, obs_dim)
            if isinstance(env.action_space, gym.spaces.Discrete):
                self.assertEqual(module.pi.layers[-1].out_features, action_dim)
            else:
                self.assertEqual(module.pi.layers[-1].out_features, action_dim * 2)
            self.assertEqual(module.vf.layers[-1].out_features, 1)

            # with shared encoder
            config_shared_encoder = get_shared_encoder_config(env)
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

    def test_get_set_state(self):

        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            env = gym.make(env_name)
            config = get_shared_encoder_config(env)
            module = SimplePPOModule(config)

            state = module.get_state()
            self.assertIsInstance(state, dict)

            module2 = SimplePPOModule(config)
            state2 = module2.get_state()
            self.assertRaises(AssertionError, lambda: check(state, state2))

            module2.set_state(state)
            state2_after = module2.get_state()
            check(state, state2_after)

    def test_rollouts(self):

        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            for fwd_fn in ["forward_exploration", "forward_inference"]:
                for shared_encoder in [False, True]:
                    print(
                        f"[ENV={env_name}] | [FWD={fwd_fn}] | [SHARED={shared_encoder}]"
                    )
                    env = gym.make(env_name)

                    if shared_encoder:
                        config = get_shared_encoder_config(env)
                    else:
                        config = get_separate_encoder_config(env)
                    module = SimplePPOModule(config)

                    obs = env.reset()
                    tstep = 0
                    while tstep < 10:

                        if fwd_fn == "forward_exploration":
                            fwd_out = module.forward_exploration(
                                {"obs": to_tensor(obs)[None]}
                            )
                            action = to_numpy(
                                fwd_out["action_dist"].sample().squeeze(0)
                            )
                        elif fwd_fn == "forward_inference":
                            # check if I sample twice, I get the same action
                            fwd_out = module.forward_inference(
                                {"obs": to_tensor(obs)[None]}
                            )
                            action = to_numpy(
                                fwd_out["action_dist"].sample().squeeze(0)
                            )
                            action2 = to_numpy(
                                fwd_out["action_dist"].sample().squeeze(0)
                            )
                            check(action, action2)

                        obs, reward, done, info = env.step(action)
                        print(
                            f"obs: {obs}, action: {action}, reward: {reward}, "
                            f"done: {done}, info: {info}"
                        )
                        tstep += 1

    def test_forward_train(self):
        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            for shared_encoder in [False, True]:
                print("-" * 80)
                print(f"[ENV={env_name}] | [SHARED={shared_encoder}]")
                env = gym.make(env_name)

                if shared_encoder:
                    config = get_shared_encoder_config(env)
                else:
                    config = get_separate_encoder_config(env)

                module = SimplePPOModule(config)

                # collect a batch of data
                batch = []
                obs = env.reset()
                tstep = 0
                while tstep < 10:
                    fwd_out = module.forward_exploration({"obs": to_tensor(obs)[None]})
                    action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                    obs, reward, done, _ = env.step(action)
                    batch.append(
                        {
                            "obs": obs,
                            "action": action[None] if action.ndim == 0 else action,
                            "reward": np.array(reward),
                            "done": np.array(done),
                        }
                    )
                    tstep += 1

                # convert the list of dicts to dict of lists
                batch = tree.map_structure(lambda *x: list(x), *batch)
                # convert dict of lists to dict of tensors
                fwd_in = {k: to_tensor(np.array(v)) for k, v in batch.items()}

                # forward train
                # before training make sure it's on the right device and it's on
                # trianing mode
                module.to("cpu")
                module.train()
                fwd_out = module.forward_train(fwd_in)
                loss = get_ppo_loss(fwd_in, fwd_out)
                loss.backward()

                # check that all neural net parameters have gradients
                for param in module.parameters():
                    self.assertIsNotNone(param.grad)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
