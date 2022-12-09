import copy
import gym
import torch
import tree
import numpy as np
import unittest


from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
    get_separate_encoder_config,
    get_shared_encoder_config,
    get_ppo_loss,
)
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check
from ray.rllib.models.catalog import MODEL_DEFAULTS


def to_numpy(tensor):
    return tensor.detach().cpu().numpy()


def to_tensor(array, device=None):
    if device:
        return torch.from_numpy(array).float().to(device)
    return torch.from_numpy(array).float()


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        model_config = copy.deepcopy(MODEL_DEFAULTS)
        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            env = gym.make(env_name)
            obs_dim = env.observation_space.shape[0]
            action_dim = (
                env.action_space.n
                if isinstance(env.action_space, gym.spaces.Discrete)
                else env.action_space.shape[0]
            )

            # without shared encoder
            model_config["vf_share_layers"] = False
            module = PPOTorchRLModule.from_model_config_dict(
                env.observation_space,
                env.action_space,
                model_config=model_config,
            )

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
            model_config["vf_share_layers"] = True
            module = PPOTorchRLModule.from_model_config_dict(
                env.observation_space,
                env.action_space,
                model_config=model_config,
            )

            self.assertIsNotNone(module.encoder)
            self.assertEqual(module.encoder.net.layers[0].in_features, obs_dim)
            self.assertEqual(module.pi.layers[0].in_features, module.encoder.net.output_dim)
            self.assertEqual(module.vf.layers[0].in_features, module.encoder.net.output_dim)
            if isinstance(env.action_space, gym.spaces.Discrete):
                self.assertEqual(module.pi.layers[-1].out_features, action_dim)
            else:
                self.assertEqual(module.pi.layers[-1].out_features, action_dim * 2)
            self.assertEqual(module.vf.layers[-1].out_features, 1)

    def test_get_set_state(self):

        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            env = gym.make(env_name)
            module = PPOTorchRLModule.from_model_config_dict(
                env.observation_space,
                env.action_space,
                model_config=MODEL_DEFAULTS,
            )

            state = module.get_state()
            self.assertIsInstance(state, dict)

            module2 = PPOTorchRLModule.from_model_config_dict(
                env.observation_space,
                env.action_space,
                model_config=MODEL_DEFAULTS,
            )
            state2 = module2.get_state()
            self.assertRaises(AssertionError, lambda: check(state, state2))

            module2.set_state(state)
            state2_after = module2.get_state()
            check(state, state2_after)

    def test_rollouts(self):

        model_config = copy.deepcopy(MODEL_DEFAULTS)
        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            for fwd_fn in ["forward_exploration", "forward_inference"]:
                for shared_encoder in [False, True]:
                    print(
                        f"[ENV={env_name}] | [FWD={fwd_fn}] | [SHARED={shared_encoder}]"
                    )
                    env = gym.make(env_name)

                    model_config["vf_share_layers"] = shared_encoder
                    module = PPOTorchRLModule.from_model_config_dict(
                        env.observation_space,
                        env.action_space,
                        model_config=model_config,
                    )

                    obs = env.reset()
                    tstep = 0
                    while tstep < 10:

                        if fwd_fn == "forward_exploration":
                            fwd_out = module.forward_exploration(
                                {SampleBatch.OBS: to_tensor(obs)[None]}
                            )
                            action = to_numpy(
                                fwd_out[SampleBatch.ACTION_DIST].sample().squeeze(0)
                            )
                        elif fwd_fn == "forward_inference":
                            # check if I sample twice, I get the same action
                            fwd_out = module.forward_inference(
                                {SampleBatch.OBS: to_tensor(obs)[None]}
                            )
                            action = to_numpy(
                                fwd_out[SampleBatch.ACTION_DIST].sample().squeeze(0)
                            )
                            action2 = to_numpy(
                                fwd_out[SampleBatch.ACTION_DIST].sample().squeeze(0)
                            )
                            check(action, action2)

                        obs, reward, done, info = env.step(action)
                        print(
                            f"obs: {obs}, action: {action}, reward: {reward}, "
                            f"done: {done}, info: {info}"
                        )
                        tstep += 1

    def test_forward_train(self):
        model_config = copy.deepcopy(MODEL_DEFAULTS)
        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            for shared_encoder in [False, True]:
                print("-" * 80)
                print(f"[ENV={env_name}] | [SHARED={shared_encoder}]")
                env = gym.make(env_name)

                model_config["vf_share_layers"] = shared_encoder
                module = PPOTorchRLModule.from_model_config_dict(
                    env.observation_space,
                    env.action_space,
                    model_config=model_config,
                )

                # collect a batch of data
                batch = []
                obs = env.reset()
                tstep = 0
                while tstep < 10:
                    fwd_out = module.forward_exploration({"obs": to_tensor(obs)[None]})
                    action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
                    new_obs, reward, done, _ = env.step(action)
                    batch.append(
                        {
                            SampleBatch.OBS: obs,
                            SampleBatch.NEXT_OBS: new_obs,
                            SampleBatch.ACTIONS: action,
                            SampleBatch.REWARDS: np.array(reward),
                            SampleBatch.DONES: np.array(done),
                        }
                    )
                    obs = new_obs
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
