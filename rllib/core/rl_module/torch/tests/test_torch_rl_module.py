import gym
import torch
import unittest
from typing import Mapping

from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

from ray.rllib.utils.test_utils import check


class TestRLModule(unittest.TestCase):
    def test_compilation(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        self.assertIsInstance(module, TorchRLModule)

    def test_forward_train(self):

        bsize = 1024
        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        obs_shape = env.observation_space.shape
        obs = torch.randn((bsize,) + obs_shape)
        actions = torch.stack(
            [torch.tensor(env.action_space.sample()) for _ in range(bsize)]
        )
        output = module.forward_train({"obs": obs})

        self.assertIsInstance(output, Mapping)
        self.assertIn("action_dist", output)
        self.assertIsInstance(output["action_dist"], torch.distributions.Categorical)

        loss = -output["action_dist"].log_prob(actions.view(-1)).mean()
        loss.backward()

        # check that all neural net parameters have gradients
        for param in module.parameters():
            self.assertIsNotNone(param.grad)

    def test_forward(self):
        """Test forward inference and exploration of"""

        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        obs_shape = env.observation_space.shape
        obs = torch.randn((1,) + obs_shape)

        # just test if the forward pass runs fine
        module.forward_inference({"obs": obs})
        module.forward_exploration({"obs": obs})

    def test_get_set_state(self):

        env = gym.make("CartPole-v1")
        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        state = module.get_state()
        self.assertIsInstance(state, dict)

        module2 = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        state2 = module2.get_state()
        check(state, state2, false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    # def test_rollouts(self):
    #
    #     for env_name in ["CartPole-v1", "Pendulum-v1"]:
    #         for fwd_fn in ["forward_exploration", "forward_inference"]:
    #             for shared_encoder in [False, True]:
    #                 print(
    #                     f"[ENV={env_name}] | [FWD={fwd_fn}] | [SHARED={shared_encoder}]"
    #                 )
    #                 env = gym.make(env_name)
    #
    #                 if shared_encoder:
    #                     config = get_shared_encoder_config(env)
    #                 else:
    #                     config = get_separate_encoder_config(env)
    #                 module = PPOTorchRLModule(config)
    #
    #                 obs = env.reset()
    #                 tstep = 0
    #                 while tstep < 10:
    #
    #                     if fwd_fn == "forward_exploration":
    #                         fwd_out = module.forward_exploration(
    #                             {SampleBatch.OBS: to_tensor(obs)[None]}
    #                         )
    #                         action = to_numpy(
    #                             fwd_out[SampleBatch.ACTION_DIST].sample().squeeze(0)
    #                         )
    #                     elif fwd_fn == "forward_inference":
    #                         # check if I sample twice, I get the same action
    #                         fwd_out = module.forward_inference(
    #                             {SampleBatch.OBS: to_tensor(obs)[None]}
    #                         )
    #                         action = to_numpy(
    #                             fwd_out[SampleBatch.ACTION_DIST].sample().squeeze(0)
    #                         )
    #                         action2 = to_numpy(
    #                             fwd_out[SampleBatch.ACTION_DIST].sample().squeeze(0)
    #                         )
    #                         check(action, action2)
    #
    #                     obs, reward, done, info = env.step(action)
    #                     print(
    #                         f"obs: {obs}, action: {action}, reward: {reward}, "
    #                         f"done: {done}, info: {info}"
    #                     )
    #                     tstep += 1
    #
    # def test_forward_train(self):
    #     for env_name in ["CartPole-v1", "Pendulum-v1"]:
    #         for shared_encoder in [False, True]:
    #             print("-" * 80)
    #             print(f"[ENV={env_name}] | [SHARED={shared_encoder}]")
    #             env = gym.make(env_name)
    #
    #             if shared_encoder:
    #                 config = get_shared_encoder_config(env)
    #             else:
    #                 config = get_separate_encoder_config(env)
    #
    #             module = PPOTorchRLModule(config)
    #
    #             # collect a batch of data
    #             batch = []
    #             obs = env.reset()
    #             tstep = 0
    #             while tstep < 10:
    #                 fwd_out = module.forward_exploration({"obs": to_tensor(obs)[None]})
    #                 action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
    #                 new_obs, reward, done, _ = env.step(action)
    #                 batch.append(
    #                     {
    #                         SampleBatch.OBS: obs,
    #                         SampleBatch.NEXT_OBS: new_obs,
    #                         SampleBatch.ACTIONS: action,
    #                         SampleBatch.REWARDS: np.array(reward),
    #                         SampleBatch.DONES: np.array(done),
    #                     }
    #                 )
    #                 obs = new_obs
    #                 tstep += 1
    #
    #             # convert the list of dicts to dict of lists
    #             batch = tree.map_structure(lambda *x: list(x), *batch)
    #             # convert dict of lists to dict of tensors
    #             fwd_in = {k: to_tensor(np.array(v)) for k, v in batch.items()}
    #
    #             # forward train
    #             # before training make sure it's on the right device and it's on
    #             # trianing mode
    #             module.to("cpu")
    #             module.train()
    #             fwd_out = module.forward_train(fwd_in)
    #             loss = get_ppo_loss(fwd_in, fwd_out)
    #             loss.backward()
    #
    #             # check that all neural net parameters have gradients
    #             for param in module.parameters():
    #                 self.assertIsNotNone(param.grad)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
