import ray
import unittest
import numpy as np
import gym
import torch
import tree

from ray.rllib import SampleBatch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
    get_ppo_loss,
    PPOModuleConfig,
)
from ray.rllib.core.rl_module.fc import FCConfig
from ray.rllib.core.rl_module.encoder import (
    FCEncoderConfig,
    LSTMEncoderConfig,
    STATE_IN,
    STATE_OUT,
)
from ray.rllib.models.base_model import BaseModelIOKeys, ModelIOKeyHelper
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


def get_expected_model_config(
    env: gym.Env,
    lstm: bool,
) -> PPOModuleConfig:
    """Get a PPOModuleConfig that we would expect from the catalog otherwise.

    Args:
        env: Environment for which we build the model later
        lstm: If True, build recurrent pi encoder

    Returns:
         A PPOModuleConfig containing the relevant configs to build PPORLModule
    """
    assert len(env.observation_space.shape) == 1, (
        "No multidimensional obs space " "supported."
    )
    obs_dim = env.observation_space.shape[0]

    if lstm:
        shared_encoder_config = LSTMEncoderConfig(
            input_dim=obs_dim,
            hidden_dim=32,
            batch_first=True,
            num_layers=1,
            output_dim=32,
            input_key=SampleBatch.OBS,
            output_key=shared_encoder_kh.create(BaseModelIOKeys.OUT),
        )
    else:
        shared_encoder_config = FCEncoderConfig(
            input_dim=obs_dim,
            hidden_layers=[32],
            activation="ReLU",
            output_dim=32,
        )

    pi_config = FCConfig(
        input_dim=32,
        hidden_layers=[32],
        activation="ReLU",
    )
    vf_config = FCConfig(
        input_dim=32,
        hidden_layers=[32, 1],
        activation="ReLU",
    )

    if isinstance(env.action_space, gym.spaces.Discrete):
        pi_config.output_dim = env.action_space.n
    else:
        pi_config.output_dim = env.action_space.shape[0] * 2

    return PPOModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        shared_encoder_config=shared_encoder_config,
        pi_config=pi_config,
        vf_config=vf_config,
    )


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_rollouts(self):
        # TODO: Add BreakoutNoFrameskip-v4 to cover a 3D obs space
        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            for fwd_fn in ["forward_exploration", "forward_inference"]:
                for lstm in [False, True]:
                    print(f"[ENV={env_name}] | LSTM={lstm}")
                    env = gym.make(env_name)

                    config = get_expected_model_config(env, lstm)
                    module = PPOTorchRLModule(config)

                    obs, _ = env.reset()

                    batch = {
                        SampleBatch.OBS: convert_to_torch_tensor(obs)[None],
                    }

                    if lstm:
                        state_in = module.get_initial_state()
                        state_in = tree.map_structure(
                            lambda x: x[None], convert_to_torch_tensor(state_in)
                        )
                        batch[STATE_IN] = state_in
                        batch[SampleBatch.SEQ_LENS] = torch.Tensor([1])

                    if fwd_fn == "forward_exploration":
                        module.forward_exploration(batch)
                    else:
                        module.forward_inference(batch)

    def test_forward_train(self):
        # TODO: Add BreakoutNoFrameskip-v4 to cover a 3D obs space
        for env_name in ["CartPole-v1", "Pendulum-v1"]:
            for lstm in [False, True]:
                print(f"[ENV={env_name}] | LSTM={lstm}")
                env = gym.make(env_name)

                config = get_expected_model_config(env, lstm)
                module = PPOTorchRLModule(config)

                # collect a batch of data
                batches = []
                obs, _ = env.reset()
                tstep = 0
                if lstm:
                    state_in = module.get_initial_state()
                    state_in = tree.map_structure(
                        lambda x: x[None], convert_to_torch_tensor(state_in)
                    )
                    initial_state = state_in
                while tstep < 10:
                    if lstm:
                        input_batch = {
                            SampleBatch.OBS: convert_to_torch_tensor(obs)[None],
                            STATE_IN: state_in,
                            SampleBatch.SEQ_LENS: np.array([1]),
                        }
                    else:
                        input_batch = {
                            SampleBatch.OBS: convert_to_torch_tensor(obs)[None]
                        }
                    fwd_out = module.forward_exploration(input_batch)
                    action = convert_to_numpy(
                        fwd_out["action_dist"].sample().squeeze(0)
                    )
                    new_obs, reward, terminated, truncated, _ = env.step(action)
                    output_batch = {
                        SampleBatch.OBS: obs,
                        SampleBatch.NEXT_OBS: new_obs,
                        SampleBatch.ACTIONS: action,
                        SampleBatch.REWARDS: np.array(reward),
                        SampleBatch.TERMINATEDS: np.array(terminated),
                        SampleBatch.TRUNCATEDS: np.array(truncated),
                    }
                    if lstm:
                        assert STATE_OUT in fwd_out
                        state_in = fwd_out[STATE_OUT]
                    batches.append(output_batch)
                    obs = new_obs
                    tstep += 1

                # convert the list of dicts to dict of lists
                batch = tree.map_structure(lambda *x: list(x), *batches)
                # convert dict of lists to dict of tensors
                fwd_in = {
                    k: convert_to_torch_tensor(np.array(v)) for k, v in batch.items()
                }
                if lstm:
                    fwd_in[STATE_IN] = initial_state
                    fwd_in[SampleBatch.SEQ_LENS] = torch.Tensor([10])

                # forward train
                # before training make sure module is on the right device and in
                # training mode
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
