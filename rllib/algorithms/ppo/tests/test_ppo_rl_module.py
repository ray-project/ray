import itertools
import ray
import unittest
import numpy as np
import gymnasium as gym
import torch
import tensorflow as tf
import tree

from ray.rllib import SampleBatch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
    PPOModuleConfig,
)
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import (
    PPOTfModule,
    PPOTfModuleConfig,
)
from ray.rllib.core.rl_module.encoder import (
    FCConfig,
    IdentityConfig,
    LSTMConfig,
    STATE_IN,
    STATE_OUT,
)
from ray.rllib.core.rl_module.encoder_tf import (
    FCTfConfig,
    IdentityTfConfig,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


def get_expected_model_config_torch(
    env: gym.Env, lstm: bool, shared_encoder: bool
) -> PPOModuleConfig:
    """Get a PPOModuleConfig that we would expect from the catalog otherwise.

    Args:
        env: Environment for which we build the model later
        lstm: If True, build recurrent pi encoder
        shared_encoder: If True, build a shared encoder for pi and vf, where pi
            encoder and vf encoder will be identity. If False, the shared encoder
            will be identity.

    Returns:
         A PPOModuleConfig containing the relevant configs to build PPORLModule
    """
    assert len(env.observation_space.shape) == 1, (
        "No multidimensional obs space " "supported."
    )
    obs_dim = env.observation_space.shape[0]

    if shared_encoder:
        assert not lstm, "LSTM can only be used in PI"
        shared_encoder_config = FCConfig(
            input_dim=obs_dim,
            hidden_layers=[32],
            activation="ReLU",
            output_dim=32,
        )
        pi_encoder_config = IdentityConfig(output_dim=32)
        vf_encoder_config = IdentityConfig(output_dim=32)
    else:
        shared_encoder_config = IdentityConfig(output_dim=obs_dim)
        if lstm:
            pi_encoder_config = LSTMConfig(
                input_dim=obs_dim,
                hidden_dim=32,
                batch_first=True,
                output_dim=32,
                num_layers=1,
            )
        else:
            pi_encoder_config = FCConfig(
                input_dim=obs_dim,
                output_dim=32,
                hidden_layers=[32],
                activation="ReLU",
            )
        vf_encoder_config = FCConfig(
            input_dim=obs_dim,
            output_dim=32,
            hidden_layers=[32],
            activation="ReLU",
        )

    pi_config = FCConfig()
    vf_config = FCConfig()

    if isinstance(env.action_space, gym.spaces.Discrete):
        pi_config.output_dim = env.action_space.n
    else:
        pi_config.output_dim = env.action_space.shape[0] * 2

    return PPOModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        shared_encoder_config=shared_encoder_config,
        pi_encoder_config=pi_encoder_config,
        vf_encoder_config=vf_encoder_config,
        pi_config=pi_config,
        vf_config=vf_config,
        shared_encoder=shared_encoder,
    )


def get_expected_model_config_tf(
    env: gym.Env, shared_encoder: bool
) -> PPOTfModuleConfig:
    """Get a PPOTfModuleConfig that we would expect from the catalog otherwise.

    Args:
        env: Environment for which we build the model later
        shared_encoder: If True, build a shared encoder for pi and vf, where pi
            encoder and vf encoder will be identity. If False, the shared encoder
            will be identity.

    Returns:
         A PPOTfModuleConfig containing the relevant configs to build PPOTfModule.
    """
    assert len(env.observation_space.shape) == 1, (
        "No multidimensional obs space " "supported."
    )
    obs_dim = env.observation_space.shape[0]

    if shared_encoder:
        shared_encoder_config = FCTfConfig(
            input_dim=obs_dim,
            hidden_layers=[32],
            activation="ReLU",
            output_dim=32,
        )
    else:
        shared_encoder_config = IdentityTfConfig(output_dim=obs_dim)
    pi_config = FCConfig()
    vf_config = FCConfig()
    pi_config.input_dim = vf_config.input_dim = shared_encoder_config.output_dim

    if isinstance(env.action_space, gym.spaces.Discrete):
        pi_config.output_dim = env.action_space.n
    else:
        pi_config.output_dim = env.action_space.shape[0] * 2

    pi_config.hidden_layers = vf_config.hidden_layers = [32]
    pi_config.activation = vf_config.activation = "ReLU"

    return PPOTfModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        shared_encoder_config=shared_encoder_config,
        pi_config=pi_config,
        vf_config=vf_config,
        shared_encoder=shared_encoder,
    )


def dummy_torch_ppo_loss(batch, fwd_out):
    """Dummy PPO loss function for testing purposes.

    Will eventually use the actual PPO loss function implemented in the PPOTfTrainer.

    Args:
        batch: SampleBatch used for training.
        fwd_out: Forward output of the model.

    Returns:
        Loss tensor
    """
    # TODO: we should replace these components later with real ppo components when
    # RLOptimizer and RLModule are integrated together.
    # this is not exactly a ppo loss, just something to show that the
    # forward train works
    adv = batch[SampleBatch.REWARDS] - fwd_out[SampleBatch.VF_PREDS]
    actor_loss = -(fwd_out[SampleBatch.ACTION_LOGP] * adv).mean()
    critic_loss = (adv**2).mean()
    loss = actor_loss + critic_loss

    return loss


def dummy_tf_ppo_loss(batch, fwd_out):
    """Dummy PPO loss function for testing purposes.

    Will eventually use the actual PPO loss function implemented in the PPOTfTrainer.

    Args:
        batch: SampleBatch used for training.
        fwd_out: Forward output of the model.

    Returns:
        Loss tensor
    """
    adv = batch[SampleBatch.REWARDS] - fwd_out[SampleBatch.VF_PREDS]
    action_probs = fwd_out[SampleBatch.ACTION_DIST].logp(batch[SampleBatch.ACTIONS])
    actor_loss = -tf.reduce_mean(action_probs * adv)
    critic_loss = tf.reduce_mean(tf.square(adv))
    return actor_loss + critic_loss


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def get_ppo_module(self, framwework, env, lstm, shared_encoder):
        if framwework == "torch":
            config = get_expected_model_config_torch(env, lstm, shared_encoder)
            module = PPOTorchRLModule(config)
        else:
            config = get_expected_model_config_tf(env, shared_encoder)
            module = PPOTfModule(config)
        return module

    def get_input_batch_from_obs(self, framework, obs):
        if framework == "torch":
            batch = {
                SampleBatch.OBS: convert_to_torch_tensor(obs)[None],
            }
        else:
            batch = {SampleBatch.OBS: np.array([obs])}
        return batch

    def test_rollouts(self):
        # TODO: Add BreakoutNoFrameskip-v4 to cover a 3D obs space
        frameworks = ["torch", "tf2"]
        env_names = ["CartPole-v1", "Pendulum-v1"]
        fwd_fns = ["forward_exploration", "forward_inference"]
        shared_encoders = [False, True]
        ltsms = [False, True]
        config_combinations = [frameworks, env_names, fwd_fns, shared_encoders, ltsms]
        for config in itertools.product(*config_combinations):
            fw, env_name, fwd_fn, shared_encoder, lstm = config
            if lstm and shared_encoder:
                # Not yet implemented
                # TODO (Artur): Implement
                continue
            if lstm and fw == "tf2":
                # LSTM not implemented in TF2 yet
                continue
            print(f"[ENV={env_name}] | [SHARED={shared_encoder}] | LSTM" f"={lstm}")
            env = gym.make(env_name)
            module = self.get_ppo_module(fw, env, lstm, shared_encoder)

            obs, _ = env.reset()

            batch = self.get_input_batch_from_obs(fw, obs)

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
        frameworks = ["torch", "tf2"]
        env_names = ["CartPole-v1", "Pendulum-v1"]
        shared_encoders = [False, True]
        ltsms = [False, True]
        config_combinations = [frameworks, env_names, shared_encoders, ltsms]
        for config in itertools.product(*config_combinations):
            fw, env_name, shared_encoder, lstm = config
            if lstm and shared_encoder:
                # Not yet implemented
                # TODO (Artur): Implement
                continue
            if lstm and fw == "tf2":
                # LSTM not implemented in TF2 yet
                continue
            print(f"[ENV={env_name}] | [SHARED=" f"{shared_encoder}] | LSTM={lstm}")
            env = gym.make(env_name)

            module = self.get_ppo_module(fw, env, lstm, shared_encoder)

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
                    input_batch = self.get_input_batch_from_obs(fw, obs)
                    input_batch[STATE_IN] = state_in
                    input_batch[SampleBatch.SEQ_LENS] = np.array([1])
                else:
                    input_batch = self.get_input_batch_from_obs(fw, obs)
                fwd_out = module.forward_exploration(input_batch)
                action = convert_to_numpy(fwd_out["action_dist"].sample()[0])
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
            batch = tree.map_structure(lambda *x: np.array(x), *batches)
            # convert dict of lists to dict of tensors
            if fw == "torch":
                fwd_in = {
                    k: convert_to_torch_tensor(np.array(v)) for k, v in batch.items()
                }
                if lstm:
                    fwd_in[STATE_IN] = initial_state
                    fwd_in[SampleBatch.SEQ_LENS] = torch.Tensor([10])

                # forward train
                # before training make sure module is on the right device
                # and in training mode
                module.to("cpu")
                module.train()
                fwd_out = module.forward_train(fwd_in)
                loss = dummy_torch_ppo_loss(fwd_in, fwd_out)
                loss.backward()

                # check that all neural net parameters have gradients
                for param in module.parameters():
                    self.assertIsNotNone(param.grad)
            else:
                with tf.GradientTape() as tape:
                    fwd_out = module.forward_train(batch)
                    loss = dummy_tf_ppo_loss(batch, fwd_out)
                grads = tape.gradient(loss, module.trainable_variables)
                for grad in grads:
                    self.assertIsNotNone(grad)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
