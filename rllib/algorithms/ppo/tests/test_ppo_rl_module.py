import itertools
import unittest

import gymnasium as gym
import numpy as np
import tensorflow as tf
import torch
import tree

import ray
from ray.rllib import SampleBatch
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import (
    PPOTfRLModule,
)
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
)
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


def get_expected_module_config(
    env: gym.Env,
    model_config_dict: dict,
    observation_space: gym.spaces.Space,
) -> RLModuleConfig:
    """Get a PPOModuleConfig that we would expect from the catalog otherwise.

    Args:
        env: Environment for which we build the model later
        model_config_dict: Model config to use for the catalog
        observation_space: Observation space to use for the catalog.

    Returns:
         A PPOModuleConfig containing the relevant configs to build PPORLModule
    """
    config = RLModuleConfig(
        observation_space=observation_space,
        action_space=env.action_space,
        model_config_dict=model_config_dict,
        catalog_class=PPOCatalog,
    )

    return config


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


def _get_ppo_module(framework, env, lstm, observation_space):
    model_config_dict = {"use_lstm": lstm}
    config = get_expected_module_config(
        env, model_config_dict=model_config_dict, observation_space=observation_space
    )
    if framework == "torch":
        module = PPOTorchRLModule(config)
    else:
        module = PPOTfRLModule(config)
    return module


def _get_input_batch_from_obs(framework, obs):
    if framework == "torch":
        batch = {
            SampleBatch.OBS: convert_to_torch_tensor(obs)[None],
        }
    else:
        batch = {SampleBatch.OBS: tf.convert_to_tensor([obs])}
    return batch


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_rollouts(self):
        # TODO: Add FrozenLake-v1 to cover LSTM case.
        frameworks = ["torch", "tf2"]
        env_names = ["CartPole-v1", "Pendulum-v1", "ALE/Breakout-v5"]
        fwd_fns = ["forward_exploration", "forward_inference"]
        # TODO(Artur): Re-enable LSTM
        lstm = [False]
        config_combinations = [frameworks, env_names, fwd_fns, lstm]
        for config in itertools.product(*config_combinations):
            fw, env_name, fwd_fn, lstm = config
            if lstm and fw == "tf2":
                # LSTM not implemented in TF2 yet
                continue
            if env_name == "ALE/Breakout-v5" and fw == "tf2":
                # TODO(Artur): Implement CNN in TF2.
                continue
            print(f"[FW={fw} | [ENV={env_name}] | [FWD={fwd_fn}] | LSTM" f"={lstm}")
            if env_name.startswith("ALE/"):
                env = gym.make("GymV26Environment-v0", env_id=env_name)
            else:
                env = gym.make(env_name)

            preprocessor_cls = get_preprocessor(env.observation_space)
            preprocessor = preprocessor_cls(env.observation_space)

            module = _get_ppo_module(
                framework=fw,
                env=env,
                lstm=lstm,
                observation_space=preprocessor.observation_space,
            )
            obs, _ = env.reset()
            obs = preprocessor.transform(obs)

            batch = _get_input_batch_from_obs(fw, obs)

            # TODO (Artur): Un-uncomment once Policy supports RNN
            # state_in = module.get_initial_state()
            # state_in = tree.map_structure(
            #     lambda x: x[None], convert_to_torch_tensor(state_in)
            # )
            # batch[STATE_IN] = state_in
            batch[SampleBatch.SEQ_LENS] = torch.Tensor([1])

            if fwd_fn == "forward_exploration":
                module.forward_exploration(batch)
            else:
                module.forward_inference(batch)

    def test_forward_train(self):
        # TODO: Add FrozenLake-v1 to cover LSTM case.
        frameworks = ["torch", "tf2"]
        env_names = ["CartPole-v1", "Pendulum-v1", "ALE/Breakout-v5"]
        # TODO(Artur): Re-enable LSTM
        lstm = [False]
        config_combinations = [frameworks, env_names, lstm]
        for config in itertools.product(*config_combinations):
            fw, env_name, lstm = config
            if lstm and fw == "tf2":
                # LSTM not implemented in TF2 yet
                continue
            if env_name == "ALE/Breakout-v5" and fw == "tf2":
                # TODO(Artur): Implement CNN in TF2.
                continue
            print(f"[FW={fw} | [ENV={env_name}] | LSTM={lstm}")
            # TODO(Artur): Figure out why this is needed and fix it.
            if env_name.startswith("ALE/"):
                env = gym.make("GymV26Environment-v0", env_id=env_name)
            else:
                env = gym.make(env_name)

            preprocessor_cls = get_preprocessor(env.observation_space)
            preprocessor = preprocessor_cls(env.observation_space)

            module = _get_ppo_module(
                framework=fw,
                env=env,
                lstm=lstm,
                observation_space=preprocessor.observation_space,
            )

            # collect a batch of data
            batches = []
            obs, _ = env.reset()
            obs = preprocessor.transform(obs)
            tstep = 0
            # TODO (Artur): Un-uncomment once Policy supports RNN
            # state_in = module.get_initial_state()
            # state_in = tree.map_structure(
            #     lambda x: x[None], convert_to_torch_tensor(state_in)
            # )
            # initial_state = state_in

            while tstep < 10:
                input_batch = _get_input_batch_from_obs(fw, obs)
                # TODO (Artur): Un-uncomment once Policy supports RNN
                # input_batch[STATE_IN] = state_in
                # input_batch[SampleBatch.SEQ_LENS] = np.array([1])

                fwd_out = module.forward_exploration(input_batch)
                _action = fwd_out["action_dist"].sample()
                action = convert_to_numpy(_action[0])
                action_logp = convert_to_numpy(fwd_out["action_dist"].logp(_action)[0])
                new_obs, reward, terminated, truncated, _ = env.step(action)
                new_obs = preprocessor.transform(new_obs)
                output_batch = {
                    SampleBatch.OBS: obs,
                    SampleBatch.NEXT_OBS: new_obs,
                    SampleBatch.ACTIONS: action,
                    SampleBatch.ACTION_LOGP: action_logp,
                    SampleBatch.REWARDS: np.array(reward),
                    SampleBatch.TERMINATEDS: np.array(terminated),
                    SampleBatch.TRUNCATEDS: np.array(truncated),
                }

                # TODO (Artur): Un-uncomment once Policy supports RNN
                # assert STATE_OUT in fwd_out
                # state_in = fwd_out[STATE_OUT]
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
                # TODO (Artur): Un-uncomment once Policy supports RNN
                # fwd_in[STATE_IN] = initial_state
                # fwd_in[SampleBatch.SEQ_LENS] = torch.Tensor([10])

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
                fwd_in = tree.map_structure(
                    lambda x: tf.convert_to_tensor(x, dtype=tf.float32), batch
                )
                # TODO (Artur): Un-uncomment once Policy supports RNN
                # fwd_in[STATE_IN] = initial_state
                # fwd_in[SampleBatch.SEQ_LENS] = torch.Tensor([10])
                with tf.GradientTape() as tape:
                    fwd_out = module.forward_train(fwd_in)
                    loss = dummy_tf_ppo_loss(fwd_in, fwd_out)
                grads = tape.gradient(loss, module.trainable_variables)
                for grad in grads:
                    self.assertIsNotNone(grad)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
