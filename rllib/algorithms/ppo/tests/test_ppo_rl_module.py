import itertools
import unittest

import gymnasium as gym
import numpy as np
import tree

import ray
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import (
    PPOTfRLModule,
)
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


tf1, tf, _ = try_import_tf()
tf1.enable_eager_execution()
torch, nn = try_import_torch()


def get_expected_module_config(env, model_config_dict, observation_space):
    config = RLModuleConfig(
        observation_space=observation_space,
        action_space=env.action_space,
        model_config_dict=model_config_dict,
        catalog_class=PPOCatalog,
    )

    return config


def dummy_torch_ppo_loss(module, batch, fwd_out):
    adv = batch[Columns.REWARDS] - module.compute_values(batch)
    action_dist_class = module.get_train_action_dist_cls()
    action_probs = action_dist_class.from_logits(
        fwd_out[Columns.ACTION_DIST_INPUTS]
    ).logp(batch[Columns.ACTIONS])
    actor_loss = -(action_probs * adv).mean()
    critic_loss = (adv**2).mean()
    loss = actor_loss + critic_loss

    return loss


def dummy_tf_ppo_loss(module, batch, fwd_out):
    adv = batch[Columns.REWARDS] - module.compute_values(batch)
    action_dist_class = module.get_train_action_dist_cls()
    action_probs = action_dist_class.from_logits(
        fwd_out[Columns.ACTION_DIST_INPUTS]
    ).logp(batch[Columns.ACTIONS])
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


def _get_input_batch_from_obs(framework, obs, lstm):
    if framework == "torch":
        batch = {
            Columns.OBS: convert_to_torch_tensor(obs)[None],
        }
    else:
        batch = {
            Columns.OBS: tf.convert_to_tensor(obs)[None],
        }
    if lstm:
        batch[Columns.OBS] = batch[Columns.OBS][None]
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
        lstm = [True, False]
        config_combinations = [frameworks, env_names, fwd_fns, lstm]
        for config in itertools.product(*config_combinations):
            fw, env_name, fwd_fn, lstm = config
            if lstm and fw == "tf2":
                # LSTM not implemented in TF2 yet
                continue
            print(f"[FW={fw} | [ENV={env_name}] | [FWD={fwd_fn}] | LSTM" f"={lstm}")
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

            batch = _get_input_batch_from_obs(fw, obs, lstm)

            if lstm:
                state_in = module.get_initial_state()
                if fw == "torch":
                    state_in = convert_to_torch_tensor(state_in)
                state_in = tree.map_structure(lambda x: x[None], state_in)
                batch[Columns.STATE_IN] = state_in

            if fwd_fn == "forward_exploration":
                module.forward_exploration(batch)
            else:
                module.forward_inference(batch)

    def test_forward_train(self):
        # TODO: Add FrozenLake-v1 to cover LSTM case.
        frameworks = ["torch", "tf2"]
        env_names = ["CartPole-v1", "Pendulum-v1", "ALE/Breakout-v5"]
        lstm = [False, True]
        config_combinations = [frameworks, env_names, lstm]
        for config in itertools.product(*config_combinations):
            fw, env_name, lstm = config
            print(f"[FW={fw} | [ENV={env_name}] | LSTM={lstm}")
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

            if lstm:
                state_in = module.get_initial_state()
                if fw == "torch":
                    state_in = tree.map_structure(
                        lambda x: x[None], convert_to_torch_tensor(state_in)
                    )
                else:
                    state_in = tree.map_structure(
                        lambda x: tf.convert_to_tensor(x)[None], state_in
                    )
                initial_state = state_in

            while tstep < 10:
                input_batch = _get_input_batch_from_obs(fw, obs, lstm=lstm)
                if lstm:
                    input_batch[Columns.STATE_IN] = state_in

                fwd_out = module.forward_exploration(input_batch)
                action_dist_cls = module.get_exploration_action_dist_cls()
                action_dist = action_dist_cls.from_logits(
                    fwd_out[Columns.ACTION_DIST_INPUTS]
                )
                _action = action_dist.sample()
                action = convert_to_numpy(_action[0])
                action_logp = convert_to_numpy(action_dist.logp(_action)[0])
                if lstm:
                    # Since this is inference, fwd out should only contain one action
                    assert len(action) == 1
                    action = action[0]
                new_obs, reward, terminated, truncated, _ = env.step(action)
                new_obs = preprocessor.transform(new_obs)
                output_batch = {
                    Columns.OBS: obs,
                    Columns.NEXT_OBS: new_obs,
                    Columns.ACTIONS: action,
                    Columns.ACTION_LOGP: action_logp,
                    Columns.REWARDS: np.array(reward),
                    Columns.TERMINATEDS: np.array(terminated),
                    Columns.TRUNCATEDS: np.array(truncated),
                    Columns.STATE_IN: None,
                }

                if lstm:
                    assert Columns.STATE_OUT in fwd_out
                    state_in = fwd_out[Columns.STATE_OUT]
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
                    fwd_in[Columns.STATE_IN] = initial_state
                    # If we test lstm, the collected timesteps make up only one batch
                    fwd_in = {
                        k: torch.unsqueeze(v, 0) if k != Columns.STATE_IN else v
                        for k, v in fwd_in.items()
                    }

                # forward train
                # before training make sure module is on the right device
                # and in training mode
                module.to("cpu")
                module.train()
                fwd_out = module.forward_train(fwd_in)
                loss = dummy_torch_ppo_loss(module, fwd_in, fwd_out)
                loss.backward()

                # check that all neural net parameters have gradients
                for param in module.parameters():
                    self.assertIsNotNone(param.grad)
            else:
                fwd_in = tree.map_structure(
                    lambda x: tf.convert_to_tensor(x, dtype=tf.float32), batch
                )
                if lstm:
                    fwd_in[Columns.STATE_IN] = initial_state
                    # If we test lstm, the collected timesteps make up only one batch
                    fwd_in = {
                        k: tf.expand_dims(v, 0) if k != Columns.STATE_IN else v
                        for k, v in fwd_in.items()
                    }

                with tf.GradientTape() as tape:
                    fwd_out = module.forward_train(fwd_in)
                    loss = dummy_tf_ppo_loss(module, fwd_in, fwd_out)
                grads = tape.gradient(loss, module.trainable_variables)
                for grad in grads:
                    self.assertIsNotNone(grad)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
