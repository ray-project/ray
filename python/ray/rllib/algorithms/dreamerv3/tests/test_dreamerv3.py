"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf

[3]
D. Hafner's (author) original code repo (for JAX):
https://github.com/danijar/dreamerv3
"""

import unittest

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

import ray
from ray import tune
from ray.rllib.algorithms.dreamerv3 import dreamerv3
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.env.wrappers.dm_control_wrapper import ActionClip, DMCEnv
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.test_utils import check

torch, nn = try_import_torch()


class TestDreamerV3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dreamerv3_compilation(self):
        """Test whether DreamerV3 can be built with all frameworks."""

        # Build a DreamerV3Config object.
        config = (
            dreamerv3.DreamerV3Config()
            .env_runners(num_env_runners=0)
            .training(
                # Keep things simple. Especially the long dream rollouts seem
                # to take an enormous amount of time (initially).
                batch_size_B=4,
                horizon_H=5,
                batch_length_T=16,
                model_size="nano",  # Use a tiny model for testing
                symlog_obs=True,
                use_float16=False,
            )
            .learners(
                num_learners=2,
                num_cpus_per_learner=1,
                num_gpus_per_learner=0,
            )
        )

        num_iterations = 3

        for env in [
            # "DMC/cartpole/swingup",  # causes strange MuJoCo error(s) on CI
            "FrozenLake-v1",
            "CartPole-v1",
            "ale_py:ALE/MsPacman-v5",
            "Pendulum-v1",
        ]:
            print("Env={}".format(env))

            # Add one-hot observations for FrozenLake env.
            if env == "FrozenLake-v1":
                config.env_runners(
                    env_to_module_connector=(
                        lambda env, spaces, device: FlattenObservations()
                    )
                )
            else:
                config.env_runners(env_to_module_connector=None)

            # Add Atari preprocessing.
            if env == "ale_py:ALE/MsPacman-v5":

                def env_creator(cfg):
                    return wrap_atari_for_new_api_stack(
                        gym.make(env, **cfg, render_mode="rgb_array"),
                        # No frame-stacking. DreamerV3 processes color images with a
                        # GRU, so partial observability is ok.
                        framestack=None,
                        grayscale=False,
                    )

                tune.register_env("env", env_creator)
                env = "env"

            elif env.startswith("DMC"):
                parts = env.split("/")
                assert len(parts) == 3, (
                    "ERROR: DMC env must be formatted as 'DMC/[task]/[domain]', e.g. "
                    f"'DMC/cartpole/swingup'! You provided '{env}'."
                )

                def env_creator(cfg):
                    return ActionClip(
                        DMCEnv(
                            parts[1],
                            parts[2],
                            from_pixels=True,
                            channels_first=False,
                        )
                    )

                tune.register_env("env", env_creator)
                env = "env"

            config.environment(env)
            algo = config.build_algo()
            obs_space = algo.env_runner._env_to_module.observation_space
            act_space = algo.env_runner.env.single_action_space
            rl_module = algo.env_runner.module

            for i in range(num_iterations):
                results = algo.train()
                print(results)
            # Test dream trajectory w/ recreated observations.
            sample = algo.replay_buffer.sample()
            start_states = rl_module.dreamer_model.get_initial_state()
            start_states = tree.map_structure(
                # Repeat only the batch dimension (B times).
                lambda s: s.unsqueeze(0).repeat(1, *([1] * len(s.shape))),
                start_states,
            )
            dream = rl_module.dreamer_model.dream_trajectory_with_burn_in(
                start_states=start_states,
                timesteps_burn_in=5,
                timesteps_H=45,
                observations=torch.from_numpy(sample["obs"][:1]),  # B=1
                actions=torch.from_numpy(
                    one_hot(
                        sample["actions"],
                        depth=act_space.n,
                    )
                    if isinstance(act_space, gym.spaces.Discrete)
                    else sample["actions"]
                )[
                    :1
                ],  # B=1
            )
            check(
                dream["actions_dreamed_t0_to_H_BxT"].shape,
                (46, 1)
                + (
                    (act_space.n,)
                    if isinstance(act_space, gym.spaces.Discrete)
                    else tuple(act_space.shape)
                ),
            )
            check(dream["continues_dreamed_t0_to_H_BxT"].shape, (46, 1))
            check(
                dream["observations_dreamed_t0_to_H_BxT"].shape,
                [46, 1] + list(obs_space.shape),
            )
            algo.stop()

    def test_dreamerv3_dreamer_model_sizes(self):
        """Tests, whether the different model sizes match the ones reported in [1]."""

        # For Atari, these are the exact numbers from the repo ([3]).
        # However, for CartPole + size "S" and "M", the author's original code will not
        # match for the world model count. This is due to the fact that the author uses
        # encoder/decoder nets with 5x1024 nodes (which corresponds to XL) regardless of
        # the `model_size` settings (iff >="S").
        expected_num_params_world_model = {
            # XS encoder
            # kernel=[4, 256], (no bias), layernorm=[256],[256]
            # XS reward_predictor
            # kernel=[1280, 256], (no bias), layernorm[256],[256]
            # kernel=[256, 255] bias=[255]
            # 1280=1024 (z-state) + 256 (h-state)
            # XS continue_predictor
            # kernel=[1280, 256], (no bias), layernorm=[256],[256]
            # kernel=[256, 1] bias=[1]
            # XS sequence_model
            # [
            # pre-MLP: kernel=[1026, 256], (no bias), layernorm=[256],[256], silu
            # custom GRU: kernel=[512, 768], (no bias), layernorm=[768],[768]
            # ]
            # XS decoder
            # kernel=[1280, 256], (no bias), layernorm=[256],[256]
            # kernel=[256, 4] bias=[4]
            # XS posterior_mlp
            # kernel=[512, 256], (no bias), layernorm=[256],[256]
            # XS posterior_representation_layer
            # kernel=[256, 1024], bias=[1024]
            "XS_cartpole": 2435076,
            "S_cartpole": 7493380,
            "M_cartpole": 16206084,
            "L_cartpole": 37802244,
            "XL_cartpole": 108353796,
            # XS encoder (atari)
            # cnn kernel=[4, 4, 3, 24], (no bias), layernorm=[24],[24],
            # cnn kernel=[4, 4, 24, 48], (no bias), layernorm=[48],[48],
            # cnn kernel=[4, 4, 48, 96], (no bias), layernorm=[96],[96],
            # cnn kernel=[4, 4, 96, 192], (no bias), layernorm=[192],[192],
            # XS decoder (atari)
            # init dense kernel[1280, 3072] bias=[3072] -> reshape into image
            # [4, 4, 96, 192], [96], [96]
            # [4, 4, 48, 96], [48], [48],
            # [4, 4, 24, 48], [24], [24],
            # [4, 4, 3, 24], [3] <- no layernorm at end
            "XS_atari": 7538979,
            "S_atari": 15687811,
            "M_atari": 32461635,
            "L_atari": 68278275,
            "XL_atari": 181558659,
        }

        # All values confirmed against [3] (100% match).
        expected_num_params_actor = {
            # hidden=[1280, 256]
            # hidden_norm=[256], [256]
            # pi (2 actions)=[256, 2], [2]
            "XS_cartpole": 328706,
            "S_cartpole": 1051650,
            "M_cartpole": 2135042,
            "L_cartpole": 4136450,
            "XL_cartpole": 9449474,
            "XS_atari": 329734,
            "S_atari": 1053702,
            "M_atari": 2137606,
            "L_atari": 4139526,
            "XL_atari": 9453574,
        }

        # All values confirmed against [3] (100% match).
        expected_num_params_critic = {
            # hidden=[1280, 256]
            # hidden_norm=[256], [256]
            # vf (buckets)=[256, 255], [255]
            "XS_cartpole": 393727,
            "S_cartpole": 1181439,
            "M_cartpole": 2297215,
            "L_cartpole": 4331007,
            "XL_cartpole": 9708799,
            "XS_atari": 393727,
            "S_atari": 1181439,
            "M_atari": 2297215,
            "L_atari": 4331007,
            "XL_atari": 9708799,
        }

        config = dreamerv3.DreamerV3Config().training(
            batch_length_T=16,
            horizon_H=5,
            symlog_obs=True,
        )

        # Check all model_sizes described in the paper ([1]) on matching the number
        # of parameters to RLlib's implementation.
        for model_size in ["XS", "S", "M", "L", "XL"]:
            config.model_size = model_size

            # Atari and CartPole spaces.
            for obs_space, num_actions, env_name in [
                (gym.spaces.Box(-1.0, 0.0, (4,), np.float32), 2, "cartpole"),
                (gym.spaces.Box(-1.0, 0.0, (64, 64, 3), np.float32), 6, "atari"),
            ]:
                print(f"Testing model_size={model_size} on env-type: {env_name} ..")
                config.environment(
                    observation_space=obs_space,
                    action_space=gym.spaces.Discrete(num_actions),
                )

                # Create our RLModule to compute actions with.
                policy_dict, _ = config.get_multi_agent_setup()
                module_spec = config.get_multi_rl_module_spec(policy_dict=policy_dict)
                rl_module = module_spec.build()[DEFAULT_MODULE_ID]

                # Count the generated RLModule's parameters and compare to the
                # paper's reported numbers ([1] and [3]).
                num_params_world_model = sum(
                    np.prod(v.shape)
                    for v in rl_module.world_model.parameters()
                    if v.requires_grad
                )
                self.assertEqual(
                    num_params_world_model,
                    expected_num_params_world_model[f"{model_size}_{env_name}"],
                )
                num_params_actor = sum(
                    np.prod(v.shape)
                    for v in rl_module.actor.parameters()
                    if v.requires_grad
                )
                self.assertEqual(
                    num_params_actor,
                    expected_num_params_actor[f"{model_size}_{env_name}"],
                )
                num_params_critic = sum(
                    np.prod(v.shape)
                    for v in rl_module.critic.parameters()
                    if v.requires_grad
                )
                self.assertEqual(
                    num_params_critic,
                    expected_num_params_critic[f"{model_size}_{env_name}"],
                )
                print("\tok")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
