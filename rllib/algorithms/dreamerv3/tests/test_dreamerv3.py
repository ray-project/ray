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

import ray
from ray.rllib.algorithms.dreamerv3 import dreamerv3
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import framework_iterator


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
            .training(
                # Keep things simple. Especially the long dream rollouts seem
                # to take an enormous amount of time (initially).
                batch_size_B=4,
                horizon_H=5,
                batch_length_T=16,
                model_size="nano",  # Use a tiny model for testing
                symlog_obs=True,
                use_float16=True,
            )
            .resources(
                num_learner_workers=2,  # Try with 2 Learners.
                num_cpus_per_learner_worker=1,
                num_gpus_per_learner_worker=0,
            )
        )

        # TODO (sven): Add a `get_model_config` utility to AlgorithmConfig
        #  that - for now - merges the user provided model_dict (which only
        #  contains settings that only affect the model, e.g. model_size)
        #  with the AlgorithmConfig-wide settings that are relevant for the model
        #  (e.g. `batch_size_B`).
        # config.get_model_config()

        num_iterations = 2

        for _ in framework_iterator(config, frameworks="tf2"):
            for env in ["ALE/MsPacman-v5", "FrozenLake-v1", "CartPole-v1"]:
                print("Env={}".format(env))
                config.environment(env)
                algo = config.build()

                for i in range(num_iterations):
                    results = algo.train()
                    print(results)

                algo.stop()

    def test_dreamerv3_dreamer_model_sizes(self):
        """Tests, whether the different model sizes match the ones reported in [1]."""

        # For Atari, these are the exact numbers from the repo ([3]).
        # However, for CartPole + size "S" and "M", the author's original code will not
        # match for the world model count. This is due to the fact that the author uses
        # encoder/decoder nets with 5x1024 nodes (which corresponds to XL) regardless of
        # the `model_size` settings (iff >="S").
        expected_num_params_world_model = {
            "XS_cartpole": 2435076,
            "S_cartpole": 7493380,
            "M_cartpole": 16206084,
            "L_cartpole": 37802244,
            "XL_cartpole": 108353796,
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
            config.training(model={"model_size": model_size})

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
                module_spec = config.get_marl_module_spec(policy_dict=policy_dict)
                rl_module = module_spec.build()[DEFAULT_POLICY_ID]

                # Count the generated RLModule's parameters and compare to the paper's
                # reported numbers ([1] and [3]).
                num_params_world_model = sum(
                    np.prod(v.shape.as_list())
                    for v in rl_module.world_model.trainable_variables
                )
                self.assertEqual(
                    num_params_world_model,
                    expected_num_params_world_model[f"{model_size}_{env_name}"],
                )
                num_params_actor = sum(
                    np.prod(v.shape.as_list())
                    for v in rl_module.actor.trainable_variables
                )
                self.assertEqual(
                    num_params_actor,
                    expected_num_params_actor[f"{model_size}_{env_name}"],
                )
                num_params_critic = sum(
                    np.prod(v.shape.as_list())
                    for v in rl_module.critic.trainable_variables
                )
                self.assertEqual(
                    num_params_critic,
                    expected_num_params_critic[f"{model_size}_{env_name}"],
                )
                print("\tok")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
