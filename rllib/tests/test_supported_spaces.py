from gym.spaces import Box, Dict, Discrete, Tuple, MultiDiscrete
import numpy as np
import unittest

import ray
from ray.rllib.algorithms.a3c import A3CConfig
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.ars import ARSConfig
from ray.rllib.algorithms.ddpg import DDPGConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.algorithms.es import ESConfig
from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.models.tf.complex_input_net import ComplexInputNetwork as ComplexNet
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork as FCNet
from ray.rllib.models.tf.visionnet import VisionNetwork as VisionNet
from ray.rllib.models.torch.complex_input_net import (
    ComplexInputNetwork as TorchComplexNet,
)
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFCNet
from ray.rllib.models.torch.visionnet import VisionNetwork as TorchVisionNet
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.test_utils import framework_iterator

ACTION_SPACES_TO_TEST = {
    "discrete": Discrete(5),
    "vector1d": Box(-1.0, 1.0, (5,), dtype=np.float32),
    "vector2d": Box(-1.0, 1.0, (5,), dtype=np.float32),
    "int_actions": Box(0, 3, (2, 3), dtype=np.int32),
    "multidiscrete": MultiDiscrete([1, 2, 3, 4]),
    "tuple": Tuple([Discrete(2), Discrete(3), Box(-1.0, 1.0, (5,), dtype=np.float32)]),
    "dict": Dict(
        {
            "action_choice": Discrete(3),
            "parameters": Box(-1.0, 1.0, (1,), dtype=np.float32),
            "yet_another_nested_dict": Dict({"a": Tuple([Discrete(2), Discrete(3)])}),
        }
    ),
}

OBSERVATION_SPACES_TO_TEST = {
    "discrete": Discrete(5),
    "vector1d": Box(-1.0, 1.0, (5,), dtype=np.float32),
    "vector2d": Box(-1.0, 1.0, (5, 5), dtype=np.float32),
    "image": Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32),
    "vizdoomgym": Box(-1.0, 1.0, (240, 320, 3), dtype=np.float32),
    "tuple": Tuple([Discrete(10), Box(-1.0, 1.0, (5,), dtype=np.float32)]),
    "dict": Dict(
        {
            "task": Discrete(10),
            "position": Box(-1.0, 1.0, (5,), dtype=np.float32),
        }
    ),
}


def check_support(alg, config, train=True, check_bounds=False, tf2=False):
    config["log_level"] = "ERROR"
    config["train_batch_size"] = 50
    config["rollout_fragment_length"] = 10
    config["env"] = RandomEnv

    def _do_check(alg, config, a_name, o_name):
        fw = config["framework"]
        action_space = ACTION_SPACES_TO_TEST[a_name]
        obs_space = OBSERVATION_SPACES_TO_TEST[o_name]
        print(
            "=== Testing {} (fw={}) A={} S={} ===".format(
                alg, fw, action_space, obs_space
            )
        )
        config.update_from_dict(
            dict(
                env_config=dict(
                    action_space=action_space,
                    observation_space=obs_space,
                    reward_space=Box(1.0, 1.0, shape=(), dtype=np.float32),
                    p_done=1.0,
                    check_action_bounds=check_bounds,
                )
            )
        )
        stat = "ok"

        try:
            algo = config.build()
        except ray.exceptions.RayActorError as e:
            if len(e.args) >= 2 and isinstance(e.args[2], UnsupportedSpaceException):
                stat = "unsupported"
            elif isinstance(e.args[0].args[2], UnsupportedSpaceException):
                stat = "unsupported"
            else:
                raise
        except UnsupportedSpaceException:
            stat = "unsupported"
        else:
            if alg not in ["DDPG", "ES", "ARS", "SAC"]:
                # 2D (image) input: Expect VisionNet.
                if o_name in ["atari", "image"]:
                    if fw == "torch":
                        assert isinstance(algo.get_policy().model, TorchVisionNet)
                    else:
                        assert isinstance(algo.get_policy().model, VisionNet)
                # 1D input: Expect FCNet.
                elif o_name == "vector1d":
                    if fw == "torch":
                        assert isinstance(algo.get_policy().model, TorchFCNet)
                    else:
                        assert isinstance(algo.get_policy().model, FCNet)
                # Could be either one: ComplexNet (if disabled Preprocessor)
                # or FCNet (w/ Preprocessor).
                elif o_name == "vector2d":
                    if fw == "torch":
                        assert isinstance(
                            algo.get_policy().model, (TorchComplexNet, TorchFCNet)
                        )
                    else:
                        assert isinstance(algo.get_policy().model, (ComplexNet, FCNet))
            if train:
                algo.train()
            algo.stop()
        print(stat)

    frameworks = ("tf", "torch")
    if tf2:
        frameworks += ("tf2",)
    for _ in framework_iterator(config, frameworks=frameworks):
        # Zip through action- and obs-spaces.
        for a_name, o_name in zip(
            ACTION_SPACES_TO_TEST.keys(), OBSERVATION_SPACES_TO_TEST.keys()
        ):
            _do_check(alg, config, a_name, o_name)
        # Do the remaining obs spaces.
        assert len(OBSERVATION_SPACES_TO_TEST) >= len(ACTION_SPACES_TO_TEST)
        fixed_action_key = next(iter(ACTION_SPACES_TO_TEST.keys()))
        for i, o_name in enumerate(OBSERVATION_SPACES_TO_TEST.keys()):
            if i < len(ACTION_SPACES_TO_TEST):
                continue
            _do_check(alg, config, fixed_action_key, o_name)


class TestSupportedSpacesPG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_a3c(self):
        config = (
            A3CConfig()
            .rollouts(num_rollout_workers=1)
            .training(
                optimizer={"grads_per_step": 1},
                model={"fcnet_hiddens": [10]},
            )
        )
        check_support("A3C", config, check_bounds=True)

    def test_appo(self):
        config = (
            APPOConfig()
            .resources(num_gpus=0)
            .training(vtrace=False, model={"fcnet_hiddens": [10]})
        )
        check_support("APPO", config, train=False)
        config.training(vtrace=True)
        check_support("APPO", config)

    def test_impala(self):
        check_support(
            "IMPALA",
            (
                ImpalaConfig()
                .resources(num_gpus=0)
                .training(model={"fcnet_hiddens": [10]})
            ),
        )

    def test_ppo(self):
        config = (
            PPOConfig()
            .rollouts(num_rollout_workers=0, rollout_fragment_length=50)
            .training(
                train_batch_size=100,
                num_sgd_iter=1,
                sgd_minibatch_size=50,
                model={
                    "fcnet_hiddens": [10],
                },
            )
        )
        check_support("PPO", config, check_bounds=True, tf2=True)

    def test_ppo_no_preprocessors_gpu(self):
        # Same test as test_ppo, but also test if we are able to move models and tensors
        # on the same device when not using preprocessors.
        # (Artur) This covers a superposition of these edge cases that can lead to
        # obscure errors.
        config = (
            PPOConfig()
            .rollouts(num_rollout_workers=0, rollout_fragment_length=50)
            .training(
                train_batch_size=100,
                num_sgd_iter=1,
                sgd_minibatch_size=50,
                model={
                    "fcnet_hiddens": [10],
                },
            )
            .experimental(_disable_preprocessor_api=True)
            .resources(num_gpus=1)
        )
        check_support("PPO", config, check_bounds=True, tf2=True)


class TestSupportedSpacesOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ddpg(self):
        check_support(
            "DDPG",
            DDPGConfig()
            .exploration(exploration_config={"ou_base_scale": 100.0})
            .reporting(min_sample_timesteps_per_iteration=1)
            .training(
                replay_buffer_config={"capacity": 1000},
                use_state_preprocessor=True,
            ),
            check_bounds=True,
        )

    def test_dqn(self):
        config = (
            DQNConfig()
            .reporting(min_sample_timesteps_per_iteration=1)
            .training(
                replay_buffer_config={
                    "capacity": 1000,
                }
            )
        )
        check_support("DQN", config, tf2=True)

    def test_sac(self):
        check_support(
            "SAC",
            SACConfig().training(replay_buffer_config={"capacity": 1000}),
            check_bounds=True,
        )


class TestSupportedSpacesEvolutionAlgos(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ars(self):
        check_support(
            "ARS",
            ARSConfig()
            .rollouts(num_rollout_workers=1)
            .training(noise_size=1500000, num_rollouts=1, rollouts_used=1),
        )

    def test_es(self):
        check_support(
            "ES",
            ESConfig()
            .rollouts(num_rollout_workers=1)
            .training(noise_size=1500000, episodes_per_batch=1, train_batch_size=1),
        )


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
