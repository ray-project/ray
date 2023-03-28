import logging
import time
import unittest

import numpy as np
from gymnasium.spaces import Box, Dict, Discrete, Tuple, MultiDiscrete, MultiBinary

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

logger = logging.getLogger(__name__)

ACTION_SPACES_TO_TEST = {
    # Test discrete twice here until we support multi_binary action spaces
    "discrete": Discrete(5),
    "continuous": Box(-1.0, 1.0, (5,), dtype=np.float32),
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
    "multi_binary": MultiBinary([3, 10, 10]),
    "discrete": Discrete(5),
    "continuous": Box(-1.0, 1.0, (5,), dtype=np.float32),
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

# TODO(Artur): Add back tf2 once we CNNs there
RLMODULE_SUPPORTED_FRAMEWORKS = {"torch"}

# The action spaces that we test RLModules with
RLMODULE_SUPPORTED_ACTION_SPACES = ["discrete", "continuous"]

# The observation spaces that we test RLModules with
RLMODULE_SUPPORTED_OBSERVATION_SPACES = [
    "multi_binary",
    "discrete",
    "continuous",
    "image",
    "vizdoomgym",
    "tuple",
    "dict",
]

DEFAULT_OBSERVATION_SPACE = DEFAULT_ACTION_SPACE = "discrete"


def check_support(alg, config, train=True, check_bounds=False, tf2=False):
    config["log_level"] = "ERROR"
    config["env"] = RandomEnv

    def _do_check(alg, config, a_name, o_name):
        # We need to copy here so that this validation does not affect the actual
        # validation method call further down the line.
        config_copy = config.copy()
        config_copy.validate()
        # If RLModules are enabled, we need to skip a few tests for now:
        if config_copy._enable_rl_module_api:
            # Skip PPO cases in which RLModules don't support the given spaces yet.
            if o_name not in RLMODULE_SUPPORTED_OBSERVATION_SPACES:
                logger.warning(
                    "Skipping PPO test with RLModules for obs space {}".format(o_name)
                )
                return
            if a_name not in RLMODULE_SUPPORTED_ACTION_SPACES:
                logger.warning(
                    "Skipping PPO test with RLModules for action space {}".format(
                        a_name
                    )
                )
                return

        fw = config["framework"]
        action_space = ACTION_SPACES_TO_TEST[a_name]
        obs_space = OBSERVATION_SPACES_TO_TEST[o_name]
        print(
            "=== Testing {} (fw={}) action_space={} obs_space={} ===".format(
                alg, fw, action_space, obs_space
            )
        )
        t0 = time.time()
        config.update_from_dict(
            dict(
                env_config=dict(
                    action_space=action_space,
                    observation_space=obs_space,
                    reward_space=Box(1.0, 1.0, shape=(), dtype=np.float32),
                    p_terminated=1.0,
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
            if alg not in ["DDPG", "ES", "ARS", "SAC", "PPO"]:
                # 2D (image) input: Expect VisionNet.
                if o_name in ["atari", "image"]:
                    if fw == "torch":
                        assert isinstance(algo.get_policy().model, TorchVisionNet)
                    else:
                        assert isinstance(algo.get_policy().model, VisionNet)
                # 1D input: Expect FCNet.
                elif o_name == "continuous":
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
        print("Test: {}, ran in {}s".format(stat, time.time() - t0))

    frameworks = {"tf", "torch"}
    if tf2:
        frameworks.add("tf2")

    if config._enable_rl_module_api:
        # Only test the frameworks that are supported by RLModules.
        frameworks = frameworks.intersection(RLMODULE_SUPPORTED_FRAMEWORKS)

    for _ in framework_iterator(config, frameworks=frameworks):
        # Test all action spaces first.
        for a_name in ACTION_SPACES_TO_TEST.keys():
            o_name = DEFAULT_OBSERVATION_SPACE
            _do_check(alg, config, a_name, o_name)

        # Now test all observation spaces.
        for o_name in OBSERVATION_SPACES_TO_TEST.keys():
            a_name = DEFAULT_ACTION_SPACE
            _do_check(alg, config, a_name, o_name)


class TestSupportedSpacesIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala(self):
        check_support(
            "IMPALA",
            (
                ImpalaConfig()
                .resources(num_gpus=0)
                .training(model={"fcnet_hiddens": [10]})
            ),
        )


class TestSupportedSpacesAPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_appo(self):
        config = (
            APPOConfig()
            .resources(num_gpus=0)
            .training(vtrace=False, model={"fcnet_hiddens": [10]})
        )
        check_support("APPO", config, train=False)
        config.training(vtrace=True)
        check_support("APPO", config)


class TestSupportedSpacesA3C(unittest.TestCase):
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


class TestSupportedSpacesPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ppo(self):
        config = (
            PPOConfig()
            .rollouts(num_rollout_workers=2, rollout_fragment_length=50)
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


class TestSupportedSpacesPPONoPreprocessorGPU(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_gpus=1)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_ppo_no_preprocessors_gpu(self):
        # Same test as test_ppo, but also test if we are able to move models and tensors
        # on the same device when not using preprocessors.
        # (Artur) This covers a superposition of these edge cases that can lead to
        # obscure errors.
        config = (
            PPOConfig()
            .rollouts(num_rollout_workers=2, rollout_fragment_length=50)
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

        # (Artur): This test only works under the old ModelV2 API because we
        # don't offer arbitrarily complex Models under the RLModules API without
        # preprocessors. Such input spaces require custom implementations of the
        # input space.
        # TODO (Artur): Delete this test once we remove ModelV2 API.
        config.rl_module(_enable_rl_module_api=False).training(
            _enable_learner_api=False
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
