import logging
import unittest

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.utils.test_utils import check_supported_spaces


logger = logging.getLogger(__name__)


class TestSupportedSpacesIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala(self):
        check_supported_spaces(
            "IMPALA",
            (
                IMPALAConfig()
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
            APPOConfig().resources(num_gpus=0).training(model={"fcnet_hiddens": [10]})
        )
        check_supported_spaces("APPO", config)


class TestSupportedSpacesA3C(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()


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
            .env_runners(num_env_runners=2, rollout_fragment_length=50)
            .training(
                train_batch_size=100,
                num_epochs=1,
                minibatch_size=50,
                model={
                    "fcnet_hiddens": [10],
                },
            )
        )
        check_supported_spaces("PPO", config, check_bounds=True)


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
            .env_runners(num_env_runners=2, rollout_fragment_length=50)
            .training(
                train_batch_size=100,
                num_epochs=1,
                minibatch_size=50,
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

        check_supported_spaces(
            "PPO",
            config,
            check_bounds=True,
            frameworks=["torch", "tf"],
            use_gpu=True,
        )


class TestSupportedSpacesDQN(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

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
        check_supported_spaces("DQN", config, frameworks=["tf2", "torch", "tf"])


class TestSupportedSpacesOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sac(self):
        check_supported_spaces(
            "SAC",
            SACConfig().training(replay_buffer_config={"capacity": 1000}),
            check_bounds=True,
        )


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
