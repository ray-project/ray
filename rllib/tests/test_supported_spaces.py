import logging
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
        config.training(vtrace=True)
        check_supported_spaces("APPO", config)


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
        check_supported_spaces("A3C", config, check_bounds=True)


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

    def test_ddpg(self):
        check_supported_spaces(
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

    def test_sac(self):
        check_supported_spaces(
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
        check_supported_spaces(
            "ARS",
            ARSConfig()
            .rollouts(num_rollout_workers=1)
            .training(noise_size=1500000, num_rollouts=1, rollouts_used=1),
            # framework=None corresponds to numpy since ARS uses a numpy policy
            frameworks=[None],
        )

    def test_es(self):
        check_supported_spaces(
            "ES",
            ESConfig()
            .rollouts(num_rollout_workers=1)
            .training(noise_size=1500000, episodes_per_batch=1, train_batch_size=1),
            # framework=None corresponds to numpy since ES uses a numpy policy
            frameworks=[None],
        )


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
