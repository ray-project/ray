import pyspiel
import unittest

import ray
import ray.rllib.agents.alpha_star as alpha_star
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.tune import register_env

# Connect-4 OpenSpiel env.
register_env("connect_four", lambda _: OpenSpielEnv(pyspiel.load_game("connect_four")))


class TestAlphaStar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=20)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_alpha_star_compilation(self):
        """Test whether a AlphaStarTrainer can be built with all frameworks."""
        config = (
            alpha_star.AlphaStarConfig()
            .environment(env="connect_four")
            .training(
                gamma=1.0,
                model={"fcnet_hiddens": [256, 256, 256]},
                vf_loss_coeff=0.01,
                entropy_coeff=0.004,
                league_builder_config={
                    "win_rate_threshold_for_new_snapshot": 0.8,
                    "num_random_policies": 2,
                    "num_learning_league_exploiters": 1,
                    "num_learning_main_exploiters": 1,
                },
                grad_clip=10.0,
                replay_buffer_capacity=10,
                replay_buffer_replay_ratio=0.0,
                use_kl_loss=True,
            )
            .rollouts(num_rollout_workers=4, num_envs_per_worker=5)
            .resources(num_gpus=4, _fake_gpus=True)
        )

        num_iterations = 2

        for _ in framework_iterator(config, with_eager_tracing=True):
            trainer = config.build()
            for i in range(num_iterations):
                results = trainer.train()
                print(results)
                check_train_results(results)
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
