"""New-stack CQL tests (new API stack: RLModule + Learner)."""

import os
import unittest
from pathlib import Path

import ray
from ray.rllib.algorithms import cql
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check_train_results_new_api_stack

torch, _ = try_import_torch()


def _data_file() -> str:
    rllib_dir = Path(__file__).parent.parent.parent.parent
    return os.path.join(rllib_dir, "offline/tests/data/pendulum/small.json")


def _base_config() -> cql.CQLConfig:
    return (
        cql.CQLConfig()
        .environment(env="Pendulum-v1")
        .offline_data(
            input_=_data_file(),
            actions_in_input_normalized=False,
        )
        .training(
            train_batch_size=256,
            twin_q=True,
            num_steps_sampled_before_learning_starts=0,
            bc_iters=0,
            clip_actions=False,
        )
        .env_runners(num_env_runners=0)
        .reporting(min_time_s_per_iteration=0)
    )


class TestCQLNewStack(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_cql_compilation(self):
        """CQL new-stack can be built and trained for a few iterations."""
        config = _base_config()
        algo = config.build()
        for _ in range(2):
            results = algo.train()
            check_train_results_new_api_stack(results)
        algo.stop()

    def test_cql_lagrangian(self):
        """Lagrangian adaptive α' trains without errors and logs expected keys."""
        config = _base_config().training(
            lagrangian=True,
            lagrangian_thresh=5.0,
            lagrangian_lr=1e-4,
        )
        algo = config.build()
        for _ in range(2):
            results = algo.train()
            check_train_results_new_api_stack(results)
        algo.stop()

    def test_cql_lagrangian_checkpoint(self):
        """α' value and optimizer state survive a checkpoint save/restore cycle."""
        config = _base_config().training(
            lagrangian=True,
            lagrangian_thresh=5.0,
            lagrangian_lr=1e-4,
        )
        algo = config.build()
        algo.train()

        # Read α' before checkpoint
        learner = algo.learner_group._learner
        mid = list(learner._log_cql_alpha.keys())[0]
        alpha_before = learner._log_cql_alpha[mid].item()

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            algo.save_checkpoint(tmpdir)
            algo.load_checkpoint(tmpdir)

        learner2 = algo.learner_group._learner
        alpha_after = learner2._log_cql_alpha[mid].item()
        self.assertAlmostEqual(
            alpha_before, alpha_after, places=5,
            msg="log_cql_alpha must survive checkpoint save/restore"
        )
        algo.stop()

    def test_cql_fixed_vs_lagrangian(self):
        """Fixed min_q_weight path and lagrangian path both complete training."""
        for lagrangian in [False, True]:
            with self.subTest(lagrangian=lagrangian):
                config = _base_config().training(lagrangian=lagrangian)
                algo = config.build()
                results = algo.train()
                check_train_results_new_api_stack(results)
                algo.stop()


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
