import os
from pathlib import Path
import tempfile
import unittest

import ray
import ray.cloudpickle as pickle
from ray.air.checkpoint import Checkpoint
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPO
from ray.rllib.utils.test_utils import framework_iterator


class TestBackwardCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_register_all(self):
        """Tests the old (1.10) way of registering all Trainers.

        Uses the old 1.10 registry.py file and thus makes sure all Trainers can still
        be imported using their old paths (albeit this will create a warning).
        """
        # Try importing old Trainer class (this is just an Alias now to the `Algorithm`
        # class).
        from ray.rllib.agents.trainer import Trainer  # noqa

        # Old registry code.
        from ray.rllib.tests.backward_compat.old_registry import (
            ALGORITHMS,
            _get_trainer_class,
        )
        from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS

        # Test the old `_get_trainer_class()` utility that was used to pull Trainer
        # class and default config.
        for key in (
            list(ALGORITHMS.keys())
            + list(CONTRIBUTED_ALGORITHMS.keys())
            + ["__fake", "__sigmoid_fake_data", "__parameter_tuning"]
        ):
            _get_trainer_class(key)

    def test_old_configs(self):
        """Tests creating various Trainers (Algorithms) using 1.10 config dicts."""
        from ray.rllib.tests.backward_compat.old_ppo import DEFAULT_CONFIG
        from ray.rllib.agents.ppo import PPOTrainer

        config = DEFAULT_CONFIG.copy()
        trainer = PPOTrainer(config=config, env="CartPole-v0")
        trainer.train()
        trainer.stop()

    def test_old_checkpoint_formats(self):
        """Tests, whether we remain backward compatible (>=2.0.0) wrt checkpoints."""
        for version in ["v0", "v1"]:
            for fw in framework_iterator():
                path_to_checkpoint = os.path.join(
                    str(Path.cwd()),
                    "checkpoints",
                    version,
                    "ppo_frozenlake_" + fw,
                )

                # Should work for both old ("v0") and newer versions of checkpoints:
                # Simply use new Algorithm.from_checkpoint staticmethod.
                algo = Algorithm.from_checkpoint(path_to_checkpoint)

                print(algo.train())
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
