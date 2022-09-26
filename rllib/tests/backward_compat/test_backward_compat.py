import os
from pathlib import Path
import unittest

import ray
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.utils.test_utils import framework_iterator


class TestBackwardCompatibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_old_checkpoint_formats(self):
        """Tests, whether we remain backward compatible (>=2.0.0) wrt checkpoints."""

        rllib_dir = Path(__file__).parent.parent.parent
        print(f"rllib dir={rllib_dir} exists={os.path.isdir(rllib_dir)}")

        for version in ["v0", "v1"]:
            for fw in framework_iterator():
                path_to_checkpoint = os.path.join(
                    rllib_dir,
                    "tests",
                    "backward_compat",
                    "checkpoints",
                    version,
                    "ppo_frozenlake_" + fw,
                )

                print(
                    f"path_to_checkpoint={path_to_checkpoint} "
                    f"exists={os.path.isdir(path_to_checkpoint)}"
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
