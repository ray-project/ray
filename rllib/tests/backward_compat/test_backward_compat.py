import os
from pathlib import Path
from packaging import version
import unittest

import ray
import ray.cloudpickle as pickle
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.ppo import PPO
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.checkpoints import get_checkpoint_info
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

        # TODO: Once checkpoints are python version independent (once we stop using
        #  pickle), add 1.0 here as well.
        for v in ["0.1"]:
            v = version.Version(v)
            for fw in framework_iterator(with_eager_tracing=True):
                path_to_checkpoint = os.path.join(
                    rllib_dir,
                    "tests",
                    "backward_compat",
                    "checkpoints",
                    "v" + str(v),
                    "ppo_frozenlake_" + fw,
                )

                print(
                    f"path_to_checkpoint={path_to_checkpoint} "
                    f"exists={os.path.isdir(path_to_checkpoint)}"
                )

                checkpoint_info = get_checkpoint_info(path_to_checkpoint)
                # v0.1: Need to create algo first, then restore.
                if checkpoint_info["checkpoint_version"] == version.Version("0.1"):
                    # For checkpoints <= v0.1, we need to magically know the original
                    # config used as well as the algo class.
                    with open(checkpoint_info["state_file"], "rb") as f:
                        state = pickle.load(f)
                    worker_state = pickle.loads(state["worker"])
                    algo = PPO(config=worker_state["policy_config"])
                    # Note, we can not use restore() here because the testing
                    # checkpoints are created with Algorithm.save() by
                    # checkpoints/create_checkpoints.py. I.e, they are missing
                    # all the Tune checkpoint metadata.
                    algo.load_checkpoint(path_to_checkpoint)
                # > v0.1: Simply use new `Algorithm.from_checkpoint()` staticmethod.
                else:
                    algo = Algorithm.from_checkpoint(path_to_checkpoint)

                    # Also test restoring a Policy from an algo checkpoint.
                    policies = Policy.from_checkpoint(path_to_checkpoint)
                    assert "default_policy" in policies

                print(algo.train())
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
