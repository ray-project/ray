import os
from pathlib import Path
import tempfile
import unittest

import ray
from ray.rllib.utils.checkpoints import get_checkpoint_info


class TestCheckpointUtils(unittest.TestCase):
    """Tests utilities helping with Checkpoint management."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_get_checkpoint_info_v0_1(self):
        # Create a simple (dummy) v0.1 Algorithm checkpoint.
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            # Old checkpoint-[iter] file.
            algo_state_file = os.path.join(checkpoint_dir, "checkpoint-000100")
            Path(algo_state_file).touch()

            info = get_checkpoint_info(checkpoint_dir)
            self.assertTrue(info["type"] == "Algorithm")
            self.assertTrue(str(info["checkpoint_version"]) == "0.1")
            self.assertTrue(info["checkpoint_dir"] == checkpoint_dir)
            self.assertTrue(info["state_file"] == algo_state_file)
            self.assertTrue(info["policy_ids"] is None)

    def test_get_checkpoint_info_v1_0(self):
        # Create a simple (dummy) v1.0 Algorithm checkpoint.
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            # algorithm_state.pkl
            algo_state_file = os.path.join(checkpoint_dir, "algorithm_state.pkl")
            Path(algo_state_file).touch()
            # 2 policies
            pol1_dir = os.path.join(checkpoint_dir, "policies", "pol1")
            os.makedirs(pol1_dir)
            pol2_dir = os.path.join(checkpoint_dir, "policies", "pol2")
            os.makedirs(pol2_dir)
            # policy_state.pkl
            Path(os.path.join(pol1_dir, "policy_state.pkl")).touch()
            Path(os.path.join(pol2_dir, "policy_state.pkl")).touch()

            info = get_checkpoint_info(checkpoint_dir)
            self.assertTrue(info["type"] == "Algorithm")
            self.assertTrue(str(info["checkpoint_version"]) == "1.0")
            self.assertTrue(info["checkpoint_dir"] == checkpoint_dir)
            self.assertTrue(info["state_file"] == algo_state_file)
            self.assertTrue(
                "pol1" in info["policy_ids"] and "pol2" in info["policy_ids"]
            )

    def test_get_policy_checkpoint_info_v1_0(self):
        # Create a simple (dummy) v1.0 Policy checkpoint.
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            # Old checkpoint-[iter] file.
            policy_state_file = os.path.join(checkpoint_dir, "policy_state.pkl")
            Path(policy_state_file).touch()

            info = get_checkpoint_info(checkpoint_dir)
            self.assertTrue(info["type"] == "Policy")
            self.assertTrue(str(info["checkpoint_version"]) == "1.0")
            self.assertTrue(info["checkpoint_dir"] == checkpoint_dir)
            self.assertTrue(info["state_file"] == policy_state_file)
            self.assertTrue(info["policy_ids"] is None)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
