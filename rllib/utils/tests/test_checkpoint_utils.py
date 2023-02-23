import os
from pathlib import Path
import tempfile
import unittest

import ray
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.simple_q import SimpleQConfig
from ray.rllib.utils.checkpoints import get_checkpoint_info, create_msgpack_checkpoint
from ray.rllib.utils.test_utils import check


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

    def test_msgpack_checkpoint_translation(self):
        """Tests, whether a checkpoint can be translated into a msgpack-checkpoint."""
        config = SimpleQConfig().environment("CartPole-v1")
        algo1 = config.build()
        pickle_state = algo1.__getstate__()
        # Create standard pickle-based checkpoint.
        with tempfile.TemporaryDirectory() as pickle_cp_dir:
            pickle_cp_dir = algo1.save(checkpoint_dir=pickle_cp_dir)
            # Convert pickle checkpoint to msgpack.
            with tempfile.TemporaryDirectory() as msgpack_cp_dir:
                create_msgpack_checkpoint(pickle_cp_dir, msgpack_cp_dir)
                algo2 = Algorithm.from_checkpoint(msgpack_cp_dir)
        msgpack_state = algo2.__getstate__()

        check(pickle_state["config"], msgpack_state["config"])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
