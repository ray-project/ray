from pathlib import Path
import random
import shutil
from tempfile import TemporaryDirectory
import unittest

import ray
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.utils.metrics import LEARNER_RESULTS


class TestCheckpointable(unittest.TestCase):
    """Tests the Checkpointable API."""

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_checkpoint_backward_compatibility(self):
        """Tests backward compat. of checkpoints created with older versions of ray."""

        # Get the directory of the current script
        old_checkpoints_dir = Path(__file__).parent.resolve() / "old_checkpoints"

        from ray.rllib.utils.tests.old_checkpoints.current_config import (
            config as current_config,
        )

        for ray_version_dir in old_checkpoints_dir.iterdir():
            import re

            if not ray_version_dir.is_dir() or not re.search(
                r"\Wray_[0-9_]+$", str(ray_version_dir)
            ):
                continue
            # Unzip checkpoint for that ray version into a temp directory.
            with TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                # Extract the zip file to the temporary directory
                shutil.unpack_archive(ray_version_dir / "checkpoint.zip", temp_path)
                # Restore the algorithm from the (old) msgpack-checkpoint, using the
                # current Ray version's `config` object.
                algo = PPO.from_checkpoint(path=temp_dir, config=current_config)
                learner_res = algo.train()[LEARNER_RESULTS]
                # Assert that the correct per-policy learning rates were used.
                assert (
                    learner_res["p0"]["default_optimizer_learning_rate"] == 0.00005
                    and learner_res["p1"]["default_optimizer_learning_rate"] == 0.0001
                )
                algo.stop()

                # Second experiment: Add all the policies to the config again that were
                # present when the checkpoint was taken and try `from_checkpoint` again.
                expanded_config = current_config.copy(copy_frozen=False)
                all_pols = {"p0", "p1", "p2", "p3"}
                expanded_config.multi_agent(
                    policies=all_pols,
                    # Create some completely new mapping function (that has nothing to
                    # do with the checkpointed one).
                    policy_mapping_fn=(
                        lambda aid, eps, _p=tuple(all_pols), **kw: random.choice(_p)
                    ),
                    policies_to_train=all_pols,
                )
                expanded_config.rl_module(
                    algorithm_config_overrides_per_module={
                        "p2": PPOConfig.overrides(lr=0.002),
                        "p3": PPOConfig.overrides(lr=0.003),
                    }
                )
                algo = PPO.from_checkpoint(path=temp_dir, config=expanded_config)
                learner_res = algo.train()[LEARNER_RESULTS]
                # Assert that the correct per-policy learning rates were used.
                assert (
                    learner_res["p0"]["default_optimizer_learning_rate"] == 0.00005
                    and learner_res["p1"]["default_optimizer_learning_rate"] == 0.0001
                    and learner_res["p2"]["default_optimizer_learning_rate"] == 0.002
                    and learner_res["p3"]["default_optimizer_learning_rate"] == 0.003
                )
                algo.stop()

        print(f"Algorithm restored and trained once. Learner results={learner_res}.")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
