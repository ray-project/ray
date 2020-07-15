# coding: utf-8
import random
import sys
import unittest
from unittest.mock import patch

from ray.tune.result import TRAINING_ITERATION
from ray.tune.checkpoint_manager import Checkpoint, CheckpointManager, logger


class CheckpointManagerTest(unittest.TestCase):
    @staticmethod
    def mock_result(i):
        return {"i": i, TRAINING_ITERATION: i}

    def checkpoint_manager(self, keep_checkpoints_num):
        return CheckpointManager(
            keep_checkpoints_num, "i", delete_fn=lambda c: None)

    def testNewestCheckpoint(self):
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=2)

        # Add persistent checkpoint A, should be newest
        persistent_checkpoint_a = Checkpoint(Checkpoint.PERSISTENT, {1},
                                             self.mock_result(1))
        checkpoint_manager.on_checkpoint(persistent_checkpoint_a)
        self.assertEqual(checkpoint_manager.newest_checkpoint,
                         persistent_checkpoint_a)

        # Add memory checkpoint X, should be newest
        memory_checkpoint_x = Checkpoint(Checkpoint.MEMORY, {2},
                                         self.mock_result(2))
        checkpoint_manager.on_checkpoint(memory_checkpoint_x)
        self.assertEqual(checkpoint_manager.newest_checkpoint,
                         memory_checkpoint_x)

        # Add persistent checkpoint B, should be newest
        persistent_checkpoint_b = Checkpoint(Checkpoint.PERSISTENT, {3},
                                             self.mock_result(3))
        checkpoint_manager.on_checkpoint(persistent_checkpoint_b)
        self.assertEqual(checkpoint_manager.newest_checkpoint,
                         persistent_checkpoint_b)

        # Add bad performing (or older) persistent checkpoint C
        # B should remain newest
        persistent_checkpoint_c = Checkpoint(Checkpoint.PERSISTENT, {0},
                                             self.mock_result(0))
        checkpoint_manager.on_checkpoint(persistent_checkpoint_c)
        self.assertEqual(checkpoint_manager.newest_checkpoint,
                         persistent_checkpoint_b)

        # Delete checkpoint B, so X should be newest
        checkpoint_manager._membership.remove(persistent_checkpoint_b)
        checkpoint_manager._best_checkpoints.pop(1)
        checkpoint_manager._delete(persistent_checkpoint_b, remove_dir=False)
        self.assertEqual(checkpoint_manager.newest_checkpoint,
                         memory_checkpoint_x)

    def testOnCheckpointOrdered(self):
        """
        Tests increasing priorities. Also tests that that the worst checkpoints
        are deleted when necessary.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [
            Checkpoint(Checkpoint.PERSISTENT, {i}, self.mock_result(i))
            for i in range(3)
        ]

        with patch.object(checkpoint_manager, "delete_fn") as delete_mock:
            for j in range(3):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 2 else 1
                self.assertEqual(delete_mock.call_count, expected_deletes, j)
                self.assertEqual(
                    checkpoint_manager.newest_persistent_checkpoint,
                    checkpoints[j])

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertIn(checkpoints[1], best_checkpoints)
        self.assertIn(checkpoints[2], best_checkpoints)

    def testOnCheckpointUnordered(self):
        """
        Tests priorities that aren't inserted in ascending order. Also tests
        that the worst checkpoints are deleted when necessary.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [
            Checkpoint(Checkpoint.PERSISTENT, {i}, self.mock_result(i))
            for i in range(3, -1, -1)
        ]

        with patch.object(checkpoint_manager, "delete") as delete_mock:
            for j in range(0, len(checkpoints)):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 3 else 1
                self.assertEqual(delete_mock.call_count, expected_deletes)
                self.assertEqual(
                    checkpoint_manager.newest_persistent_checkpoint,
                    checkpoints[j])

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertIn(checkpoints[0], best_checkpoints)
        self.assertIn(checkpoints[1], best_checkpoints)

    def testBestCheckpoints(self):
        """
        Tests that the best checkpoints are tracked and ordered correctly.
        """
        keep_checkpoints_num = 4
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [
            Checkpoint(Checkpoint.PERSISTENT, i, self.mock_result(i))
            for i in range(16)
        ]
        random.shuffle(checkpoints)

        for checkpoint in checkpoints:
            checkpoint_manager.on_checkpoint(checkpoint)

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        for i in range(len(best_checkpoints)):
            self.assertEqual(best_checkpoints[i].value, i + 12)

    def testOnCheckpointUnavailableAttribute(self):
        """
        Tests that an error is logged when the associated result of the
        checkpoint has no checkpoint score attribute.
        """
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)

        no_attr_checkpoint = Checkpoint(Checkpoint.PERSISTENT, 0, {})
        with patch.object(logger, "error") as log_error_mock:
            checkpoint_manager.on_checkpoint(no_attr_checkpoint)
            log_error_mock.assert_called_once()
            # The newest checkpoint should still be set despite this error.
            self.assertEqual(checkpoint_manager.newest_persistent_checkpoint,
                             no_attr_checkpoint)

    def testOnMemoryCheckpoint(self):
        checkpoints = [
            Checkpoint(Checkpoint.MEMORY, 0, self.mock_result(0)),
            Checkpoint(Checkpoint.MEMORY, 0, self.mock_result(0))
        ]
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        checkpoint_manager.on_checkpoint(checkpoints[0])
        checkpoint_manager.on_checkpoint(checkpoints[1])
        newest = checkpoint_manager.newest_memory_checkpoint

        self.assertEqual(newest, checkpoints[1])
        self.assertEqual(checkpoint_manager.best_checkpoints(), [])


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
