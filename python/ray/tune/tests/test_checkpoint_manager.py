# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import sys
import unittest

from ray.tune.checkpoint_manager import Checkpoint, CheckpointManager, logger

if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


class CheckpointManagerTest(unittest.TestCase):
    @staticmethod
    def mock_result(i):
        return {"i": i}

    def testOnCheckpointOrdered(self):
        """
        Tests increasing priorities. Also tests that that the worst checkpoints
        are deleted when necessary.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = CheckpointManager(keep_checkpoints_num, "i")
        checkpoints = [
            Checkpoint(Checkpoint.DISK, {i}, self.mock_result(i))
            for i in range(3)
        ]

        with patch("shutil.rmtree") as rmtree_mock, patch("os.path"):
            for j in range(3):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 2 else 1
                self.assertEqual(rmtree_mock.call_count, expected_deletes)
                self.assertEqual(checkpoint_manager.newest_checkpoint,
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
        checkpoint_manager = CheckpointManager(keep_checkpoints_num, "i")
        checkpoints = [
            Checkpoint(Checkpoint.DISK, {i}, self.mock_result(i))
            for i in range(3, -1, -1)
        ]

        with patch("shutil.rmtree") as rmtree_mock, patch("os.path"):
            for j in range(0, len(checkpoints)):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 3 else 1
                self.assertEqual(rmtree_mock.call_count, expected_deletes)
                self.assertEqual(checkpoint_manager.newest_checkpoint,
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
        checkpoint_manager = CheckpointManager(keep_checkpoints_num, "i")
        checkpoints = [
            Checkpoint(Checkpoint.MEMORY, i, self.mock_result(i))
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
        keep_checkpoints_num = 1
        checkpoint_manager = CheckpointManager(keep_checkpoints_num, "i")

        no_attr_checkpoint = Checkpoint(Checkpoint.MEMORY, 0, {})
        with patch.object(logger, "error") as log_error_mock:
            checkpoint_manager.on_checkpoint(no_attr_checkpoint)
            log_error_mock.assert_called_once()
            # The newest checkpoint should still be set despite this error.
            assert checkpoint_manager.newest_checkpoint == no_attr_checkpoint


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
