# coding: utf-8
import os
import random
import sys
import tempfile
import unittest
from unittest.mock import patch

from ray.tune.result import TRAINING_ITERATION
from ray.tune.checkpoint_manager import (
    CheckpointManager,
    logger,
)
from ray.util.ml_utils.checkpoint import LocalStorageCheckpoint, DataCheckpoint


class CheckpointManagerTest(unittest.TestCase):
    @staticmethod
    def mock_result(i):
        return {"i": i, TRAINING_ITERATION: i}

    def checkpoint_manager(self, keep_checkpoints_num):
        return CheckpointManager(keep_checkpoints_num, "i", delete_fn=lambda c: None)

    def testNewestCheckpoint(self):
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        memory_checkpoint = DataCheckpoint(data={0})
        checkpoint_manager.on_checkpoint(memory_checkpoint, self.mock_result(0))
        persistent_checkpoint = LocalStorageCheckpoint(path={1})
        checkpoint_manager.on_checkpoint(persistent_checkpoint, self.mock_result(1))
        self.assertEqual(
            checkpoint_manager.newest_persistent_wrapped_checkpoint.value,
            persistent_checkpoint,
        )

    def testOnCheckpointOrdered(self):
        """
        Tests increasing priorities. Also tests that that the worst checkpoints
        are deleted when necessary.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [LocalStorageCheckpoint(path=i) for i in range(3)]

        with patch.object(checkpoint_manager, "delete") as delete_mock:
            for j in range(3):
                checkpoint_manager.on_checkpoint(checkpoints[j], self.mock_result(j))
                expected_deletes = 0 if j != 2 else 1
                self.assertEqual(delete_mock.call_count, expected_deletes, j)
                self.assertEqual(
                    checkpoint_manager.newest_persistent_wrapped_checkpoint.value,
                    checkpoints[j],
                )

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
        checkpoints_results = [
            (LocalStorageCheckpoint(path=i), self.mock_result(i))
            for i in range(3, -1, -1)
        ]

        with patch.object(checkpoint_manager, "delete") as delete_mock:
            for j, (checkpoint, result) in enumerate(checkpoints_results):
                checkpoint_manager.on_checkpoint(checkpoint, result)
                expected_deletes = 0 if j != 3 else 1
                self.assertEqual(delete_mock.call_count, expected_deletes)
                self.assertEqual(
                    checkpoint_manager.newest_persistent_wrapped_checkpoint.value,
                    checkpoint,
                )

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertIn(checkpoints_results[0][0], best_checkpoints)
        self.assertIn(checkpoints_results[1][0], best_checkpoints)

    def testBestCheckpoints(self):
        """
        Tests that the best checkpoints are tracked and ordered correctly.
        """
        keep_checkpoints_num = 4
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoint_results = [
            (LocalStorageCheckpoint(path=i), self.mock_result(i)) for i in range(16)
        ]
        random.shuffle(checkpoint_results)

        for checkpoint, result in checkpoint_results:
            checkpoint_manager.on_checkpoint(checkpoint, result)

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        for i in range(len(best_checkpoints)):
            self.assertEqual(best_checkpoints[i].path, i + 12)

    def testOnCheckpointUnavailableAttribute(self):
        """
        Tests that an error is logged when the associated result of the
        checkpoint has no checkpoint score attribute.
        """
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)

        no_attr_checkpoint = LocalStorageCheckpoint(path=0)
        with patch.object(logger, "error") as log_error_mock:
            checkpoint_manager.on_checkpoint(no_attr_checkpoint, {})
            log_error_mock.assert_called_once()
            # The newest checkpoint should still be set despite this error.
            self.assertEqual(
                checkpoint_manager.newest_persistent_wrapped_checkpoint.value,
                no_attr_checkpoint,
            )

    def testOnMemoryCheckpoint(self):
        checkpoints = [
            DataCheckpoint(0),
            DataCheckpoint(0),
        ]
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        checkpoint_manager.on_checkpoint(checkpoints[0], self.mock_result(0))
        checkpoint_manager.on_checkpoint(checkpoints[1], self.mock_result(0))
        newest = checkpoint_manager.newest_memory_checkpoint

        self.assertEqual(newest, checkpoints[1])
        self.assertEqual(checkpoint_manager.best_checkpoints(), [])

    def testSameCheckpoint(self):
        checkpoint_manager = CheckpointManager(
            1, "i", delete_fn=lambda c: os.remove(c.path)
        )

        tmpfiles = []
        for i in range(3):
            _, tmpfile = tempfile.mkstemp()
            with open(tmpfile, "wt") as fp:
                fp.write("")
            tmpfiles.append(tmpfile)

        checkpoints_results = [
            (LocalStorageCheckpoint(path=tmpfiles[0]), self.mock_result(5)),
            (LocalStorageCheckpoint(path=tmpfiles[1]), self.mock_result(10)),
            (LocalStorageCheckpoint(path=tmpfiles[2]), self.mock_result(0)),
            (LocalStorageCheckpoint(path=tmpfiles[1]), self.mock_result(20)),
        ]
        for checkpoint, result in checkpoints_results:
            checkpoint_manager.on_checkpoint(checkpoint, result)
            self.assertTrue(os.path.exists(checkpoint.path))

        for tmpfile in tmpfiles:
            if os.path.exists(tmpfile):
                os.remove(tmpfile)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
