# coding: utf-8
import itertools
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from ray.tune.result import TRAINING_ITERATION
from ray.tune.checkpoint_manager import _TuneCheckpoint, _CheckpointManager, logger


class CheckpointManagerTest(unittest.TestCase):
    @staticmethod
    def mock_result(metric, i):
        return {"i": metric, TRAINING_ITERATION: i}

    def checkpoint_manager(self, keep_checkpoints_num):
        return _CheckpointManager(keep_checkpoints_num, "i", delete_fn=lambda c: None)

    def testNewestCheckpoint(self):
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        memory_checkpoint = _TuneCheckpoint(
            _TuneCheckpoint.MEMORY, {0}, self.mock_result(0, 0)
        )
        checkpoint_manager.on_checkpoint(memory_checkpoint)
        persistent_checkpoint = _TuneCheckpoint(
            _TuneCheckpoint.PERSISTENT, {1}, self.mock_result(1, 1)
        )
        checkpoint_manager.on_checkpoint(persistent_checkpoint)
        self.assertEqual(
            checkpoint_manager.newest_persistent_checkpoint, persistent_checkpoint
        )

    def testOnCheckpointOrdered(self):
        """
        Tests increasing priorities. Also tests that that the worst checkpoints
        are deleted when necessary.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [
            _TuneCheckpoint(_TuneCheckpoint.PERSISTENT, {i}, self.mock_result(i, i))
            for i in range(3)
        ]

        with patch.object(checkpoint_manager, "delete") as delete_mock:
            for j in range(3):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 2 else 1
                self.assertEqual(delete_mock.call_count, expected_deletes, j)
                self.assertEqual(
                    checkpoint_manager.newest_persistent_checkpoint, checkpoints[j]
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
        checkpoints = [
            _TuneCheckpoint(_TuneCheckpoint.PERSISTENT, {i}, self.mock_result(i, i))
            for i in range(3, -1, -1)
        ]

        with patch.object(checkpoint_manager, "delete") as delete_mock:
            for j in range(0, len(checkpoints)):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 3 else 1
                self.assertEqual(delete_mock.call_count, expected_deletes)
                self.assertEqual(
                    checkpoint_manager.newest_persistent_checkpoint, checkpoints[j]
                )

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertIn(checkpoints[0], best_checkpoints)
        self.assertIn(checkpoints[1], best_checkpoints)

    def testBestCheckpoints(self):
        """
        Tests that the best checkpoints are tracked and ordered correctly.
        """
        keep_checkpoints_num = 4
        checkpoints = [
            _TuneCheckpoint(_TuneCheckpoint.PERSISTENT, i, self.mock_result(i, i))
            for i in range(8)
        ]

        for permutation in itertools.permutations(checkpoints):
            checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)

            for checkpoint in permutation:
                checkpoint_manager.on_checkpoint(checkpoint)

            best_checkpoints = checkpoint_manager.best_checkpoints()
            self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
            for i in range(len(best_checkpoints)):
                self.assertEqual(best_checkpoints[i].value, i + 4)

    def testBestCheckpointsWithNan(self):
        """
        Tests that checkpoints with nan priority are handled correctly.
        """
        keep_checkpoints_num = 2
        checkpoints = [
            _TuneCheckpoint(
                _TuneCheckpoint.PERSISTENT, None, self.mock_result(float("nan"), i)
            )
            for i in range(2)
        ] + [_TuneCheckpoint(_TuneCheckpoint.PERSISTENT, 3, self.mock_result(0, 3))]

        for permutation in itertools.permutations(checkpoints):
            checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
            for checkpoint in permutation:
                checkpoint_manager.on_checkpoint(checkpoint)

            best_checkpoints = checkpoint_manager.best_checkpoints()
            # best_checkpoints is sorted from worst to best
            self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
            self.assertEqual(best_checkpoints[0].value, None)
            self.assertEqual(best_checkpoints[1].value, 3)

    def testBestCheckpointsOnlyNan(self):
        """
        Tests that checkpoints with only nan priority are handled correctly.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [
            _TuneCheckpoint(
                _TuneCheckpoint.PERSISTENT, i, self.mock_result(float("nan"), i)
            )
            for i in range(4)
        ]

        for checkpoint in checkpoints:
            checkpoint_manager.on_checkpoint(checkpoint)

        best_checkpoints = checkpoint_manager.best_checkpoints()
        # best_checkpoints is sorted from worst to best
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertEqual(best_checkpoints[0].value, 2)
        self.assertEqual(best_checkpoints[1].value, 3)

    def testOnCheckpointUnavailableAttribute(self):
        """
        Tests that an error is logged when the associated result of the
        checkpoint has no checkpoint score attribute.
        """
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)

        no_attr_checkpoint = _TuneCheckpoint(_TuneCheckpoint.PERSISTENT, 0, {})
        with patch.object(logger, "error") as log_error_mock:
            checkpoint_manager.on_checkpoint(no_attr_checkpoint)
            log_error_mock.assert_called_once()
            # The newest checkpoint should still be set despite this error.
            self.assertEqual(
                checkpoint_manager.newest_persistent_checkpoint, no_attr_checkpoint
            )

    def testOnMemoryCheckpoint(self):
        checkpoints = [
            _TuneCheckpoint(_TuneCheckpoint.MEMORY, 0, self.mock_result(0, 0)),
            _TuneCheckpoint(_TuneCheckpoint.MEMORY, 0, self.mock_result(0, 0)),
        ]
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        checkpoint_manager.on_checkpoint(checkpoints[0])
        checkpoint_manager.on_checkpoint(checkpoints[1])
        newest = checkpoint_manager.newest_memory_checkpoint

        self.assertEqual(newest, checkpoints[1])
        self.assertEqual(checkpoint_manager.best_checkpoints(), [])

    def testSameCheckpoint(self):
        checkpoint_manager = _CheckpointManager(
            1, "i", delete_fn=lambda c: os.remove(c.value)
        )

        tmpfiles = []
        for i in range(3):
            _, tmpfile = tempfile.mkstemp()
            with open(tmpfile, "wt") as fp:
                fp.write("")
            tmpfiles.append(tmpfile)

        checkpoints = [
            _TuneCheckpoint(
                _TuneCheckpoint.PERSISTENT, tmpfiles[0], self.mock_result(5, 5)
            ),
            _TuneCheckpoint(
                _TuneCheckpoint.PERSISTENT, tmpfiles[1], self.mock_result(10, 10)
            ),
            _TuneCheckpoint(
                _TuneCheckpoint.PERSISTENT, tmpfiles[2], self.mock_result(0, 0)
            ),
            _TuneCheckpoint(
                _TuneCheckpoint.PERSISTENT, tmpfiles[1], self.mock_result(20, 20)
            ),
        ]
        for checkpoint in checkpoints:
            checkpoint_manager.on_checkpoint(checkpoint)
            self.assertTrue(os.path.exists(checkpoint.value))

        for tmpfile in tmpfiles:
            if os.path.exists(tmpfile):
                os.remove(tmpfile)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
