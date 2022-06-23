# coding: utf-8
import itertools
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from ray.tune.result import TRAINING_ITERATION
from ray.tune.execution.checkpoint_manager import _CheckpointManager
from ray.util.ml_utils.checkpoint_manager import (
    _TrackedCheckpoint,
    logger,
    CheckpointStorage,
)


class CheckpointManagerTest(unittest.TestCase):
    @staticmethod
    def mock_result(metric, i):
        return {"i": metric, TRAINING_ITERATION: i}

    def checkpoint_manager(self, keep_checkpoints_num):
        return _CheckpointManager(
            keep_checkpoints_num=keep_checkpoints_num,
            checkpoint_score_attr="i",
            delete_fn=lambda c: None,
        )

    def testNewestCheckpoint(self):
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        memory_checkpoint = _TrackedCheckpoint(
            dir_or_data={"a": 0},
            storage_mode=CheckpointStorage.MEMORY,
            metrics=self.mock_result(0, 0),
        )
        checkpoint_manager.on_checkpoint(memory_checkpoint)
        persistent_checkpoint = _TrackedCheckpoint(
            dir_or_data={"a": 1},
            storage_mode=CheckpointStorage.PERSISTENT,
            metrics=self.mock_result(1, 1),
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
            _TrackedCheckpoint(
                dir_or_data={i},
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(i, i),
            )
            for i in range(3)
        ]

        with patch.object(
            checkpoint_manager, "_delete_persisted_checkpoint"
        ) as delete_mock:
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
            _TrackedCheckpoint(
                dir_or_data={i},
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(i, i),
            )
            for i in range(3, -1, -1)
        ]

        with patch.object(
            checkpoint_manager, "_delete_persisted_checkpoint"
        ) as delete_mock:
            for j in range(0, len(checkpoints)):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 3 else 1
                self.assertEqual(
                    delete_mock.call_count,
                    expected_deletes,
                    msg=f"Called {delete_mock.call_count} times",
                )
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
            _TrackedCheckpoint(
                dir_or_data=i,
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(i, i),
            )
            for i in range(8)
        ]

        for permutation in itertools.permutations(checkpoints):
            checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)

            for checkpoint in permutation:
                checkpoint_manager.on_checkpoint(checkpoint)

            best_checkpoints = checkpoint_manager.best_checkpoints()
            self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
            for i in range(len(best_checkpoints)):
                self.assertEqual(best_checkpoints[i].dir_or_data, i + 4)

    def testBestCheckpointsWithNan(self):
        """
        Tests that checkpoints with nan priority are handled correctly.
        """
        keep_checkpoints_num = 2
        checkpoints = [
            _TrackedCheckpoint(
                dir_or_data=None,
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(float("nan"), i),
            )
            for i in range(2)
        ] + [
            _TrackedCheckpoint(
                dir_or_data=3,
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(0, 3),
            )
        ]

        for permutation in itertools.permutations(checkpoints):
            checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
            for checkpoint in permutation:
                checkpoint_manager.on_checkpoint(checkpoint)

            best_checkpoints = checkpoint_manager.best_checkpoints()
            # best_checkpoints is sorted from worst to best
            self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
            self.assertEqual(best_checkpoints[0].dir_or_data, None)
            self.assertEqual(best_checkpoints[1].dir_or_data, 3)

    def testBestCheckpointsOnlyNan(self):
        """
        Tests that checkpoints with only nan priority are handled correctly.
        """
        keep_checkpoints_num = 2
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num)
        checkpoints = [
            _TrackedCheckpoint(
                dir_or_data=i,
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(float("nan"), i),
            )
            for i in range(4)
        ]

        for checkpoint in checkpoints:
            checkpoint_manager.on_checkpoint(checkpoint)

        best_checkpoints = checkpoint_manager.best_checkpoints()
        # best_checkpoints is sorted from worst to best
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertEqual(best_checkpoints[0].dir_or_data, 2)
        self.assertEqual(best_checkpoints[1].dir_or_data, 3)

    def testOnCheckpointUnavailableAttribute(self):
        """
        Tests that an error is logged when the associated result of the
        checkpoint has no checkpoint score attribute.
        """
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)

        no_attr_checkpoint = _TrackedCheckpoint(
            dir_or_data=0,
            storage_mode=CheckpointStorage.PERSISTENT,
            metrics={},
        )

        with patch.object(logger, "error") as log_error_mock:
            checkpoint_manager.on_checkpoint(no_attr_checkpoint)
            log_error_mock.assert_called_once()
            # The newest checkpoint should still be set despite this error.
            self.assertEqual(
                checkpoint_manager.newest_persistent_checkpoint, no_attr_checkpoint
            )

    def testOnMemoryCheckpoint(self):
        checkpoints = [
            _TrackedCheckpoint(
                dir_or_data={"a": 0},
                storage_mode=CheckpointStorage.MEMORY,
                metrics=self.mock_result(0, 0),
            ),
            _TrackedCheckpoint(
                dir_or_data={"a": 0},
                storage_mode=CheckpointStorage.MEMORY,
                metrics=self.mock_result(0, 0),
            ),
        ]
        checkpoint_manager = self.checkpoint_manager(keep_checkpoints_num=1)
        checkpoint_manager.on_checkpoint(checkpoints[0])
        checkpoint_manager.on_checkpoint(checkpoints[1])
        newest = checkpoint_manager.newest_memory_checkpoint

        self.assertEqual(newest, checkpoints[1])
        self.assertEqual(checkpoint_manager.best_checkpoints(), [])

    def testSameCheckpoint(self):
        checkpoint_manager = _CheckpointManager(
            keep_checkpoints_num=1,
            checkpoint_score_attr="i",
            delete_fn=lambda c: os.remove(c.dir_or_data),
        )

        tmpfiles = []
        for i in range(3):
            _, tmpfile = tempfile.mkstemp()
            with open(tmpfile, "wt") as fp:
                fp.write("")
            tmpfiles.append(tmpfile)

        checkpoints = [
            _TrackedCheckpoint(
                dir_or_data=tmpfiles[0],
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(5, 5),
            ),
            _TrackedCheckpoint(
                dir_or_data=tmpfiles[1],
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(10, 10),
            ),
            _TrackedCheckpoint(
                dir_or_data=tmpfiles[2],
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(0, 0),
            ),
            _TrackedCheckpoint(
                dir_or_data=tmpfiles[1],
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=self.mock_result(20, 20),
            ),
        ]
        for checkpoint in checkpoints:
            checkpoint_manager.on_checkpoint(checkpoint)
            self.assertTrue(os.path.exists(checkpoint.dir_or_data))

        for tmpfile in tmpfiles:
            if os.path.exists(tmpfile):
                os.remove(tmpfile)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
