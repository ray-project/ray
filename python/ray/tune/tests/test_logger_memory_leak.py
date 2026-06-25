import csv
import json
import os
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest

from ray.air.constants import EXPR_PARAM_FILE, EXPR_PROGRESS_FILE, EXPR_RESULT_FILE
from ray.tune.logger import CSVLoggerCallback, JsonLoggerCallback


@dataclass
class Trial:
    evaluated_params: dict
    trial_id: str
    logdir: str
    experiment_path: Optional[str] = None
    experiment_dir_name: Optional[str] = None
    path: Optional[str] = None
    checkpoint: Optional[object] = None

    @property
    def config(self):
        return self.evaluated_params

    def init_local_path(self):
        return

    @property
    def local_path(self):
        if self.logdir:
            return self.logdir
        if not self.experiment_dir_name:
            return None
        return str(Path(self.experiment_path) / self.experiment_dir_name)

    @property
    def local_experiment_path(self):
        return self.experiment_path

    def __hash__(self):
        return hash(self.trial_id)

    def get_ray_actor_ip(self) -> str:
        return "127.0.0.1"


def result(t, rew, **kwargs):
    results = dict(
        time_total_s=t,
        episode_reward_mean=rew,
        mean_accuracy=rew * 2,
        training_iteration=int(t),
    )
    results.update(kwargs)
    return results


class TestLoggerMemoryLeak(unittest.TestCase):
    """Test that logger callbacks properly clean up trial state on end.

    These tests verify the fixes for GitHub issue #64231 where running
    thousands of trials caused linear RSS growth because logger callbacks
    retained per-trial dictionaries after trial completion.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_csv_logger_cleans_up_trial_continue(self):
        """CSVLoggerCallback._trial_continue should be empty after all trials end."""
        logger = CSVLoggerCallback()
        trials = []
        for i in range(10):
            config = {"a": i, "b": i * 2}
            t = Trial(
                evaluated_params=config,
                trial_id=f"csv_{i}",
                logdir=os.path.join(self.test_dir, f"trial_{i}"),
            )
            trials.append(t)
            logger.log_trial_result(0, t, result(0, i))
            logger.log_trial_result(1, t, result(1, i + 1))
            logger.log_trial_end(t, failed=False)

        # After all trials end, _trial_continue should be empty
        self.assertEqual(len(logger._trial_continue), 0)
        self.assertEqual(len(logger._trial_files), 0)
        self.assertEqual(len(logger._trial_csv), 0)

    def test_csv_logger_cleans_up_on_failed_trial(self):
        """CSVLoggerCallback should clean up even when a trial fails."""
        logger = CSVLoggerCallback()
        config = {"a": 1}
        t = Trial(
            evaluated_params=config,
            trial_id="csv_fail",
            logdir=os.path.join(self.test_dir, "trial_fail"),
        )
        logger.log_trial_result(0, t, result(0, 4))
        logger.log_trial_end(t, failed=True)

        self.assertEqual(len(logger._trial_continue), 0)
        self.assertEqual(len(logger._trial_files), 0)
        self.assertEqual(len(logger._trial_csv), 0)

    def test_json_logger_cleans_up_trial_configs(self):
        """JsonLoggerCallback._trial_configs should be empty after all trials end."""
        logger = JsonLoggerCallback()
        trials = []
        for i in range(10):
            config = {"a": i, "b": i * 2}
            t = Trial(
                evaluated_params=config,
                trial_id=f"json_{i}",
                logdir=os.path.join(self.test_dir, f"trial_{i}"),
            )
            trials.append(t)
            logger.log_trial_result(0, t, result(0, i))
            logger.log_trial_result(1, t, result(1, i + 1))
            logger.log_trial_end(t, failed=False)

        # After all trials end, _trial_configs should be empty
        self.assertEqual(len(logger._trial_configs), 0)
        self.assertEqual(len(logger._trial_files), 0)

    def test_json_logger_cleans_up_on_failed_trial(self):
        """JsonLoggerCallback should clean up even when a trial fails."""
        logger = JsonLoggerCallback()
        config = {"a": 1}
        t = Trial(
            evaluated_params=config,
            trial_id="json_fail",
            logdir=os.path.join(self.test_dir, "trial_fail"),
        )
        logger.log_trial_result(0, t, result(0, 4))
        logger.log_trial_end(t, failed=True)

        self.assertEqual(len(logger._trial_configs), 0)
        self.assertEqual(len(logger._trial_files), 0)

    def test_csv_logger_files_still_written_correctly(self):
        """Cleanup should not affect the actual CSV file output."""
        config = {"a": 2, "b": 5}
        t = Trial(
            evaluated_params=config,
            trial_id="csv_file",
            logdir=self.test_dir,
        )
        logger = CSVLoggerCallback()
        logger.log_trial_result(0, t, result(0, 4))
        logger.log_trial_result(1, t, result(1, 5))
        logger.log_trial_end(t, failed=False)

        result_file = os.path.join(self.test_dir, EXPR_PROGRESS_FILE)
        self.assertTrue(os.path.exists(result_file))

        with open(result_file, "rt") as fp:
            reader = csv.DictReader(fp)
            rows = list(reader)

        self.assertEqual(len(rows), 2)
        self.assertEqual(int(rows[0]["episode_reward_mean"]), 4)
        self.assertEqual(int(rows[1]["episode_reward_mean"]), 5)

    def test_json_logger_files_still_written_correctly(self):
        """Cleanup should not affect the actual JSON file output."""
        config = {"a": 2, "b": 5}
        t = Trial(
            evaluated_params=config,
            trial_id="json_file",
            logdir=self.test_dir,
        )
        logger = JsonLoggerCallback()
        logger.log_trial_result(0, t, result(0, 4))
        logger.log_trial_result(1, t, result(1, 5))
        logger.log_trial_end(t, failed=False)

        result_file = os.path.join(self.test_dir, EXPR_RESULT_FILE)
        self.assertTrue(os.path.exists(result_file))

        with open(result_file, "rt") as fp:
            rows = [json.loads(line) for line in fp.readlines()]

        self.assertEqual(len(rows), 2)
        self.assertEqual(int(rows[0]["episode_reward_mean"]), 4)
        self.assertEqual(int(rows[1]["episode_reward_mean"]), 5)

        # Config should still be written
        config_file = os.path.join(self.test_dir, EXPR_PARAM_FILE)
        self.assertTrue(os.path.exists(config_file))
        with open(config_file, "rt") as fp:
            loaded_config = json.load(fp)
        self.assertEqual(loaded_config, config)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
