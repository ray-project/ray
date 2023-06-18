import csv
from dataclasses import dataclass
import glob
import json
import os
import unittest
import tempfile
from pathlib import Path
from typing import Optional
import shutil
import numpy as np

import ray
from ray.air.constants import (
    EXPR_PARAM_FILE,
    EXPR_PARAM_PICKLE_FILE,
    EXPR_PROGRESS_FILE,
    EXPR_RESULT_FILE,
)
from ray.cloudpickle import cloudpickle
from ray.tune.logger import (
    CSVLoggerCallback,
    JsonLoggerCallback,
    JsonLogger,
    CSVLogger,
    TBXLoggerCallback,
    TBXLogger,
)
from ray.tune.logger.aim import AimLoggerCallback
from ray.tune.utils import flatten_dict


@dataclass
class Trial:
    evaluated_params: dict
    trial_id: str
    logdir: str
    experiment_path: Optional[str] = None
    experiment_dir_name: Optional[str] = None
    remote_checkpoint_dir: Optional[str] = None

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

    @property
    def remote_path(self):
        return self.remote_checkpoint_dir

    def __hash__(self):
        return hash(self.trial_id)

    def get_runner_ip(self) -> str:
        return ray.util.get_node_ip_address()


def result(t, rew, **kwargs):
    results = dict(
        time_total_s=t,
        episode_reward_mean=rew,
        mean_accuracy=rew * 2,
        training_iteration=int(t),
    )
    results.update(kwargs)
    return results


class LoggerSuite(unittest.TestCase):
    """Test built-in loggers."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def testLegacyCSV(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="csv", logdir=self.test_dir)
        logger = CSVLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 5))
        logger.on_result(result(2, 6, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

        self._validate_csv_result()

    def testCSV(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="csv", logdir=self.test_dir)
        logger = CSVLoggerCallback()
        logger.on_trial_result(0, [], t, result(0, 4))
        logger.on_trial_result(1, [], t, result(1, 5))
        logger.on_trial_result(
            2, [], t, result(2, 6, score=[1, 2, 3], hello={"world": 1})
        )

        logger.on_trial_complete(3, [], t)
        self._validate_csv_result()

    def testCSVEmptyHeader(self):
        """Test that starting a trial twice does not lead to empty CSV headers.

        In a previous bug, the CSV header was sometimes missing when a trial
        crashed before reporting results. See
        https://github.com/ray-project/ray/issues/15106
        """
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="csv", logdir=self.test_dir)
        logger = CSVLoggerCallback()
        logger.on_trial_start(0, [], t)
        logger.on_trial_start(0, [], t)
        logger.on_trial_result(1, [], t, result(1, 5))

        with open(os.path.join(self.test_dir, "progress.csv"), "rt") as f:
            csv_contents = f.read()

        csv_lines = csv_contents.split("\n")

        # Assert header has been written to progress.csv
        assert "training_iteration" in csv_lines[0]

    def _validate_csv_result(self):
        results = []
        result_file = os.path.join(self.test_dir, EXPR_PROGRESS_FILE)
        with open(result_file, "rt") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                results.append(row)

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual(
            [int(row["episode_reward_mean"]) for row in results], [4, 5, 6]
        )

    def testJSONLegacyLogger(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="json", logdir=self.test_dir)
        logger = JsonLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(1, 5))
        logger.on_result(result(2, 6, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

        self._validate_json_result(config)

    def testJSON(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="json", logdir=self.test_dir)
        logger = JsonLoggerCallback()
        logger.on_trial_result(0, [], t, result(0, 4))
        logger.on_trial_result(1, [], t, result(1, 5))
        logger.on_trial_result(
            2, [], t, result(2, 6, score=[1, 2, 3], hello={"world": 1})
        )

        logger.on_trial_complete(3, [], t)
        self._validate_json_result(config)

    def _validate_json_result(self, config):
        # Check result logs
        results = []
        result_file = os.path.join(self.test_dir, EXPR_RESULT_FILE)
        with open(result_file, "rt") as fp:
            for row in fp.readlines():
                results.append(json.loads(row))

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual(
            [int(row["episode_reward_mean"]) for row in results], [4, 5, 6]
        )

        # Check json saved config file
        config_file = os.path.join(self.test_dir, EXPR_PARAM_FILE)
        with open(config_file, "rt") as fp:
            loaded_config = json.load(fp)

        self.assertEqual(loaded_config, config)

        # Check pickled config file
        config_file = os.path.join(self.test_dir, EXPR_PARAM_PICKLE_FILE)
        with open(config_file, "rb") as fp:
            loaded_config = cloudpickle.load(fp)

        self.assertEqual(loaded_config, config)

    def testLegacyTBX(self):
        config = {
            "a": 2,
            "b": [1, 2],
            "c": {"c": {"D": 123}},
            "d": np.int64(1),
            "e": np.bool8(True),
            "f": None,
        }
        t = Trial(evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(1, 5))
        logger.on_result(result(2, 6, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

        self._validate_tbx_result()

    def testTBX(self):
        config = {
            "a": 2,
            "b": [1, 2],
            "c": {"c": {"D": 123}},
            "int32": np.int32(1),
            "int64": np.int64(2),
            "bool8": np.bool8(True),
            "float32": np.float32(3),
            "float64": np.float64(4),
            "bad": np.float128(4),
        }
        t = Trial(evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLoggerCallback()
        logger.on_trial_result(0, [], t, result(0, 4))
        logger.on_trial_result(1, [], t, result(1, 5))
        logger.on_trial_result(
            2, [], t, result(2, 6, score=[1, 2, 3], hello={"world": 1})
        )

        logger.on_trial_complete(3, [], t)

        self._validate_tbx_result(
            params=(b"float32", b"float64", b"int32", b"int64", b"bool8"),
            excluded_params=(b"bad",),
        )

    def _validate_tbx_result(self, params=None, excluded_params=None):
        try:
            from tensorflow.python.summary.summary_iterator import summary_iterator
        except ImportError:
            print("Skipping rest of test as tensorflow is not installed.")
            return

        events_file = list(glob.glob(f"{self.test_dir}/events*"))[0]
        results = []
        excluded_params = excluded_params or []
        for event in summary_iterator(events_file):
            for v in event.summary.value:
                if v.tag == "ray/tune/episode_reward_mean":
                    results.append(v.simple_value)
                elif v.tag == "_hparams_/experiment" and params:
                    for key in params:
                        self.assertIn(key, v.metadata.plugin_data.content)
                    for key in excluded_params:
                        self.assertNotIn(key, v.metadata.plugin_data.content)
                elif v.tag == "_hparams_/session_start_info" and params:
                    for key in params:
                        self.assertIn(key, v.metadata.plugin_data.content)
                    for key in excluded_params:
                        self.assertNotIn(key, v.metadata.plugin_data.content)

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual([int(res) for res in results], [4, 5, 6])

    def testLegacyBadTBX(self):
        config = {"b": (1, 2, 3)}
        t = Trial(evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        with self.assertLogs("ray.tune.logger", level="INFO") as cm:
            logger.close()
        assert "INFO" in cm.output[0]

    def testBadTBX(self):
        config = {"b": (1, 2, 3)}
        t = Trial(evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLoggerCallback()
        logger.on_trial_result(0, [], t, result(0, 4))
        logger.on_trial_result(1, [], t, result(1, 5))
        logger.on_trial_result(
            2, [], t, result(2, 6, score=[1, 2, 3], hello={"world": 1})
        )
        with self.assertLogs("ray.tune.logger", level="INFO") as cm:
            logger.on_trial_complete(3, [], t)
        assert "INFO" in cm.output[0]


class AimLoggerSuite(unittest.TestCase):
    """Test Aim integration."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def initialize_logger(self, repo=None, experiment_name=None, metrics=None):
        try:
            from aim import Repo
        except ImportError:
            print("Skipping rest of test as aim is not installed.")
            return

        class Dummy:
            pass

        self.config = {
            "a": 2,
            "b": [1, 2],
            "c": {"d": {"e": 123}},
            "int32": np.int32(1),
            "int64": np.int64(2),
            "bool8": np.bool8(True),
            "float32": np.float32(3),
            "float64": np.float64(4),
            "bad": Dummy(),
        }
        trial_logdir = os.path.join(self.test_dir, "trial_logdir")
        trials = [
            Trial(
                evaluated_params=self.config,
                trial_id="aim_1",
                experiment_path=self.test_dir,
                logdir=trial_logdir,
                experiment_dir_name="aim_test",
                remote_checkpoint_dir="s3://bucket/aim_test/trial_0_logdir",
            ),
            Trial(
                evaluated_params=self.config,
                trial_id="aim_2",
                experiment_path=self.test_dir,
                logdir=trial_logdir,
                experiment_dir_name="aim_test",
                remote_checkpoint_dir="s3://bucket/aim_test/trial_1_logdir",
            ),
        ]

        # Test that aim repo is saved to the experiment directory
        # (one up from the trial directory) as the default.
        # In this example, this is `self.test_dir`.
        repo = repo or self.test_dir
        logger = AimLoggerCallback(
            repo=repo, experiment_name=experiment_name, metrics=metrics
        )

        for i, t in enumerate(trials):
            with self.assertLogs("ray.tune.logger", level="INFO") as cm:
                logger.log_trial_start(t)
                # Check that we log that the "bad" hparam gets thrown away
                assert "INFO" in cm.output[0]

            logger.on_trial_result(0, [], t, result(0, 3 * i + 1))
            logger.on_trial_result(1, [], t, result(1, 3 * i + 2))
            logger.on_trial_result(
                2, [], t, result(2, 3 * i + 3, score=[1, 2, 3], hello={"world": 1})
            )
            logger.on_trial_complete(3, [], t)

        aim_repo = Repo(repo)
        runs = list(aim_repo.iter_runs())
        assert len(runs) == 2
        runs.sort(key=lambda r: r["trial_id"])
        return runs

    def validateLogs(self, runs: list, metrics: list = None):
        expected_logged_hparams = set(flatten_dict(self.config)) - {"bad"}

        for i, run in enumerate(runs):
            assert set(run["hparams"]) == expected_logged_hparams
            assert run.get("trial_remote_log_dir")
            assert run.get("trial_ip")

            results = None
            all_tune_metrics = set()
            for metric in run.metrics():
                if metric.name.startswith("ray/tune/"):
                    all_tune_metrics.add(metric.name.replace("ray/tune/", ""))
                if metric.name == "ray/tune/episode_reward_mean":
                    results = metric.values.values_list()

            assert results
            # Make sure that the set of reported metrics matches with the
            # set of metric names passed in
            # If None is passed in, then all Tune metrics get reported
            assert metrics is None or set(metrics) == all_tune_metrics

            results = [int(res) for res in results]
            if i == 0:
                self.assertSequenceEqual(results, [1, 2, 3])
            elif i == 1:
                self.assertSequenceEqual(results, [4, 5, 6])

    def testDefault(self):
        """Test AimLoggerCallback with default settings.
        - Req: a repo gets created at the experiment-level directory.
        - Req: the experiment param passed into each aim Run is the Tune experiment name
        """
        runs = self.initialize_logger()
        self.validateLogs(runs)
        for run in runs:
            assert run.repo.path == os.path.join(self.test_dir, ".aim")
            assert run.experiment == "aim_test"

    def testFilteredMetrics(self):
        """Test AimLoggerCallback, logging only a subset of metrics."""
        metrics_to_log = ("episode_reward_mean",)
        runs = self.initialize_logger(metrics=metrics_to_log)
        self.validateLogs(runs=runs, metrics=metrics_to_log)

    def testCustomConfigurations(self):
        """Test AimLoggerCallback, setting a custom repo and experiment name."""
        custom_repo = os.path.join(self.test_dir, "custom_repo")
        runs = self.initialize_logger(repo=custom_repo, experiment_name="custom")
        self.validateLogs(runs)
        for run in runs:
            assert run.repo.path == os.path.join(custom_repo, ".aim")
            assert run.experiment == "custom"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
