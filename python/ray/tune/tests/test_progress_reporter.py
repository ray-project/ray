import collections
import os
import regex as re
import unittest
from unittest.mock import MagicMock, Mock, patch

import pytest

from ray import tune
from ray._private.test_utils import run_string_as_driver
from ray.tune.progress_reporter import (
    CLIReporter,
    JupyterNotebookReporter,
    ProgressReporter,
    _fair_filter_trials,
    _best_trial_str,
    _detect_reporter,
    _time_passed_str,
    _trial_progress_str,
    TuneReporterBase,
    _max_len,
)
from ray.tune.result import AUTO_RESULT_KEYS
from ray.tune.trial import Trial

EXPECTED_RESULT_1 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+-----+------------+
|   Trial name | status     | loc   |   a |   b |   metric_1 |
|--------------+------------+-------+-----+-----+------------|
|        00002 | RUNNING    | here  |   2 |   4 |        1   |
|        00001 | PENDING    | here  |   1 |   2 |        0.5 |
|        00000 | TERMINATED | here  |   0 |   0 |        0   |
+--------------+------------+-------+-----+-----+------------+
... 2 more trials not shown (2 RUNNING)"""

EXPECTED_RESULT_2 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+-----+---------+---------+
|   Trial name | status     | loc   |   a |   b |   n/k/0 |   n/k/1 |
|--------------+------------+-------+-----+-----+---------+---------|
|        00002 | RUNNING    | here  |   2 |   4 |       2 |       4 |
|        00003 | RUNNING    | here  |   3 |   6 |       3 |       6 |
|        00004 | RUNNING    | here  |   4 |   8 |       4 |       8 |
|        00001 | PENDING    | here  |   1 |   2 |       1 |       2 |
|        00000 | TERMINATED | here  |   0 |   0 |       0 |       0 |
+--------------+------------+-------+-----+-----+---------+---------+"""

EXPECTED_RESULT_3 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+-----------+------------+
|   Trial name | status     | loc   |   A |   NestSub |   Metric 2 |
|--------------+------------+-------+-----+-----------+------------|
|        00002 | RUNNING    | here  |   2 |       1   |       0.5  |
|        00001 | PENDING    | here  |   1 |       0.5 |       0.25 |
|        00000 | TERMINATED | here  |   0 |       0   |       0    |
+--------------+------------+-------+-----+-----------+------------+
... 2 more trials not shown (2 RUNNING)"""

EXPECTED_RESULT_4 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+-----+------------+
|   Trial name | status     | loc   |   a |   b |   metric_1 |
|--------------+------------+-------+-----+-----+------------|
|        00002 | RUNNING    | here  |   2 |   4 |        1   |
|        00003 | RUNNING    | here  |   3 |   6 |        1.5 |
|        00004 | RUNNING    | here  |   4 |   8 |        2   |
|        00001 | PENDING    | here  |   1 |   2 |        0.5 |
|        00000 | TERMINATED | here  |   0 |   0 |        0   |
+--------------+------------+-------+-----+-----+------------+"""

END_TO_END_COMMAND = """
import ray
from ray import tune
from ray.tune.experiment.trial import _Location
from ray.tune.progress_reporter import _get_trial_location
from unittest.mock import patch


def mock_get_trial_location(trial, result):
    location = _get_trial_location(trial, result)
    if location.pid:
        return _Location("123.123.123.123", "1")
    return location


with patch("ray.tune.progress_reporter._get_trial_location",
           mock_get_trial_location):
    reporter = tune.progress_reporter.CLIReporter(metric_columns=["done"])

    def f(config):
        return {"done": True}

    ray.init(num_cpus=1)
    tune.run_experiments(
        {
            "one": {
                "run": f,
                "config": {
                    "a": tune.grid_search(list(range(10))),
                },
            },
            "two": {
                "run": f,
                "config": {
                    "b": tune.grid_search(list(range(10))),
                },
            },
            "three": {
                "run": f,
                "config": {
                    "c": tune.grid_search(list(range(10))),
                },
            },
        },
        verbose=3,
        progress_reporter=reporter)"""

EXPECTED_END_TO_END_START = """Number of trials: 30/30 (29 PENDING, 1 RUNNING)
+---------------+----------+-------------------+-----+-----+
| Trial name    | status   | loc               |   a |   b |
|---------------+----------+-------------------+-----+-----|
| f_xxxxx_00000 | RUNNING  | 123.123.123.123:1 |   0 |     |
| f_xxxxx_00001 | PENDING  |                   |   1 |     |"""

EXPECTED_END_TO_END_END = """Number of trials: 30/30 (30 TERMINATED)
+---------------+------------+-------------------+-----+-----+-----+--------+
| Trial name    | status     | loc               |   a |   b |   c | done   |
|---------------+------------+-------------------+-----+-----+-----+--------|
| f_xxxxx_00000 | TERMINATED | 123.123.123.123:1 |   0 |     |     | True   |
| f_xxxxx_00001 | TERMINATED | 123.123.123.123:1 |   1 |     |     | True   |
| f_xxxxx_00002 | TERMINATED | 123.123.123.123:1 |   2 |     |     | True   |
| f_xxxxx_00003 | TERMINATED | 123.123.123.123:1 |   3 |     |     | True   |
| f_xxxxx_00004 | TERMINATED | 123.123.123.123:1 |   4 |     |     | True   |
| f_xxxxx_00005 | TERMINATED | 123.123.123.123:1 |   5 |     |     | True   |
| f_xxxxx_00006 | TERMINATED | 123.123.123.123:1 |   6 |     |     | True   |
| f_xxxxx_00007 | TERMINATED | 123.123.123.123:1 |   7 |     |     | True   |
| f_xxxxx_00008 | TERMINATED | 123.123.123.123:1 |   8 |     |     | True   |
| f_xxxxx_00009 | TERMINATED | 123.123.123.123:1 |   9 |     |     | True   |
| f_xxxxx_00010 | TERMINATED | 123.123.123.123:1 |     |   0 |     | True   |
| f_xxxxx_00011 | TERMINATED | 123.123.123.123:1 |     |   1 |     | True   |
| f_xxxxx_00012 | TERMINATED | 123.123.123.123:1 |     |   2 |     | True   |
| f_xxxxx_00013 | TERMINATED | 123.123.123.123:1 |     |   3 |     | True   |
| f_xxxxx_00014 | TERMINATED | 123.123.123.123:1 |     |   4 |     | True   |
| f_xxxxx_00015 | TERMINATED | 123.123.123.123:1 |     |   5 |     | True   |
| f_xxxxx_00016 | TERMINATED | 123.123.123.123:1 |     |   6 |     | True   |
| f_xxxxx_00017 | TERMINATED | 123.123.123.123:1 |     |   7 |     | True   |
| f_xxxxx_00018 | TERMINATED | 123.123.123.123:1 |     |   8 |     | True   |
| f_xxxxx_00019 | TERMINATED | 123.123.123.123:1 |     |   9 |     | True   |
| f_xxxxx_00020 | TERMINATED | 123.123.123.123:1 |     |     |   0 | True   |
| f_xxxxx_00021 | TERMINATED | 123.123.123.123:1 |     |     |   1 | True   |
| f_xxxxx_00022 | TERMINATED | 123.123.123.123:1 |     |     |   2 | True   |
| f_xxxxx_00023 | TERMINATED | 123.123.123.123:1 |     |     |   3 | True   |
| f_xxxxx_00024 | TERMINATED | 123.123.123.123:1 |     |     |   4 | True   |
| f_xxxxx_00025 | TERMINATED | 123.123.123.123:1 |     |     |   5 | True   |
| f_xxxxx_00026 | TERMINATED | 123.123.123.123:1 |     |     |   6 | True   |
| f_xxxxx_00027 | TERMINATED | 123.123.123.123:1 |     |     |   7 | True   |
| f_xxxxx_00028 | TERMINATED | 123.123.123.123:1 |     |     |   8 | True   |
| f_xxxxx_00029 | TERMINATED | 123.123.123.123:1 |     |     |   9 | True   |
+---------------+------------+-------------------+-----+-----+-----+--------+"""  # noqa

EXPECTED_END_TO_END_AC = """Number of trials: 30/30 (30 TERMINATED)
+---------------+------------+-------+-----+-----+-----+
| Trial name    | status     | loc   |   a |   b |   c |
|---------------+------------+-------+-----+-----+-----|
| f_xxxxx_00000 | TERMINATED |       |   0 |     |     |
| f_xxxxx_00001 | TERMINATED |       |   1 |     |     |
| f_xxxxx_00002 | TERMINATED |       |   2 |     |     |
| f_xxxxx_00003 | TERMINATED |       |   3 |     |     |
| f_xxxxx_00004 | TERMINATED |       |   4 |     |     |
| f_xxxxx_00005 | TERMINATED |       |   5 |     |     |
| f_xxxxx_00006 | TERMINATED |       |   6 |     |     |
| f_xxxxx_00007 | TERMINATED |       |   7 |     |     |
| f_xxxxx_00008 | TERMINATED |       |   8 |     |     |
| f_xxxxx_00009 | TERMINATED |       |   9 |     |     |
| f_xxxxx_00010 | TERMINATED |       |     |   0 |     |
| f_xxxxx_00011 | TERMINATED |       |     |   1 |     |
| f_xxxxx_00012 | TERMINATED |       |     |   2 |     |
| f_xxxxx_00013 | TERMINATED |       |     |   3 |     |
| f_xxxxx_00014 | TERMINATED |       |     |   4 |     |
| f_xxxxx_00015 | TERMINATED |       |     |   5 |     |
| f_xxxxx_00016 | TERMINATED |       |     |   6 |     |
| f_xxxxx_00017 | TERMINATED |       |     |   7 |     |
| f_xxxxx_00018 | TERMINATED |       |     |   8 |     |
| f_xxxxx_00019 | TERMINATED |       |     |   9 |     |
| f_xxxxx_00020 | TERMINATED |       |     |     |   0 |
| f_xxxxx_00021 | TERMINATED |       |     |     |   1 |
| f_xxxxx_00022 | TERMINATED |       |     |     |   2 |
| f_xxxxx_00023 | TERMINATED |       |     |     |   3 |
| f_xxxxx_00024 | TERMINATED |       |     |     |   4 |
| f_xxxxx_00025 | TERMINATED |       |     |     |   5 |
| f_xxxxx_00026 | TERMINATED |       |     |     |   6 |
| f_xxxxx_00027 | TERMINATED |       |     |     |   7 |
| f_xxxxx_00028 | TERMINATED |       |     |     |   8 |
| f_xxxxx_00029 | TERMINATED |       |     |     |   9 |
+---------------+------------+-------+-----+-----+-----+"""

EXPECTED_BEST_1 = (
    "Current best trial: 00001 with metric_1=0.5 and "
    "parameters={'a': 1, 'b': 2, 'n': {'k': [1, 2]}}"
)

EXPECTED_BEST_2 = "Current best trial: 00004 with metric_1=2.0 and parameters={'a': 4}"

EXPECTED_SORT_RESULT_UNSORTED = """Number of trials: 5 (1 PENDING, 1 RUNNING, 3 TERMINATED)
+--------------+------------+-------+-----+------------+
|   Trial name | status     | loc   |   a |   metric_1 |
|--------------+------------+-------+-----+------------|
|        00004 | RUNNING    | here  |   4 |            |
|        00003 | PENDING    | here  |   3 |            |
|        00000 | TERMINATED | here  |   0 |        0.3 |
|        00001 | TERMINATED | here  |   1 |        0.2 |
+--------------+------------+-------+-----+------------+
... 1 more trials not shown (1 TERMINATED)"""

EXPECTED_SORT_RESULT_ASC = """Number of trials: 5 (1 PENDING, 1 RUNNING, 3 TERMINATED)
+--------------+------------+-------+-----+------------+
|   Trial name | status     | loc   |   a |   metric_1 |
|--------------+------------+-------+-----+------------|
|        00004 | RUNNING    | here  |   4 |            |
|        00003 | PENDING    | here  |   3 |            |
|        00001 | TERMINATED | here  |   1 |        0.2 |
|        00000 | TERMINATED | here  |   0 |        0.3 |
+--------------+------------+-------+-----+------------+
... 1 more trials not shown (1 TERMINATED)"""

EXPECTED_SORT_RESULT_DESC = """Number of trials: 5 (1 PENDING, 1 RUNNING, 3 TERMINATED)
+--------------+------------+-------+-----+------------+
|   Trial name | status     | loc   |   a |   metric_1 |
|--------------+------------+-------+-----+------------|
|        00004 | RUNNING    | here  |   4 |            |
|        00003 | PENDING    | here  |   3 |            |
|        00002 | TERMINATED | here  |   2 |        0.4 |
|        00000 | TERMINATED | here  |   0 |        0.3 |
+--------------+------------+-------+-----+------------+
... 1 more trials not shown (1 TERMINATED)"""

VERBOSE_EXP_OUT_1 = "Number of trials: 3/3 (2 PENDING, 1 RUNNING)"
VERBOSE_EXP_OUT_2 = "Number of trials: 3/3 (3 TERMINATED)"

VERBOSE_TRIAL_NORM_1 = (
    "Trial train_xxxxx_00000 reported acc=5 "
    "with parameters={'do': 'complete'}. This trial completed.\n"
)

# NOTE: We use Regex for `VERBOSE_TRIAL_NORM_2` to make the test deterministic.
# `"Trial train_xxxxx_00001 reported..."` and `"Trial train_xxxxx_00001 completed..."`
# are printed in separate calls. Sometimes, a status update is printed between the
# calls. For more information, see #29693.
VERBOSE_TRIAL_NORM_2_PATTERN = (
    r"Trial train_xxxxx_00001 reported _metric=6 with parameters=\{'do': 'once'\}\.\n"
    r"(?s).*"
    r"Trial train_xxxxx_00001 completed\. Last result: _metric=6\n"
)

VERBOSE_TRIAL_NORM_3 = (
    "Trial train_xxxxx_00002 reported acc=7 with parameters={'do': 'twice'}.\n"
)

VERBOSE_TRIAL_NORM_4 = (
    "Trial train_xxxxx_00002 reported acc=8 "
    "with parameters={'do': 'twice'}. This trial completed.\n"
)

VERBOSE_TRIAL_DETAIL = """+-------------------+----------+-------------------+----------+
| Trial name        | status   | loc               | do       |
|-------------------+----------+-------------------+----------|
| train_xxxxx_00000 | RUNNING  | 123.123.123.123:1 | complete |"""

VERBOSE_CMD = """from ray import tune
import random
import numpy as np
import time
from ray.tune.experiment.trial import _Location
from ray.tune.progress_reporter import _get_trial_location
from unittest.mock import patch


def mock_get_trial_location(trial, result):
    location = _get_trial_location(trial, result)
    if location.pid:
        return _Location("123.123.123.123", "1")
    return location


def train(config):
    if config["do"] == "complete":
        time.sleep(0.1)
        tune.report(acc=5, done=True)
    elif config["do"] == "once":
        time.sleep(0.5)
        tune.report(6)
    else:
        time.sleep(1.0)
        tune.report(acc=7)
        tune.report(acc=8)

random.seed(1234)
np.random.seed(1234)


with patch("ray.tune.progress_reporter._get_trial_location",
           mock_get_trial_location):
    tune.run(
        train,
        config={
            "do": tune.grid_search(["complete", "once", "twice"])
        },"""

# Add "verbose=3)" etc


class ProgressReporterTest(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "auto"

    def mock_trial(self, status, i):
        mock = MagicMock()
        mock.status = status
        mock.trial_id = "%05d" % i
        return mock

    def testFairFilterTrials(self):
        """Tests that trials are represented fairly."""
        trials_by_state = collections.defaultdict(list)
        # States for which trials are under and overrepresented
        states_under = (Trial.PAUSED, Trial.ERROR)
        states_over = (Trial.PENDING, Trial.RUNNING, Trial.TERMINATED)

        max_trials = 13
        num_trials_under = 2  # num of trials for each underrepresented state
        num_trials_over = 10  # num of trials for each overrepresented state

        i = 0
        for state in states_under:
            for _ in range(num_trials_under):
                trials_by_state[state].append(self.mock_trial(state, i))
                i += 1
        for state in states_over:
            for _ in range(num_trials_over):
                trials_by_state[state].append(self.mock_trial(state, i))
                i += 1

        filtered_trials_by_state = _fair_filter_trials(
            trials_by_state, max_trials=max_trials
        )
        for state in trials_by_state:
            if state in states_under:
                expected_num_trials = num_trials_under
            else:
                expected_num_trials = (
                    max_trials - num_trials_under * len(states_under)
                ) / len(states_over)
            state_trials = filtered_trials_by_state[state]
            self.assertEqual(len(state_trials), expected_num_trials)
            # Make sure trials are sorted newest-first within state.
            for i in range(len(state_trials) - 1):
                assert state_trials[i].trial_id < state_trials[i + 1].trial_id

    def testAddMetricColumn(self):
        """Tests edge cases of add_metric_column."""

        # Test list-initialized metric columns.
        reporter = CLIReporter(metric_columns=["foo", "bar"])
        with self.assertRaises(ValueError):
            reporter.add_metric_column("bar")

        with self.assertRaises(ValueError):
            reporter.add_metric_column("baz", "qux")

        reporter.add_metric_column("baz")
        self.assertIn("baz", reporter._metric_columns)

        # Test default-initialized (dict) metric columns.
        reporter = CLIReporter()
        reporter.add_metric_column("foo", "bar")
        self.assertIn("foo", reporter._metric_columns)

    def testInfer(self):
        reporter = CLIReporter()
        test_result = dict(foo_result=1, baz_result=4123, bar_result="testme")

        def test(config):
            for i in range(3):
                tune.report(**test_result)

        analysis = tune.run(test, num_samples=3)
        all_trials = analysis.trials
        inferred_results = reporter._infer_user_metrics(all_trials)
        for metric in inferred_results:
            self.assertNotIn(metric, AUTO_RESULT_KEYS)
            self.assertTrue(metric in test_result)

        class TestReporter(CLIReporter):
            _output = []

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._max_report_freqency = 0

            def report(self, *args, **kwargs):
                progress_str = self._progress_str(*args, **kwargs)
                self._output.append(progress_str)

        reporter = TestReporter()
        analysis = tune.run(test, num_samples=3, progress_reporter=reporter)
        found = {k: False for k in test_result}
        for output in reporter._output:
            for key in test_result:
                if key in output:
                    found[key] = True
        assert found["foo_result"]
        assert found["baz_result"]
        assert not found["bar_result"]

    def testProgressStr(self):
        trials = []
        for i in range(5):
            t = Mock()
            if i == 0:
                t.status = "TERMINATED"
            elif i == 1:
                t.status = "PENDING"
            else:
                t.status = "RUNNING"
            t.trial_id = "%05d" % i
            t.local_dir = "/foo"
            t.location = "here"
            t.config = {"a": i, "b": i * 2, "n": {"k": [i, 2 * i]}}
            t.evaluated_params = {"a": i, "b": i * 2, "n/k/0": i, "n/k/1": 2 * i}
            t.last_result = {
                "config": {"a": i, "b": i * 2, "n": {"k": [i, 2 * i]}},
                "metric_1": i / 2,
                "metric_2": i / 4,
                "nested": {"sub": i / 2},
            }
            t.__str__ = lambda self: self.trial_id
            trials.append(t)
        # One metric, two parameters
        prog1 = _trial_progress_str(
            trials, ["metric_1"], ["a", "b"], fmt="psql", max_rows=3, force_table=True
        )
        print(prog1)
        assert prog1 == EXPECTED_RESULT_1

        # No metric, all parameters
        prog2 = _trial_progress_str(
            trials, [], None, fmt="psql", max_rows=None, force_table=True
        )
        print(prog2)
        assert prog2 == EXPECTED_RESULT_2

        # Two metrics, one parameter, all with custom representation
        prog3 = _trial_progress_str(
            trials,
            {"nested/sub": "NestSub", "metric_2": "Metric 2"},
            {"a": "A"},
            fmt="psql",
            max_rows=3,
            force_table=True,
        )
        print(prog3)
        assert prog3 == EXPECTED_RESULT_3

        # Current best trial
        best1 = _best_trial_str(trials[1], "metric_1")
        assert best1 == EXPECTED_BEST_1

    def testBestTrialStr(self):
        """Assert that custom nested parameter columns are printed correctly"""
        config = {"nested": {"conf": "nested_value"}, "toplevel": "toplevel_value"}

        trial = Trial("", config=config, stub=True)
        trial.last_result = {"metric": 1, "config": config}

        result = _best_trial_str(trial, "metric")
        self.assertIn("nested_value", result)

        result = _best_trial_str(trial, "metric", parameter_columns=["nested/conf"])
        self.assertIn("nested_value", result)

    def testBestTrialZero(self):
        trial1 = Trial("", config={}, stub=True)
        trial1.last_result = {"metric": 7, "config": {}}

        trial2 = Trial("", config={}, stub=True)
        trial2.last_result = {"metric": 0, "config": {}}

        trial3 = Trial("", config={}, stub=True)
        trial3.last_result = {"metric": 2, "config": {}}

        reporter = TuneReporterBase(metric="metric", mode="min")
        best_trial, metric = reporter._current_best_trial([trial1, trial2, trial3])
        assert best_trial == trial2

    def testTimeElapsed(self):
        # Sun Feb 7 14:18:40 2016 -0800
        # (time of the first Ray commit)
        time_start = 1454825920
        time_now = (
            time_start
            + 1 * 60 * 60  # 1 hour
            + 31 * 60  # 31 minutes
            + 22  # 22 seconds
        )  # time to second commit

        # Local timezone output can be tricky, so we don't check the
        # day and the hour in this test.
        output = _time_passed_str(time_start, time_now)
        self.assertIn("Current time: 2016-02-", output)
        self.assertIn(":50:02 (running for 01:31:22.00)", output)

        time_now += 2 * 60 * 60 * 24  # plus two days
        output = _time_passed_str(time_start, time_now)
        self.assertIn("Current time: 2016-02-", output)
        self.assertIn(":50:02 (running for 2 days, 01:31:22.00)", output)

    def testCurrentBestTrial(self):
        trials = []
        for i in range(5):
            t = Mock()
            t.status = "RUNNING"
            t.trial_id = "%05d" % i
            t.local_dir = "/foo"
            t.location = "here"
            t.config = {"a": i, "b": i * 2, "n": {"k": [i, 2 * i]}}
            t.evaluated_params = {"a": i}
            t.last_result = {"config": {"a": i}, "metric_1": i / 2}
            t.__str__ = lambda self: self.trial_id
            trials.append(t)

        class TestReporter(CLIReporter):
            _output = []

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._max_report_freqency = 0

            def report(self, *args, **kwargs):
                progress_str = self._progress_str(*args, **kwargs)
                self._output.append(progress_str)

        reporter = TestReporter(mode="max")
        reporter.report(trials, done=False)
        assert EXPECTED_BEST_2 in reporter._output[0]

    def testSortByMetric(self):
        trials = []
        for i in range(5):
            t = Mock()
            if i < 3:
                t.status = "TERMINATED"
            elif i == 3:
                t.status = "PENDING"
            else:
                t.status = "RUNNING"
            t.trial_id = "%05d" % i
            t.local_dir = "/foo"
            t.location = "here"
            t.config = {"a": i}
            t.evaluated_params = {"a": i}
            t.last_result = {"config": {"a": i}}
            t.__str__ = lambda self: self.trial_id
            trials.append(t)
        # Set `metric_1` for terminated trails
        trials[0].last_result["metric_1"] = 0.3
        trials[1].last_result["metric_1"] = 0.2
        trials[2].last_result["metric_1"] = 0.4

        class TestReporter(CLIReporter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._max_report_freqency = 0
                self._output = ""

            def report(self, *args, **kwargs):
                progress_str = self._progress_str(*args, **kwargs)
                self._output = progress_str

        # Default reporter
        reporter1 = TestReporter(max_progress_rows=4, mode="max", metric="metric_1")
        reporter1.report(trials, done=False)
        print(reporter1._output)
        assert EXPECTED_SORT_RESULT_UNSORTED in reporter1._output

        # Sort by metric (asc)
        reporter2 = TestReporter(
            max_progress_rows=4, mode="min", metric="metric_1", sort_by_metric=True
        )
        reporter2.report(trials, done=False)
        assert EXPECTED_SORT_RESULT_ASC in reporter2._output

        # Sort by metric (desc)
        reporter3 = TestReporter(
            max_progress_rows=4, mode="max", metric="metric_1", sort_by_metric=True
        )
        reporter3.report(trials, done=False)
        print(reporter3._output)
        assert EXPECTED_SORT_RESULT_DESC in reporter3._output

        # Sort by metric when mode is None
        reporter4 = TestReporter(
            max_progress_rows=4, metric="metric_1", sort_by_metric=True
        )
        reporter4.report(trials, done=False)
        assert EXPECTED_SORT_RESULT_UNSORTED in reporter4._output

        # Sort by metric when metric is None
        reporter5 = TestReporter(max_progress_rows=4, mode="max", sort_by_metric=True)
        reporter5.report(trials, done=False)
        assert EXPECTED_SORT_RESULT_UNSORTED in reporter5._output

        # Sort by metric when metric is passed using
        # reporter.setup (called from tune.run)
        # calling repoter.set_search_properties
        reporter6 = TestReporter(max_progress_rows=4, sort_by_metric=True)
        reporter6.set_search_properties(metric="metric_1", mode="max")
        reporter6.report(trials, done=False)
        assert EXPECTED_SORT_RESULT_DESC in reporter6._output

    def testEndToEndReporting(self):
        try:
            os.environ["_TEST_TUNE_TRIAL_UUID"] = "xxxxx"
            os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "100"
            output = run_string_as_driver(END_TO_END_COMMAND)
            try:
                assert EXPECTED_END_TO_END_START in output
                assert EXPECTED_END_TO_END_END in output
                assert "(raylet)" not in output, "Unexpected raylet log messages"
            except Exception:
                print("*** BEGIN OUTPUT ***")
                print(output)
                print("*** END OUTPUT ***")
                raise
        finally:
            del os.environ["_TEST_TUNE_TRIAL_UUID"]

    def testVerboseReporting(self):
        try:
            os.environ["_TEST_TUNE_TRIAL_UUID"] = "xxxxx"

            verbose_0_cmd = VERBOSE_CMD + "verbose=0)"
            output = run_string_as_driver(verbose_0_cmd)
            try:
                self.assertNotIn(VERBOSE_EXP_OUT_1, output)
                self.assertNotIn(VERBOSE_EXP_OUT_2, output)
                self.assertNotIn(VERBOSE_TRIAL_NORM_1, output)
                self.assertIsNone(re.search(VERBOSE_TRIAL_NORM_2_PATTERN, output))
                self.assertNotIn(VERBOSE_TRIAL_NORM_3, output)
                self.assertNotIn(VERBOSE_TRIAL_NORM_4, output)
                self.assertNotIn(VERBOSE_TRIAL_DETAIL, output)
            except Exception:
                print("*** BEGIN OUTPUT ***")
                print(output)
                print("*** END OUTPUT ***")
                raise

            verbose_1_cmd = VERBOSE_CMD + "verbose=1)"
            output = run_string_as_driver(verbose_1_cmd)
            try:
                self.assertIn(VERBOSE_EXP_OUT_1, output)
                self.assertIn(VERBOSE_EXP_OUT_2, output)
                self.assertNotIn(VERBOSE_TRIAL_NORM_1, output)
                self.assertIsNone(re.search(VERBOSE_TRIAL_NORM_2_PATTERN, output))
                self.assertNotIn(VERBOSE_TRIAL_NORM_3, output)
                self.assertNotIn(VERBOSE_TRIAL_NORM_4, output)
                self.assertNotIn(VERBOSE_TRIAL_DETAIL, output)
            except Exception:
                print("*** BEGIN OUTPUT ***")
                print(output)
                print("*** END OUTPUT ***")
                raise

            verbose_2_cmd = VERBOSE_CMD + "verbose=2)"
            output = run_string_as_driver(verbose_2_cmd)
            try:
                self.assertIn(VERBOSE_EXP_OUT_1, output)
                self.assertIn(VERBOSE_EXP_OUT_2, output)
                self.assertIn(VERBOSE_TRIAL_NORM_1, output)
                self.assertIsNotNone(re.search(VERBOSE_TRIAL_NORM_2_PATTERN, output))
                self.assertIn(VERBOSE_TRIAL_NORM_3, output)
                self.assertIn(VERBOSE_TRIAL_NORM_4, output)
                self.assertNotIn(VERBOSE_TRIAL_DETAIL, output)
            except Exception:
                print("*** BEGIN OUTPUT ***")
                print(output)
                print("*** END OUTPUT ***")
                raise

            verbose_3_cmd = VERBOSE_CMD + "verbose=3)"
            output = run_string_as_driver(verbose_3_cmd)
            try:
                self.assertIn(VERBOSE_EXP_OUT_1, output)
                self.assertIn(VERBOSE_EXP_OUT_2, output)
                self.assertNotIn(VERBOSE_TRIAL_NORM_1, output)
                self.assertIsNone(re.search(VERBOSE_TRIAL_NORM_2_PATTERN, output))
                self.assertNotIn(VERBOSE_TRIAL_NORM_3, output)
                self.assertNotIn(VERBOSE_TRIAL_NORM_4, output)
                self.assertIn(VERBOSE_TRIAL_DETAIL, output)
            except Exception:
                print("*** BEGIN OUTPUT ***")
                print(output)
                print("*** END OUTPUT ***")
                raise
        finally:
            del os.environ["_TEST_TUNE_TRIAL_UUID"]

    def testReporterDetection(self):
        """Test if correct reporter is returned from ``detect_reporter()``"""
        reporter = _detect_reporter()
        self.assertTrue(isinstance(reporter, CLIReporter))
        self.assertFalse(isinstance(reporter, JupyterNotebookReporter))

        with patch("ray.tune.progress_reporter.IS_NOTEBOOK", True):
            reporter = _detect_reporter()
            self.assertFalse(isinstance(reporter, CLIReporter))
            self.assertTrue(isinstance(reporter, JupyterNotebookReporter))

    def testProgressReporterAPI(self):
        class CustomReporter(ProgressReporter):
            def should_report(self, trials, done=False):
                return True

            def report(self, trials, done, *sys_info):
                pass

        tune.run(lambda config: 2, num_samples=1, progress_reporter=CustomReporter())

    def testMaxLen(self):
        trials = []
        for i in range(5):
            t = Mock()
            t.status = "TERMINATED"
            t.trial_id = "%05d" % i
            t.local_dir = "/foo"
            t.location = "here"
            t.config = {"verylong" * 20: i}
            t.evaluated_params = {"verylong" * 20: i}
            t.last_result = {"some_metric": "evenlonger" * 100}
            t.__str__ = lambda self: self.trial_id
            trials.append(t)

        progress_str = _trial_progress_str(
            trials, metric_columns=["some_metric"], force_table=True
        )
        assert any(len(row) <= 90 for row in progress_str.split("\n"))


def test_max_len():
    assert (
        _max_len("some_long_string/even_longer", max_len=28)
        == "some_long_string/even_longer"
    )
    assert _max_len("some_long_string/even_longer", max_len=15) == ".../even_longer"

    assert (
        _max_len(
            "19_character_string/19_character_string/too_long", max_len=20, wrap=True
        )
        == "...r_string/19_chara\ncter_string/too_long"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
