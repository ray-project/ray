import pytest
import collections
import os
import unittest
from unittest.mock import MagicMock, Mock
from ray import tune
from ray.test_utils import run_string_as_driver
from ray.tune.trial import Trial
from ray.tune.result import AUTO_RESULT_KEYS
from ray.tune.progress_reporter import (CLIReporter, _fair_filter_trials,
                                        best_trial_str, trial_progress_str)

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

END_TO_END_COMMAND = """
import ray
from ray import tune

reporter = tune.progress_reporter.CLIReporter(metric_columns=["done"])

def f(config):
    return {"done": True}

ray.init(num_cpus=1)
tune.run_experiments({
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
}, verbose=3, progress_reporter=reporter)"""

EXPECTED_END_TO_END_START = """Number of trials: 30/30 (29 PENDING, 1 RUNNING)
+---------------+----------+-------+-----+-----+
| Trial name    | status   | loc   |   a |   b |
|---------------+----------+-------+-----+-----|
| f_xxxxx_00000 | RUNNING  |       |   0 |     |
| f_xxxxx_00001 | PENDING  |       |   1 |     |"""

EXPECTED_END_TO_END_END = """Number of trials: 30/30 (30 TERMINATED)
+---------------+------------+-------+-----+-----+-----+--------+
| Trial name    | status     | loc   |   a |   b |   c | done   |
|---------------+------------+-------+-----+-----+-----+--------|
| f_xxxxx_00000 | TERMINATED |       |   0 |     |     | True   |
| f_xxxxx_00001 | TERMINATED |       |   1 |     |     | True   |
| f_xxxxx_00002 | TERMINATED |       |   2 |     |     | True   |
| f_xxxxx_00003 | TERMINATED |       |   3 |     |     | True   |
| f_xxxxx_00004 | TERMINATED |       |   4 |     |     | True   |
| f_xxxxx_00005 | TERMINATED |       |   5 |     |     | True   |
| f_xxxxx_00006 | TERMINATED |       |   6 |     |     | True   |
| f_xxxxx_00007 | TERMINATED |       |   7 |     |     | True   |
| f_xxxxx_00008 | TERMINATED |       |   8 |     |     | True   |
| f_xxxxx_00009 | TERMINATED |       |   9 |     |     | True   |
| f_xxxxx_00010 | TERMINATED |       |     |   0 |     | True   |
| f_xxxxx_00011 | TERMINATED |       |     |   1 |     | True   |
| f_xxxxx_00012 | TERMINATED |       |     |   2 |     | True   |
| f_xxxxx_00013 | TERMINATED |       |     |   3 |     | True   |
| f_xxxxx_00014 | TERMINATED |       |     |   4 |     | True   |
| f_xxxxx_00015 | TERMINATED |       |     |   5 |     | True   |
| f_xxxxx_00016 | TERMINATED |       |     |   6 |     | True   |
| f_xxxxx_00017 | TERMINATED |       |     |   7 |     | True   |
| f_xxxxx_00018 | TERMINATED |       |     |   8 |     | True   |
| f_xxxxx_00019 | TERMINATED |       |     |   9 |     | True   |
| f_xxxxx_00020 | TERMINATED |       |     |     |   0 | True   |
| f_xxxxx_00021 | TERMINATED |       |     |     |   1 | True   |
| f_xxxxx_00022 | TERMINATED |       |     |     |   2 | True   |
| f_xxxxx_00023 | TERMINATED |       |     |     |   3 | True   |
| f_xxxxx_00024 | TERMINATED |       |     |     |   4 | True   |
| f_xxxxx_00025 | TERMINATED |       |     |     |   5 | True   |
| f_xxxxx_00026 | TERMINATED |       |     |     |   6 | True   |
| f_xxxxx_00027 | TERMINATED |       |     |     |   7 | True   |
| f_xxxxx_00028 | TERMINATED |       |     |     |   8 | True   |
| f_xxxxx_00029 | TERMINATED |       |     |     |   9 | True   |
+---------------+------------+-------+-----+-----+-----+--------+"""

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

EXPECTED_BEST_1 = "Current best trial: 00001 with metric_1=0.5 and " \
                  "parameters={'a': 1, 'b': 2, 'n': {'k': [1, 2]}}"

EXPECTED_BEST_2 = "Current best trial: 00004 with metric_1=2.0 and " \
                  "parameters={'a': 4}"

VERBOSE_EXP_OUT_1 = "Number of trials: 3/3 (2 PENDING, 1 RUNNING)"
VERBOSE_EXP_OUT_2 = "Number of trials: 3/3 (3 TERMINATED)"

VERBOSE_TRIAL_NORM = "Trial train_xxxxx_00000 reported acc=5 with " + \
    """parameters={'do': 'complete'}. This trial completed.
Trial train_xxxxx_00001 reported _metric=6 with parameters={'do': 'once'}.
Trial train_xxxxx_00001 completed. Last result: _metric=6
Trial train_xxxxx_00002 reported acc=7 with parameters={'do': 'twice'}.
Trial train_xxxxx_00002 reported acc=8 with parameters={'do': 'twice'}. """ + \
    "This trial completed."

VERBOSE_TRIAL_DETAIL = """+-------------------+----------+-------+----------+
| Trial name        | status   | loc   | do       |
|-------------------+----------+-------+----------|
| train_xxxxx_00000 | RUNNING  |       | complete |"""

VERBOSE_CMD = """from ray import tune
import random
import numpy as np
import time

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

tune.run(
    train,
    config={
        "do": tune.grid_search(["complete", "once", "twice"])
    },"""

# Add "verbose=3)" etc


class ProgressReporterTest(unittest.TestCase):
    def setUp(self) -> None:
        # Wait up to five seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"

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
            trials_by_state, max_trials=max_trials)
        for state in trials_by_state:
            if state in states_under:
                expected_num_trials = num_trials_under
            else:
                expected_num_trials = (max_trials - num_trials_under *
                                       len(states_under)) / len(states_over)
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
            t.evaluated_params = {
                "a": i,
                "b": i * 2,
                "n/k/0": i,
                "n/k/1": 2 * i
            }
            t.last_result = {
                "config": {
                    "a": i,
                    "b": i * 2,
                    "n": {
                        "k": [i, 2 * i]
                    }
                },
                "metric_1": i / 2,
                "metric_2": i / 4,
                "nested": {
                    "sub": i / 2
                }
            }
            t.__str__ = lambda self: self.trial_id
            trials.append(t)
        # One metric, two parameters
        prog1 = trial_progress_str(
            trials, ["metric_1"], ["a", "b"],
            fmt="psql",
            max_rows=3,
            force_table=True)
        print(prog1)
        assert prog1 == EXPECTED_RESULT_1

        # No metric, all parameters
        prog2 = trial_progress_str(
            trials, [], None, fmt="psql", max_rows=None, force_table=True)
        print(prog2)
        assert prog2 == EXPECTED_RESULT_2

        # Two metrics, one parameter, all with custom representation
        prog3 = trial_progress_str(
            trials, {
                "nested/sub": "NestSub",
                "metric_2": "Metric 2"
            }, {"a": "A"},
            fmt="psql",
            max_rows=3,
            force_table=True)
        print(prog3)
        assert prog3 == EXPECTED_RESULT_3

        # Current best trial
        best1 = best_trial_str(trials[1], "metric_1")
        assert best1 == EXPECTED_BEST_1

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

    def testEndToEndReporting(self):
        try:
            os.environ["_TEST_TUNE_TRIAL_UUID"] = "xxxxx"
            output = run_string_as_driver(END_TO_END_COMMAND)
            try:
                assert EXPECTED_END_TO_END_START in output
                assert EXPECTED_END_TO_END_END in output
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
                self.assertNotIn(VERBOSE_TRIAL_NORM, output)
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
                self.assertNotIn(VERBOSE_TRIAL_NORM, output)
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
                self.assertIn(VERBOSE_TRIAL_NORM, output)
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
                self.assertNotIn(VERBOSE_TRIAL_NORM, output)
                self.assertIn(VERBOSE_TRIAL_DETAIL, output)
            except Exception:
                print("*** BEGIN OUTPUT ***")
                print(output)
                print("*** END OUTPUT ***")
                raise
        finally:
            del os.environ["_TEST_TUNE_TRIAL_UUID"]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
