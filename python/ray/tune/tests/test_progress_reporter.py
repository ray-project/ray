import pytest
import collections
import os
import unittest
from unittest.mock import MagicMock, Mock

from ray.test_utils import run_string_as_driver
from ray.tune.trial import Trial
from ray.tune.progress_reporter import (CLIReporter, _fair_filter_trials,
                                        trial_progress_str)

EXPECTED_RESULT_1 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+-----+------------+
|   Trial name | status     | loc   |   a |   b |   metric_1 |
|--------------+------------+-------+-----+-----+------------|
|        00001 | PENDING    | here  |   1 |   2 |        0.5 |
|        00002 | RUNNING    | here  |   2 |   4 |        1   |
|        00000 | TERMINATED | here  |   0 |   0 |        0   |
+--------------+------------+-------+-----+-----+------------+
... 2 more trials not shown (2 RUNNING)"""

EXPECTED_RESULT_2 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+-----+---------+---------+
|   Trial name | status     | loc   |   a |   b |   n/k/0 |   n/k/1 |
|--------------+------------+-------+-----+-----+---------+---------|
|        00000 | TERMINATED | here  |   0 |   0 |       0 |       0 |
|        00001 | PENDING    | here  |   1 |   2 |       1 |       2 |
|        00002 | RUNNING    | here  |   2 |   4 |       2 |       4 |
|        00003 | RUNNING    | here  |   3 |   6 |       3 |       6 |
|        00004 | RUNNING    | here  |   4 |   8 |       4 |       8 |
+--------------+------------+-------+-----+-----+---------+---------+"""

EXPECTED_RESULT_3 = """Result logdir: /foo
Number of trials: 5 (1 PENDING, 3 RUNNING, 1 TERMINATED)
+--------------+------------+-------+-----+------------+------------+
|   Trial name | status     | loc   |   A |   Metric 1 |   Metric 2 |
|--------------+------------+-------+-----+------------+------------|
|        00001 | PENDING    | here  |   1 |        0.5 |       0.25 |
|        00002 | RUNNING    | here  |   2 |        1   |       0.5  |
|        00000 | TERMINATED | here  |   0 |        0   |       0    |
+--------------+------------+-------+-----+------------+------------+
... 2 more trials not shown (2 RUNNING)"""

END_TO_END_COMMAND = """
import ray
from ray import tune

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
}, reuse_actors=True, verbose=1)"""

EXPECTED_END_TO_END_START = """Number of trials: 30 (29 PENDING, 1 RUNNING)
+---------------+----------+-------+-----+-----+
| Trial name    | status   | loc   |   a |   b |
|---------------+----------+-------+-----+-----|
| f_xxxxx_00001 | PENDING  |       |   1 |     |
| f_xxxxx_00002 | PENDING  |       |   2 |     |
| f_xxxxx_00003 | PENDING  |       |   3 |     |
| f_xxxxx_00004 | PENDING  |       |   4 |     |
| f_xxxxx_00005 | PENDING  |       |   5 |     |
| f_xxxxx_00006 | PENDING  |       |   6 |     |
| f_xxxxx_00007 | PENDING  |       |   7 |     |
| f_xxxxx_00008 | PENDING  |       |   8 |     |
| f_xxxxx_00009 | PENDING  |       |   9 |     |
| f_xxxxx_00010 | PENDING  |       |     |   0 |
| f_xxxxx_00011 | PENDING  |       |     |   1 |
| f_xxxxx_00012 | PENDING  |       |     |   2 |
| f_xxxxx_00013 | PENDING  |       |     |   3 |
| f_xxxxx_00014 | PENDING  |       |     |   4 |
| f_xxxxx_00015 | PENDING  |       |     |   5 |
| f_xxxxx_00016 | PENDING  |       |     |   6 |
| f_xxxxx_00017 | PENDING  |       |     |   7 |
| f_xxxxx_00018 | PENDING  |       |     |   8 |
| f_xxxxx_00019 | PENDING  |       |     |   9 |
| f_xxxxx_00000 | RUNNING  |       |   0 |     |
+---------------+----------+-------+-----+-----+
... 10 more trials not shown (10 PENDING)"""

EXPECTED_END_TO_END_END = """Number of trials: 30 (30 TERMINATED)
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

EXPECTED_END_TO_END_AC = """Number of trials: 30 (30 TERMINATED)
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


class ProgressReporterTest(unittest.TestCase):
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
                "metric_2": i / 4
            }
            t.__str__ = lambda self: self.trial_id
            trials.append(t)
        # One metric, two parameters
        prog1 = trial_progress_str(
            trials, ["metric_1"], ["a", "b"], fmt="psql", max_rows=3)
        print(prog1)
        assert prog1 == EXPECTED_RESULT_1

        # No metric, all parameters
        prog2 = trial_progress_str(trials, [], None, fmt="psql", max_rows=None)
        print(prog2)
        assert prog2 == EXPECTED_RESULT_2

        # Both metrics, one parameter, all with custom representation
        prog3 = trial_progress_str(
            trials, {
                "metric_1": "Metric 1",
                "metric_2": "Metric 2"
            }, {"a": "A"},
            fmt="psql",
            max_rows=3)
        print(prog3)
        assert prog3 == EXPECTED_RESULT_3

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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
