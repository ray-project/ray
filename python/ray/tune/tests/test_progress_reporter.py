from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import sys
import time
import unittest

from ray.tune.trial import Trial
from ray.tune.progress_reporter import _fair_filter_trials

if sys.version_info >= (3, 3):
    from unittest.mock import MagicMock
else:
    from mock import MagicMock


class ProgressReporterTest(unittest.TestCase):
    def mock_trial(self, status, start_time):
        mock = MagicMock()
        mock.status = status
        mock.start_time = start_time
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

        for state in states_under:
            for _ in range(num_trials_under):
                trials_by_state[state].append(
                    self.mock_trial(state, time.time()))
        for state in states_over:
            for _ in range(num_trials_over):
                trials_by_state[state].append(
                    self.mock_trial(state, time.time()))

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
                self.assertGreaterEqual(state_trials[i].start_time,
                                        state_trials[i + 1].start_time)
