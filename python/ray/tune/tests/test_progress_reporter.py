from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import sys
import unittest

from ray.tune.trial import Trial
from ray.tune.progress_reporter import _fair_filter_trials

if sys.version_info >= (3, 3):
    from unittest.mock import MagicMock
else:
    from mock import MagicMock


class ProgressReporterTest(unittest.TestCase):
    def testFairFilterTrials(self):
        """Tests that trials are represented fairly."""
        trials_by_state = collections.defaultdict(list)
        # States for which trials are under and overrepresented
        states_under = (Trial.PAUSED, Trial.ERROR)
        states_over = (Trial.PENDING, Trial.RUNNING, Trial.TERMINATED)

        max_trials = 13
        num_trials_under = 2  # num of trials for each underrepresented state
        num_trials_over = 10  # num of trials for each overrepresented state

        for state in states_under + states_over:
            n = num_trials_under if state in states_under else num_trials_over
            for _ in range(n):
                trial = MagicMock()
                trial.status = state
                trials_by_state[state].append(trial)

        trials = _fair_filter_trials(trials_by_state, max_trials=max_trials)

        filtered_trials_by_state = collections.defaultdict(int)
        for trial in trials:
            filtered_trials_by_state[trial.status] += 1
        for state in trials_by_state:
            if state in states_under:
                expected_num_trials = num_trials_under
            else:
                expected_num_trials = (max_trials - num_trials_under *
                                       len(states_under)) / len(states_over)
            self.assertEqual(filtered_trials_by_state[state],
                             expected_num_trials)
