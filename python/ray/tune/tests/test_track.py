from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd
import unittest

from ray.tune import track
from ray.tune.result import TRAINING_ITERATION


class TrackApiTest(unittest.TestCase):

    def testSessionInitShutdown(self):
        self.assertTrue(track._session is None)
        """Checks that the singleton _session is created/destroyed

        by track.init() and track.shutdown()
        """
        for _ in range(2):
            # do it twice to see that we can reopen the session
            track.init(trial_prefix="test_init")
            self.assertTrue(track._session is not None)
            track.shutdown()
            self.assertTrue(track._session is None)

    def testLogCreation(self):
        """Checks that track.init() starts logger and creates log files.
        """
        track.init(trial_prefix="test_init")
        session = track._session
        self.assertTrue(session is not None)

        self.assertTrue(os.path.isdir(session.base_dir))
        self.assertTrue(os.path.isdir(session.artifact_dir))

        params_fname = os.path.join(session.artifact_dir, "params.json")
        result_fname = os.path.join(session.artifact_dir, "result.json")

        self.assertTrue(os.path.exists(params_fname))
        self.assertTrue(os.path.exists(result_fname))
        track.shutdown()

    def testRayOutput(self):
        """Checks that local and remote log format are the same.
        """
        pass

    def testLocalMetrics(self):
        """Checks that metric state is updated correctly.
        """
        track.init(trial_prefix="test_metrics")
        session = track._session
        self.assertEqual(set(session.param_map.keys()), set(
            ["trial_id", TRAINING_ITERATION, "trial_completed"]))

        # iteration=None defaults to max_iteration
        track.metric(test=1)
        self.assertEqual(session.param_map[TRAINING_ITERATION], -1)

        params_fname = os.path.join(session.artifact_dir, "params.json")
        result_fname = os.path.join(session.artifact_dir, "result.json")

        # check that dict was correctly dumped to json
        def _assert_json_val(fname, key, val):
            with open(fname, "r") as f:
                df = pd.read_json(f, typ='frame', lines=True)
                self.assertTrue(key in df.columns)
                self.assertTrue((df[key].tail(n=1) == val).all())

        # check that params and results are dumped
        _assert_json_val(params_fname, TRAINING_ITERATION, -1)
        _assert_json_val(result_fname, "test", 1)

        # check that they are updated!
        track.metric(iteration=1, test=2)
        _assert_json_val(result_fname, "test", 2)
        self.assertEqual(session.param_map[TRAINING_ITERATION], 1)

        # params are updated at the end
        track.shutdown()
        _assert_json_val(params_fname, TRAINING_ITERATION, 1)
