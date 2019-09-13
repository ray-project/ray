from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd
import unittest

import ray
from ray import tune
from ray.tune import track
from ray.tune.result import EXPR_PARAM_FILE, EXPR_RESULT_FILE


def _check_json_val(fname, key, val):
    with open(fname, "r") as f:
        df = pd.read_json(f, typ="frame", lines=True)
        return key in df.columns and (df[key].tail(n=1) == val).all()


class TrackApiTest(unittest.TestCase):
    def tearDown(self):
        track.shutdown()
        ray.shutdown()

    def testSessionInitShutdown(self):
        self.assertTrue(track._session is None)

        # Checks that the singleton _session is created/destroyed
        # by track.init() and track.shutdown()
        for _ in range(2):
            # do it twice to see that we can reopen the session
            track.init(trial_name="test_init")
            self.assertTrue(track._session is not None)
            track.shutdown()
            self.assertTrue(track._session is None)

    def testLogCreation(self):
        """Checks that track.init() starts logger and creates log files."""
        track.init(trial_name="test_init")
        session = track.get_session()
        self.assertTrue(session is not None)

        self.assertTrue(os.path.isdir(session.logdir))

        params_path = os.path.join(session.logdir, EXPR_PARAM_FILE)
        result_path = os.path.join(session.logdir, EXPR_RESULT_FILE)

        self.assertTrue(os.path.exists(params_path))
        self.assertTrue(os.path.exists(result_path))
        self.assertTrue(session.logdir == track.trial_dir())

    def testMetric(self):
        track.init(trial_name="test_log")
        session = track.get_session()
        for i in range(5):
            track.log(test=i)
        result_path = os.path.join(session.logdir, EXPR_RESULT_FILE)
        self.assertTrue(_check_json_val(result_path, "test", i))

    def testRayOutput(self):
        """Checks that local and remote log format are the same."""
        ray.init()

        def testme(config):
            for i in range(config["iters"]):
                track.log(iteration=i, hi="test")

        trials = tune.run(testme, config={"iters": 5}).trials
        trial_res = trials[0].last_result
        self.assertTrue(trial_res["hi"], "test")
        self.assertTrue(trial_res["training_iteration"], 5)

    def testLocalMetrics(self):
        """Checks that metric state is updated correctly."""
        track.init(trial_name="test_logs")
        session = track.get_session()
        self.assertEqual(set(session.trial_config.keys()), {"trial_id"})

        result_path = os.path.join(session.logdir, EXPR_RESULT_FILE)
        track.log(test=1)
        self.assertTrue(_check_json_val(result_path, "test", 1))
        track.log(iteration=1, test=2)
        self.assertTrue(_check_json_val(result_path, "test", 2))
