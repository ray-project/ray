from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd
import unittest

import ray
from ray import tune
from ray.tune import track
from ray.tune.result import (TRAINING_ITERATION, EXPR_PARARM_FILE,
                             EXPR_RESULT_FILE)


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

        self.assertTrue(os.path.isdir(session.experiment_dir))
        self.assertTrue(os.path.isdir(session.logdir))

        params_path = os.path.join(session.logdir, EXPR_PARARM_FILE)
        result_path = os.path.join(session.logdir, EXPR_RESULT_FILE)

        self.assertTrue(os.path.exists(params_path))
        self.assertTrue(os.path.exists(result_path))
        track.shutdown()

    def testMetric(self):
        track.init(trial_name="test_metric")
        session = track.get_session()
        for i in range(5):
            track.metric(test=i)
        result_path = os.path.join(session.logdir, EXPR_RESULT_FILE)
        self.assertTrue(_check_json_val(result_path, "test", i))


    def testRayOutput(self):
        """Checks that local and remote log format are the same."""
        ray.init()
        def testme(config):
            for i in range(config["iters"]):
                track.metric(iteration=i, **{"hi": "test"})

        trials = tune.run(testme, config={"iters": 5})
        trial_res = trials[0].last_result
        self.assertTrue(trial_res["hi"], "test")
        self.assertTrue(trial_res["training_iteration"], 5)


    # def testLocalMetrics(self):
    #     """Checks that metric state is updated correctly."""
    #     track.init(trial_name="test_metrics")
    #     session = track.get_session()
    #     self.assertEqual(
    #         set(session.trial_config.keys()),
    #         set(["trial_id"]))

    #     # iteration=None defaults to max_iteration
    #     track.metric(test=1)
    #     self.assertEqual(session.trial_config[TRAINING_ITERATION], -1)

    #     params_path = os.path.join(session.artifact_dir, EXPR_PARARM_FILE)
    #     result_path = os.path.join(session.artifact_dir, EXPR_RESULT_FILE)

    #     # check that dict was correctly dumped to json

    #     # check that params and results are dumped
    #     self.assertTrue(_check_json_val(params_path, TRAINING_ITERATION, -1))
    #     self.assertTrue(_check_json_val(result_path, "test", 1))

    #     # check that they are updated!
    #     track.metric(iteration=1, test=2)
    #     self.assertTrue(_check_json_val(result_path, "test", 2))
    #     self.assertEqual(session.trial_config[TRAINING_ITERATION], 1)

    #     # params are updated at the end
    #     track.shutdown()
    #     self.assertTrue(_check_json_val(params_path, TRAINING_ITERATION, 1))
