# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pickle
import shutil
import tempfile
import unittest

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune import track


class TrackSaveRestoreSuite(object):

    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0, local_mode=self.local_mode)

    def tearDown(self):
        track.shutdown()
        ray.shutdown()

    def testSaveRestore(self):
        track.init(trial_name="test_log")
        session = track.get_session()
        for i in range(5):
            track.log(test=i)
            checkpoint_dir = track.current_iter_checkpoint_dir()
            checkpoint_path = os.path.join(checkpoint_dir, "model")
            with open(checkpoint_path) as f:
                f.write(str(i))
            if i % 2 == 0:
                track.save(checkpoint_path)

        # result_path = os.path.join(session.logdir, EXPR_RESULT_FILE)
        # self.assertTrue(_check_json_val(result_path, "test", i))

        # restore shit

