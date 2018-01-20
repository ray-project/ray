from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import numpy as np

import ray
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.result import TrainingResult
from ray.tune.trial import Trial, Resources
from ray.tune.trial import Trial
from ray.tune.web_server import TuneClient
from ray.tune.trial_runner import TrialRunner


class TuneServerSuite(unittest.TestCase):
    def basicSetup(self):
        ray.init(num_cpus=4, num_gpus=1)
        self.runner = TrialRunner()
        runner = self.runner
        kwargs = {
            "stopping_criterion": {"training_iteration": 3},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        port = 4321
        client = TuneClient(port)
        # add trials to runner
        return runner, client

    def tearDown(self):
        try:
            self.runner._server.shutdown()
            self.runner = None
        except Exception as e:
            print(e)
        ray.worker.cleanup()

    def testAddTrial(self):
        runner, client = self.basicSetup()
        for i in range(3):
            runner.step()
        spec = {
            "run": "train_mnist",
            "stop": {"training_iteration": 3},
            "resources": dict(cpu=1, gpu=1),
        }
        client.add_trial("test", spec)
        runner.step()
        all_trials = client.get_all_trials()
        runner.step()
        self.assertEqual(len(all_trials), 3)

    def testGetTrials(self):
        runner, client = self.basicSetup()
        for i in range(3):
            runner.step()
        all_trials = client.get_all_trials()
        self.assertEqual(len(all_trials), 2)
        tid = all_trials[0][0]["id"]
        trial_info = client.get_trial(tid)
        runner.step()
        self.assertEqual(len(all_trials), 2)


    def testStopTrial(self):
        runner, client = self.basicSetup()
        for i in range(3):
            runner.step()
        all_trials = client.get_all_trials()
        self.assertEqual(
            len([t for t in all_trials if t["status"] == Trial.RUNNING]), 2)
        tid = all_trials[0][0]["id"]
        client.stop_trial(tid)
        self.assertEqual(
            len([t for t in all_trials if t["status"] == Trial.RUNNING]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
