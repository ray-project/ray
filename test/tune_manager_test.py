from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import numpy as np

from ray.tune.hyperband import HyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.result import TrainingResult
from ray.tune.trial import Trial
from ray.tune.trial_scheduler import TrialScheduler


class ExpManagerSuite(unittest.TestCase):
    def basicSetup(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        port = None
        manager = ExpManager(port)
        # add trials to runner
        return runner, manager

    def testAddTrial(self):
        runner, manager = self.basicSetup()
        spec = {}
        future = manager.add_trial(spec, nowait=True)
        runner.step()
        # assert trial is running
        raise NotImplementedError

    def testGetTrial(self):
        runner, manager = self.basicSetup()
        runner.step

    def testGetAllTrials(self):
        runner, manager = self.basicSetup()
        all_trials = manager.get_all_trials(nowait=True)
        runner.step()
        # assert
        raise NotImplementedError

    def testStopTrial(self):
        runner, manager = self.basicSetup()
        raise NotImplementedError


if __name__ == "__main__":
    unittest.main(verbosity=2)
