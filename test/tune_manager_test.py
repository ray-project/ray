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


# TODO(rliaw): These tests need to be written
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
        future = manager.get_all_trials(nowait=True)
        runner.step()
        all_trials = ray.get(future)
        self.assertEqual(len(all_trials), 3)

    def testGetTrials(self):
        runner, manager = self.basicSetup()
        future = manager.get_all_trials(nowait=True)
        runner.step()
        all_trials = ray.get(future)
        import ipdb; ipdb.set_trace()
        self.assertEqual(len(all_trials), 2)
        tid = all_trials[0][0]
        future = manager.get_trial(tid, nowait=True)
        runner.step()
        trial_info = ray.get(future)
        import ipdb; ipdb.set_trace()
        self.assertEqual(len(all_trials), 2)


    def testStopTrial(self):
        runner, manager = self.basicSetup()
        future = manager.get_all_trials(nowait=True)
        runner.step()
        all_trials = ray.get(future)
        tid = all_trials[0][0]
        manager.stop_trial(tid, nowait=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
