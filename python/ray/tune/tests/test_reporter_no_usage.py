from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
from ray.tune import Experiment


class TuneServerSuite(unittest.TestCase):
    def test_reporter_no_usage(self):
        ray.init()

        def run_task(config, reporter):
            print('hello')

        experiment = Experiment(run=run_task, name="ray_crash_repro")
        ray.tune.run(experiment)
        print("end")


if __name__ == "__main__":
    unittest.main(verbosity=2)
