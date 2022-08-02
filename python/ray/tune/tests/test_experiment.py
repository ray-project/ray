import unittest
import threading

import ray
from ray.rllib import _register_all
from ray.tune import register_trainable
from ray.tune.experiment import Experiment, _convert_to_experiment_list
from ray.tune.error import TuneError
from ray.tune.utils import diagnose_serialization


class ExperimentTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def setUp(self):
        def train(config, reporter):
            for i in range(100):
                reporter(timesteps_total=i)

        register_trainable("f1", train)

    def testConvertExperimentFromExperiment(self):
        exp1 = Experiment(
            **{"name": "foo", "run": "f1", "config": {"script_min_iter_time_s": 0}}
        )
        result = _convert_to_experiment_list(exp1)
        self.assertEqual(len(result), 1)
        self.assertEqual(type(result), list)

    def testConvertExperimentNone(self):
        result = _convert_to_experiment_list(None)
        self.assertEqual(len(result), 0)
        self.assertEqual(type(result), list)

    def testConvertExperimentList(self):
        exp1 = Experiment(
            **{"name": "foo", "run": "f1", "config": {"script_min_iter_time_s": 0}}
        )
        result = _convert_to_experiment_list([exp1, exp1])
        self.assertEqual(len(result), 2)
        self.assertEqual(type(result), list)

    def testConvertExperimentJSON(self):
        experiment = {
            "name": {"run": "f1", "config": {"script_min_iter_time_s": 0}},
            "named": {"run": "f1", "config": {"script_min_iter_time_s": 0}},
        }
        result = _convert_to_experiment_list(experiment)
        self.assertEqual(len(result), 2)
        self.assertEqual(type(result), list)

    def testConvertExperimentIncorrect(self):
        self.assertRaises(TuneError, lambda: _convert_to_experiment_list("hi"))


class ValidateUtilTest(unittest.TestCase):
    def testDiagnoseSerialization(self):

        # this is not serializable
        e = threading.Event()

        def test(config):
            print(e)

        assert diagnose_serialization(test) is not True

        # should help identify that 'e' should be moved into
        # the `test` scope.

        # correct implementation
        def test(config):
            e = threading.Event()
            print(e)

        assert diagnose_serialization(test) is True


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
