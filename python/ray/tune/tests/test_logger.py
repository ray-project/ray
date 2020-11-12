import csv
import json
import os
from collections import namedtuple
import unittest
import tempfile
import shutil
import numpy as np
from ray.cloudpickle import cloudpickle

from ray.tune.logger import CSVExperimentLogger, JsonExperimentLogger, \
    JsonLogger, CSVLogger, \
    TBXLogger
from ray.tune.result import EXPR_PARAM_FILE, EXPR_PARAM_PICKLE_FILE, \
    EXPR_PROGRESS_FILE, \
    EXPR_RESULT_FILE


class Trial(
        namedtuple("MockTrial", ["evaluated_params", "trial_id", "logdir"])):
    @property
    def config(self):
        return self.evaluated_params

    def init_logdir(self):
        return

    def __hash__(self):
        return hash(self.trial_id)


def result(t, rew, **kwargs):
    results = dict(
        time_total_s=t,
        episode_reward_mean=rew,
        mean_accuracy=rew * 2,
        training_iteration=int(t))
    results.update(kwargs)
    return results


class LoggerSuite(unittest.TestCase):
    """Test built-in loggers."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def testLegacyCSV(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(
            evaluated_params=config, trial_id="csv", logdir=self.test_dir)
        logger = CSVLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 5))
        logger.on_result(result(2, 6, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

        results = []
        result_file = os.path.join(self.test_dir, EXPR_PROGRESS_FILE)
        with open(result_file, "rt") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                results.append(row)

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual(
            [int(row["episode_reward_mean"]) for row in results], [4, 5, 6])

    def testCSV(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(
            evaluated_params=config, trial_id="csv", logdir=self.test_dir)
        logger = CSVExperimentLogger()
        logger.on_trial_result(0, [], t, result(0, 4))
        logger.on_trial_result(1, [], t, result(1, 5))
        logger.on_trial_result(
            2, [], t, result(2, 6, score=[1, 2, 3], hello={"world": 1}))

        logger.on_trial_complete(3, [], t)

        results = []
        result_file = os.path.join(self.test_dir, EXPR_PROGRESS_FILE)
        with open(result_file, "rt") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                results.append(row)

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual(
            [int(row["episode_reward_mean"]) for row in results], [4, 5, 6])

    def testJSONLegacyLogger(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(
            evaluated_params=config, trial_id="json", logdir=self.test_dir)
        logger = JsonLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(1, 5))
        logger.on_result(result(2, 6, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

        # Check result logs
        results = []
        result_file = os.path.join(self.test_dir, EXPR_RESULT_FILE)
        with open(result_file, "rt") as fp:
            for row in fp.readlines():
                results.append(json.loads(row))

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual(
            [int(row["episode_reward_mean"]) for row in results], [4, 5, 6])

        # Check json saved config file
        config_file = os.path.join(self.test_dir, EXPR_PARAM_FILE)
        with open(config_file, "rt") as fp:
            loaded_config = json.load(fp)

        self.assertEqual(loaded_config, config)

        # Check pickled config file
        config_file = os.path.join(self.test_dir, EXPR_PARAM_PICKLE_FILE)
        with open(config_file, "rb") as fp:
            loaded_config = cloudpickle.load(fp)

        self.assertEqual(loaded_config, config)

    def testJSON(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(
            evaluated_params=config, trial_id="json", logdir=self.test_dir)
        logger = JsonExperimentLogger()
        logger.on_trial_result(0, [], t, result(0, 4))
        logger.on_trial_result(1, [], t, result(1, 5))
        logger.on_trial_result(
            2, [], t, result(2, 6, score=[1, 2, 3], hello={"world": 1}))

        logger.on_trial_complete(3, [], t)

        # Check result logs
        results = []
        result_file = os.path.join(self.test_dir, EXPR_RESULT_FILE)
        with open(result_file, "rt") as fp:
            for row in fp.readlines():
                results.append(json.loads(row))

        self.assertEqual(len(results), 3)
        self.assertSequenceEqual(
            [int(row["episode_reward_mean"]) for row in results], [4, 5, 6])

        # Check json saved config file
        config_file = os.path.join(self.test_dir, EXPR_PARAM_FILE)
        with open(config_file, "rt") as fp:
            loaded_config = json.load(fp)

        self.assertEqual(loaded_config, config)

        # Check pickled config file
        config_file = os.path.join(self.test_dir, EXPR_PARAM_PICKLE_FILE)
        with open(config_file, "rb") as fp:
            loaded_config = cloudpickle.load(fp)

        self.assertEqual(loaded_config, config)

    def testTBX(self):
        config = {
            "a": 2,
            "b": [1, 2],
            "c": {
                "c": {
                    "D": 123
                }
            },
            "d": np.int64(1),
            "e": np.bool8(True)
        }
        t = Trial(
            evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(1, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

    def testBadTBX(self):
        config = {"b": (1, 2, 3)}
        t = Trial(
            evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        with self.assertLogs("ray.tune.logger", level="INFO") as cm:
            logger.close()
        assert "INFO" in cm.output[0]

        config = {"None": None}
        t = Trial(
            evaluated_params=config, trial_id="tbx", logdir=self.test_dir)
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        with self.assertLogs("ray.tune.logger", level="INFO") as cm:
            logger.close()
        assert "INFO" in cm.output[0]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
