from collections import namedtuple
import unittest
import tempfile
import shutil

from ray.tune.logger import JsonLogger, CSVLogger, TBXLogger

Trial = namedtuple("MockTrial", ["evaluated_params", "trial_id"])


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

    def testCSV(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="csv")
        logger = CSVLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

    def testJSON(self):
        config = {"a": 2, "b": 5, "c": {"c": {"D": 123}, "e": None}}
        t = Trial(evaluated_params=config, trial_id="json")
        logger = JsonLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(1, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

    def testTBX(self):
        config = {"a": 2, "b": [1, 2], "c": {"c": {"D": 123}}}
        t = Trial(evaluated_params=config, trial_id="tbx")
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(1, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        logger.close()

    def testBadTBX(self):
        config = {"b": (1, 2, 3)}
        t = Trial(evaluated_params=config, trial_id="tbx")
        logger = TBXLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(0, 4))
        logger.on_result(result(2, 4, score=[1, 2, 3], hello={"world": 1}))
        with self.assertLogs("ray.tune.logger", level="INFO") as cm:
            logger.close()
        assert "INFO" in cm.output[0]

        config = {"None": None}
        t = Trial(evaluated_params=config, trial_id="tbx")
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
