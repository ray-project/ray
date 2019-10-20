from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import unittest
import tempfile
import shutil

from ray.tune.logger import tf2_compat_logger, JsonLogger, CSVLogger

Trial = namedtuple("MockTrial", ["evaluated_params", "trial_id"])


def result(t, rew):
    return dict(
        time_total_s=t,
        episode_reward_mean=rew,
        mean_accuracy=rew * 2,
        training_iteration=int(t))


class LoggerSuite(unittest.TestCase):
    """Test built-in loggers."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def testTensorBoardLogger(self):
        config = {"a": 2, "b": 5}
        t = Trial(evaluated_params=config, trial_id=5342)
        logger = tf2_compat_logger(
            config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 4))
        logger.close()

    def testCSV(self):
        config = {"a": 2, "b": 5}
        t = Trial(evaluated_params=config, trial_id="csv")
        logger = CSVLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 4))
        logger.close()

    def testJSON(self):
        config = {"a": 2, "b": 5}
        t = Trial(evaluated_params=config, trial_id="json")
        logger = JsonLogger(config=config, logdir=self.test_dir, trial=t)
        logger.on_result(result(2, 4))
        logger.on_result(result(2, 4))
        logger.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
