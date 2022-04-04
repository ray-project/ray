import unittest

import ray.ml
from ray.ml.exceptions import TrainerConfigError
from ray.ml.trainer import Trainer


class DummyTrainer(Trainer):
    def training_loop(self) -> None:
        pass


class ArgumentHandlingTest(unittest.TestCase):
    def testRunConfig(self):
        with self.assertRaises(TrainerConfigError):
            DummyTrainer(run_config="invalid")

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(run_config=False)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(run_config=True)

        # Succeed
        DummyTrainer(run_config=None)

        # Succeed
        DummyTrainer(run_config=ray.ml.RunConfig())
