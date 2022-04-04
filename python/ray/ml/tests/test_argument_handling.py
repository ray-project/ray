import unittest

import ray.ml
from ray.ml import Checkpoint
from ray.ml.exceptions import TrainerConfigError
from ray.ml.trainer import Trainer
from ray.ml.preprocessor import Preprocessor


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

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(run_config={})

        # Succeed
        DummyTrainer(run_config=None)

        # Succeed
        DummyTrainer(run_config=ray.ml.RunConfig())

    def testScalingConfig(self):
        with self.assertRaises(TrainerConfigError):
            DummyTrainer(scaling_config="invalid")

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(scaling_config=False)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(scaling_config=True)

        # Succeed
        DummyTrainer(scaling_config={})

        # Succeed
        DummyTrainer(scaling_config=None)

        # Succeed
        DummyTrainer(scaling_config=ray.ml.ScalingConfig())

    def testDatasets(self):
        with self.assertRaises(TrainerConfigError):
            DummyTrainer(datasets="invalid")

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(datasets=False)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(datasets=True)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(datasets={"test": "invalid"})

        # Succeed
        DummyTrainer(datasets=None)

        # Succeed
        DummyTrainer(datasets={"test": ray.data.from_items([0, 1])})

    def testPreprocessor(self):
        with self.assertRaises(TrainerConfigError):
            DummyTrainer(preprocessor="invalid")

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(preprocessor=False)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(preprocessor=True)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(preprocessor={})

        # Succeed
        DummyTrainer(preprocessor=None)

        # Succeed
        DummyTrainer(preprocessor=Preprocessor())

    def testResumeFromCheckpoint(self):
        with self.assertRaises(TrainerConfigError):
            DummyTrainer(resume_from_checkpoint="invalid")

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(resume_from_checkpoint=False)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(resume_from_checkpoint=True)

        with self.assertRaises(TrainerConfigError):
            DummyTrainer(resume_from_checkpoint={})

        # Succeed
        DummyTrainer(resume_from_checkpoint=None)

        # Succeed
        DummyTrainer(resume_from_checkpoint=Checkpoint.from_dict({"empty": ""}))
