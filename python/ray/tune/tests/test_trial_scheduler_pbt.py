import numpy as np
import os
import pickle
import random
import unittest
import sys

from ray import tune
from ray.tune.schedulers import PopulationBasedTraining


class MockTrainable(tune.Trainable):
    def setup(self, config):
        self.iter = 0
        self.a = config["a"]
        self.b = config["b"]
        self.c = config["c"]

    def step(self):
        self.iter += 1
        return {"mean_accuracy": (self.a - self.iter) * self.b}

    def save_checkpoint(self, tmp_checkpoint_dir):
        checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.mock")
        with open(checkpoint_path, "wb") as fp:
            pickle.dump((self.a, self.b), fp)
        return tmp_checkpoint_dir

    def load_checkpoint(self, tmp_checkpoint_dir):
        checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.mock")
        with open(checkpoint_path, "rb") as fp:
            self.a, self.b = pickle.load(fp)


class MockParam(object):
    def __init__(self, params):
        self._params = params
        self._index = 0

    def __call__(self, *args, **kwargs):
        val = self._params[self._index % len(self._params)]
        self._index += 1
        return val


class PopulationBasedTrainingResumeTest(unittest.TestCase):
    def testPermutationContinuation(self):
        """
        Tests continuation of runs after permutation.
        Sometimes, runs were continued from deleted checkpoints.
        This deterministic initialisation would fail when the
        fix was not applied.
        See issues #9036, #9036
        """
        scheduler = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="mean_accuracy",
            mode="max",
            perturbation_interval=1,
            log_config=True,
            hyperparam_mutations={"c": lambda: 1})

        param_a = MockParam([10, 20, 30, 40])
        param_b = MockParam([1.2, 0.9, 1.1, 0.8])

        random.seed(100)
        np.random.seed(1000)
        tune.run(
            MockTrainable,
            config={
                "a": tune.sample_from(lambda _: param_a()),
                "b": tune.sample_from(lambda _: param_b()),
                "c": 1
            },
            fail_fast=True,
            num_samples=20,
            global_checkpoint_period=1,
            checkpoint_freq=1,
            checkpoint_at_end=True,
            keep_checkpoints_num=1,
            checkpoint_score_attr="min-training_iteration",
            scheduler=scheduler,
            name="testPermutationContinuation",
            stop={"training_iteration": 5})


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
