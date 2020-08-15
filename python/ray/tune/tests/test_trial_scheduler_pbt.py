import numpy as np
import os
import pickle
import random
import unittest
import sys

import ray
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
            pickle.dump((self.a, self.b, self.iter), fp)
        return tmp_checkpoint_dir

    def load_checkpoint(self, tmp_checkpoint_dir):
        checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.mock")
        with open(checkpoint_path, "rb") as fp:
            self.a, self.b, self.iter = pickle.load(fp)


def MockTrainingFunc(config, checkpoint_dir=None):
    iter = 0
    a = config["a"]
    b = config["b"]

    if checkpoint_dir:
        checkpoint_path = os.path.join(checkpoint_dir, "model.mock")
        with open(checkpoint_path, "rb") as fp:
            a, b, iter = pickle.load(fp)

    while True:
        iter += 1
        with tune.checkpoint_dir(step=iter) as checkpoint_dir:
            checkpoint_path = os.path.join(checkpoint_dir, "model.mock")
            with open(checkpoint_path, "wb") as fp:
                pickle.dump((a, b, iter), fp)
        tune.report(mean_accuracy=(a - iter) * b)


class MockParam(object):
    def __init__(self, params):
        self._params = params
        self._index = 0

    def __call__(self, *args, **kwargs):
        val = self._params[self._index % len(self._params)]
        self._index += 1
        return val


class PopulationBasedTrainingResumeTest(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

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
            checkpoint_freq=1,
            checkpoint_at_end=True,
            keep_checkpoints_num=1,
            checkpoint_score_attr="min-training_iteration",
            scheduler=scheduler,
            name="testPermutationContinuation",
            stop={"training_iteration": 5})

    def testPermutationContinuationFunc(self):
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
            MockTrainingFunc,
            config={
                "a": tune.sample_from(lambda _: param_a()),
                "b": tune.sample_from(lambda _: param_b()),
                "c": 1
            },
            fail_fast=True,
            num_samples=4,
            keep_checkpoints_num=1,
            checkpoint_score_attr="min-training_iteration",
            scheduler=scheduler,
            name="testPermutationContinuationFunc",
            stop={"training_iteration": 3})


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
