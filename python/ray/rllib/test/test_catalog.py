import gym
import numpy as np
import tensorflow as tf
import unittest

import ray
from ray.tune.registry import get_registry

from ray.rllib.models import ModelCatalog
from ray.rllib.models.model import Model
from ray.rllib.models.preprocessors import (
    NoPreprocessor, OneHotPreprocessor, Preprocessor)
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.visionnet import VisionNetwork


class CustomPreprocessor(Preprocessor):
    pass


class CustomPreprocessor2(Preprocessor):
    pass


class CustomModel(Model):
    def _init(self, *args):
        return None, None


class ModelCatalogTest(unittest.TestCase):
    def tearDown(self):
        ray.worker.cleanup()

    def testGymPreprocessors(self):
        p1 = ModelCatalog.get_preprocessor(
            get_registry(), gym.make("CartPole-v0"))
        assert type(p1) == NoPreprocessor

        p2 = ModelCatalog.get_preprocessor(
            get_registry(), gym.make("FrozenLake-v0"))
        assert type(p2) == OneHotPreprocessor

    def testCustomPreprocessor(self):
        ray.init()
        ModelCatalog.register_custom_preprocessor("foo", CustomPreprocessor)
        ModelCatalog.register_custom_preprocessor("bar", CustomPreprocessor2)
        env = gym.make("CartPole-v0")
        p1 = ModelCatalog.get_preprocessor(
            get_registry(), env, {"custom_preprocessor": "foo"})
        assert type(p1) == CustomPreprocessor
        p2 = ModelCatalog.get_preprocessor(
            get_registry(), env, {"custom_preprocessor": "bar"})
        assert type(p2) == CustomPreprocessor2
        p3 = ModelCatalog.get_preprocessor(get_registry(), env)
        assert type(p3) == NoPreprocessor

    def testDefaultModels(self):
        ray.init()

        with tf.variable_scope("test1"):
            p1 = ModelCatalog.get_model(
                get_registry(), np.zeros((10, 3), dtype=np.float32), 5)
            assert type(p1) == FullyConnectedNetwork

        with tf.variable_scope("test2"):
            p2 = ModelCatalog.get_model(
                get_registry(), np.zeros((10, 80, 80, 3), dtype=np.float32), 5)
            assert type(p2) == VisionNetwork

    def testCustomModel(self):
        ray.init()
        ModelCatalog.register_custom_model("foo", CustomModel)
        p1 = ModelCatalog.get_model(
            get_registry(), 1, 5, {"custom_model": "foo"})
        assert type(p1) == CustomModel


if __name__ == "__main__":
    unittest.main(verbosity=2)
