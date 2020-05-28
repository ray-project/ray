import gym
from gym.spaces import Box, Discrete, Tuple
import numpy as np
import unittest

import ray
from ray.rllib.models import ModelCatalog, MODEL_DEFAULTS, ActionDistribution
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.models.preprocessors import (NoPreprocessor, OneHotPreprocessor,
                                            Preprocessor)
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.tf.visionnet import VisionNetwork
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class CustomPreprocessor(Preprocessor):
    def _init_shape(self, obs_space, options):
        return [1]


class CustomPreprocessor2(Preprocessor):
    def _init_shape(self, obs_space, options):
        return [1]


class CustomModel(TFModelV2):
    def _build_layers(self, *args):
        return tf.constant([[0] * 5]), None


class CustomActionDistribution(TFActionDistribution):
    def __init__(self, inputs, model):
        # Store our output shape.
        custom_model_config = model.model_config["custom_model_config"]
        if "output_dim" in custom_model_config:
            self.output_shape = tf.concat(
                [tf.shape(inputs)[:1], custom_model_config["output_dim"]],
                axis=0)
        else:
            self.output_shape = tf.shape(inputs)
        super().__init__(inputs, model)

    @staticmethod
    def required_model_output_shape(action_space, model_config=None):
        custom_model_config = model_config["custom_model_config"] or {}
        if custom_model_config is not None and \
                custom_model_config.get("output_dim"):
            return custom_model_config.get("output_dim")
        return action_space.shape

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return tf.random_uniform(self.output_shape)

    @override(ActionDistribution)
    def logp(self, x):
        return tf.zeros(self.output_shape)


class ModelCatalogTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def test_gym_preprocessors(self):
        p1 = ModelCatalog.get_preprocessor(gym.make("CartPole-v0"))
        self.assertEqual(type(p1), NoPreprocessor)

        p2 = ModelCatalog.get_preprocessor(gym.make("FrozenLake-v0"))
        self.assertEqual(type(p2), OneHotPreprocessor)

    def test_tuple_preprocessor(self):
        ray.init(object_store_memory=1000 * 1024 * 1024)

        class TupleEnv:
            def __init__(self):
                self.observation_space = Tuple(
                    [Discrete(5),
                     Box(0, 5, shape=(3, ), dtype=np.float32)])

        p1 = ModelCatalog.get_preprocessor(TupleEnv())
        self.assertEqual(p1.shape, (8, ))
        self.assertEqual(
            list(p1.transform((0, np.array([1, 2, 3])))),
            [float(x) for x in [1, 0, 0, 0, 0, 1, 2, 3]])

    def test_custom_preprocessor(self):
        ray.init(object_store_memory=1000 * 1024 * 1024)
        ModelCatalog.register_custom_preprocessor("foo", CustomPreprocessor)
        ModelCatalog.register_custom_preprocessor("bar", CustomPreprocessor2)
        env = gym.make("CartPole-v0")
        p1 = ModelCatalog.get_preprocessor(env, {"custom_preprocessor": "foo"})
        self.assertEqual(str(type(p1)), str(CustomPreprocessor))
        p2 = ModelCatalog.get_preprocessor(env, {"custom_preprocessor": "bar"})
        self.assertEqual(str(type(p2)), str(CustomPreprocessor2))
        p3 = ModelCatalog.get_preprocessor(env)
        self.assertEqual(type(p3), NoPreprocessor)

    def test_default_models(self):
        ray.init(object_store_memory=1000 * 1024 * 1024)

        with tf.variable_scope("test1"):
            p1 = ModelCatalog.get_model_v2(
                obs_space=Box(0, 1, shape=(3, ), dtype=np.float32),
                action_space=Discrete(5),
                num_outputs=5,
                model_config={})
            self.assertEqual(type(p1), FullyConnectedNetwork)

        with tf.variable_scope("test2"):
            p2 = ModelCatalog.get_model_v2(
                obs_space=Box(0, 1, shape=(84, 84, 3), dtype=np.float32),
                action_space=Discrete(5),
                num_outputs=5,
                model_config={})
            self.assertEqual(type(p2), VisionNetwork)

    def test_custom_model(self):
        ray.init(object_store_memory=1000 * 1024 * 1024)
        ModelCatalog.register_custom_model("foo", CustomModel)
        p1 = ModelCatalog.get_model_v2(
            obs_space=Box(0, 1, shape=(3, ), dtype=np.float32),
            action_space=Discrete(5),
            num_outputs=5,
            model_config={"custom_model": "foo"})
        self.assertEqual(str(type(p1)), str(CustomModel))

    def test_custom_action_distribution(self):
        class Model():
            pass

        ray.init(
            object_store_memory=1000 * 1024 * 1024,
            ignore_reinit_error=True)  # otherwise fails sometimes locally
        # registration
        ModelCatalog.register_custom_action_dist("test",
                                                 CustomActionDistribution)
        action_space = Box(0, 1, shape=(5, 3), dtype=np.float32)

        # test retrieving it
        model_config = MODEL_DEFAULTS.copy()
        model_config["custom_action_dist"] = "test"
        dist_cls, param_shape = ModelCatalog.get_action_dist(
            action_space, model_config)
        self.assertEqual(str(dist_cls), str(CustomActionDistribution))
        self.assertEqual(param_shape, action_space.shape)

        # test the class works as a distribution
        dist_input = tf.placeholder(tf.float32, (None, ) + param_shape)
        model = Model()
        model.model_config = model_config
        dist = dist_cls(dist_input, model=model)
        self.assertEqual(dist.sample().shape[1:], dist_input.shape[1:])
        self.assertIsInstance(dist.sample(), tf.Tensor)
        with self.assertRaises(NotImplementedError):
            dist.entropy()

        # test passing the options to it
        model_config["custom_model_config"].update({"output_dim": (3, )})
        dist_cls, param_shape = ModelCatalog.get_action_dist(
            action_space, model_config)
        self.assertEqual(param_shape, (3, ))
        dist_input = tf.placeholder(tf.float32, (None, ) + param_shape)
        model.model_config = model_config
        dist = dist_cls(dist_input, model=model)
        self.assertEqual(dist.sample().shape[1:], dist_input.shape[1:])
        self.assertIsInstance(dist.sample(), tf.Tensor)
        with self.assertRaises(NotImplementedError):
            dist.entropy()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
