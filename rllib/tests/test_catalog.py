import gym
import numpy as np
import unittest
from gym.spaces import Box, Discrete, Tuple

import ray

from ray.rllib.models import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.model import Model
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.models.preprocessors import (NoPreprocessor, OneHotPreprocessor,
                                            Preprocessor)
from ray.rllib.models.tf.fcnet_v1 import FullyConnectedNetwork
from ray.rllib.models.tf.visionnet_v1 import VisionNetwork
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class CustomPreprocessor(Preprocessor):
    def _init_shape(self, obs_space, options):
        return [1]


class CustomPreprocessor2(Preprocessor):
    def _init_shape(self, obs_space, options):
        return [1]


class CustomModel(Model):
    def _build_layers(self, *args):
        return tf.constant([[0] * 5]), None


class CustomActionDistribution(TFActionDistribution):
    @staticmethod
    def required_model_output_shape(action_space, model_config=None):
        custom_options = model_config["custom_options"] or {}
        if custom_options is not None and custom_options.get("output_dim"):
            return custom_options.get("output_dim")
        return action_space.shape

    def _build_sample_op(self):
        custom_options = self.model.model_config["custom_options"]
        if "output_dim" in custom_options:
            output_shape = tf.concat(
                [tf.shape(self.inputs)[:1], custom_options["output_dim"]],
                axis=0)
        else:
            output_shape = tf.shape(self.inputs)
        return tf.random_uniform(output_shape)


class ModelCatalogTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testGymPreprocessors(self):
        p1 = ModelCatalog.get_preprocessor(gym.make("CartPole-v0"))
        self.assertEqual(type(p1), NoPreprocessor)

        p2 = ModelCatalog.get_preprocessor(gym.make("FrozenLake-v0"))
        self.assertEqual(type(p2), OneHotPreprocessor)

    def testTuplePreprocessor(self):
        ray.init()

        class TupleEnv(object):
            def __init__(self):
                self.observation_space = Tuple(
                    [Discrete(5),
                     Box(0, 5, shape=(3, ), dtype=np.float32)])

        p1 = ModelCatalog.get_preprocessor(TupleEnv())
        self.assertEqual(p1.shape, (8, ))
        self.assertEqual(
            list(p1.transform((0, np.array([1, 2, 3])))),
            [float(x) for x in [1, 0, 0, 0, 0, 1, 2, 3]])

    def testCustomPreprocessor(self):
        ray.init()
        ModelCatalog.register_custom_preprocessor("foo", CustomPreprocessor)
        ModelCatalog.register_custom_preprocessor("bar", CustomPreprocessor2)
        env = gym.make("CartPole-v0")
        p1 = ModelCatalog.get_preprocessor(env, {"custom_preprocessor": "foo"})
        self.assertEqual(str(type(p1)), str(CustomPreprocessor))
        p2 = ModelCatalog.get_preprocessor(env, {"custom_preprocessor": "bar"})
        self.assertEqual(str(type(p2)), str(CustomPreprocessor2))
        p3 = ModelCatalog.get_preprocessor(env)
        self.assertEqual(type(p3), NoPreprocessor)

    def testDefaultModels(self):
        ray.init()

        with tf.variable_scope("test1"):
            p1 = ModelCatalog.get_model({
                "obs": tf.zeros((10, 3), dtype=tf.float32)
            }, Box(0, 1, shape=(3, ), dtype=np.float32), Discrete(5), 5, {})
            self.assertEqual(type(p1), FullyConnectedNetwork)

        with tf.variable_scope("test2"):
            p2 = ModelCatalog.get_model({
                "obs": tf.zeros((10, 84, 84, 3), dtype=tf.float32)
            }, Box(0, 1, shape=(84, 84, 3), dtype=np.float32), Discrete(5), 5,
                                        {})
            self.assertEqual(type(p2), VisionNetwork)

    def testCustomModel(self):
        ray.init()
        ModelCatalog.register_custom_model("foo", CustomModel)
        p1 = ModelCatalog.get_model({
            "obs": tf.constant([1, 2, 3])
        }, Box(0, 1, shape=(3, ), dtype=np.float32), Discrete(5), 5,
                                    {"custom_model": "foo"})
        self.assertEqual(str(type(p1)), str(CustomModel))

    def testCustomActionDistribution(self):
        class Model():
            pass

        ray.init()
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
        model_config["custom_options"].update({"output_dim": (3, )})
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
    unittest.main(verbosity=1)
