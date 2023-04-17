from functools import partial
from gymnasium.spaces import Box, Dict, Discrete, Tuple
import numpy as np
import unittest

import ray
from ray.rllib.models import ActionDistribution, ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.preprocessors import (
    Preprocessor,
    TupleFlatteningPreprocessor,
)
from ray.rllib.models.tf.tf_action_dist import (
    MultiActionDistribution,
    TFActionDistribution,
)
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.space_utils import get_dummy_batch_for_space
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


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
                [tf.shape(inputs)[:1], custom_model_config["output_dim"]], axis=0
            )
        else:
            self.output_shape = tf.shape(inputs)
        super().__init__(inputs, model)

    @staticmethod
    def required_model_output_shape(action_space, model_config=None):
        custom_model_config = model_config["custom_model_config"] or {}
        if custom_model_config is not None and custom_model_config.get("output_dim"):
            return custom_model_config.get("output_dim")
        return action_space.shape

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return tf.random.uniform(self.output_shape)

    @override(ActionDistribution)
    def logp(self, x):
        return tf.zeros(self.output_shape)


class CustomMultiActionDistribution(MultiActionDistribution):
    @override(MultiActionDistribution)
    def entropy(self):
        raise NotImplementedError


class TestModelCatalog(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def test_default_models(self):
        ray.init(object_store_memory=1000 * 1024 * 1024)

        # Build test cases
        flat_input_case = {
            "obs_space": Box(0, 1, shape=(3,), dtype=np.float32),
            "action_space": Box(0, 1, shape=(4,)),
            "num_outputs": 4,
            "expected_model": "FullyConnectedNetwork",
        }
        img_input_case = {
            "obs_space": Box(0, 1, shape=(84, 84, 3), dtype=np.float32),
            "action_space": Discrete(5),
            "num_outputs": 5,
            "expected_model": "VisionNetwork",
        }
        complex_obs_space = Tuple(
            [
                Box(0, 1, shape=(3,), dtype=np.float32),
                Box(0, 1, shape=(4,), dtype=np.float32),
                Discrete(3),
            ]
        )
        obs_prep = TupleFlatteningPreprocessor(complex_obs_space)
        flat_complex_input_case = {
            "obs_space": obs_prep.observation_space,
            "action_space": Box(0, 1, shape=(5,)),
            "num_outputs": 5,
            "expected_model": "FullyConnectedNetwork",
        }
        nested_complex_input_case = {
            "obs_space": Tuple(
                [
                    Box(0, 1, shape=(3,), dtype=np.float32),
                    Discrete(3),
                    Tuple(
                        [
                            Box(0, 1, shape=(84, 84, 3), dtype=np.float32),
                            Box(0, 1, shape=(84, 84, 3), dtype=np.float32),
                        ]
                    ),
                ]
            ),
            "action_space": Box(0, 1, shape=(7,)),
            "num_outputs": 7,
            "expected_model": "ComplexInputNetwork",
        }

        # Define which tests to run per framework
        test_suite = {
            "tf": [
                flat_input_case,
                img_input_case,
                flat_complex_input_case,
                nested_complex_input_case,
            ],
            "tf2": [
                flat_input_case,
                img_input_case,
                flat_complex_input_case,
                nested_complex_input_case,
            ],
            "torch": [
                flat_input_case,
                img_input_case,
                flat_complex_input_case,
                nested_complex_input_case,
            ],
            "jax": [
                flat_input_case,
            ],
        }

        for fw, test_cases in test_suite.items():
            for test in test_cases:
                model_config = {}
                if test["expected_model"] == "ComplexInputNetwork":
                    model_config["fcnet_hiddens"] = [256, 256]
                m = ModelCatalog.get_model_v2(
                    obs_space=test["obs_space"],
                    action_space=test["action_space"],
                    num_outputs=test["num_outputs"],
                    model_config=model_config,
                    framework=fw,
                )
                self.assertTrue(test["expected_model"] in type(m).__name__)
                # Do a test forward pass.
                batch_size = 16
                obs = get_dummy_batch_for_space(
                    test["obs_space"],
                    batch_size=batch_size,
                    fill_value="random",
                )
                if fw == "torch":
                    obs = convert_to_torch_tensor(obs)
                out, state_outs = m({"obs": obs})
                self.assertTrue(out.shape == (batch_size, test["num_outputs"]))
                self.assertTrue(state_outs == [])

    def test_custom_model(self):
        ray.init(object_store_memory=1000 * 1024 * 1024)
        ModelCatalog.register_custom_model("foo", CustomModel)
        p1 = ModelCatalog.get_model_v2(
            obs_space=Box(0, 1, shape=(3,), dtype=np.float32),
            action_space=Discrete(5),
            num_outputs=5,
            model_config={"custom_model": "foo"},
        )
        self.assertEqual(str(type(p1)), str(CustomModel))

    def test_custom_action_distribution(self):
        class Model:
            pass

        ray.init(
            object_store_memory=1000 * 1024 * 1024, ignore_reinit_error=True
        )  # otherwise fails sometimes locally
        # registration
        ModelCatalog.register_custom_action_dist("test", CustomActionDistribution)
        action_space = Box(0, 1, shape=(5, 3), dtype=np.float32)

        # test retrieving it
        model_config = MODEL_DEFAULTS.copy()
        model_config["custom_action_dist"] = "test"
        dist_cls, param_shape = ModelCatalog.get_action_dist(action_space, model_config)
        self.assertEqual(str(dist_cls), str(CustomActionDistribution))
        self.assertEqual(param_shape, action_space.shape)

        # test the class works as a distribution
        dist_input = tf1.placeholder(tf.float32, (None,) + param_shape)
        model = Model()
        model.model_config = model_config
        dist = dist_cls(dist_input, model=model)
        self.assertEqual(dist.sample().shape[1:], dist_input.shape[1:])
        self.assertIsInstance(dist.sample(), tf.Tensor)
        with self.assertRaises(NotImplementedError):
            dist.entropy()

        # test passing the options to it
        model_config["custom_model_config"].update({"output_dim": (3,)})
        dist_cls, param_shape = ModelCatalog.get_action_dist(action_space, model_config)
        self.assertEqual(param_shape, (3,))
        dist_input = tf1.placeholder(tf.float32, (None,) + param_shape)
        model.model_config = model_config
        dist = dist_cls(dist_input, model=model)
        self.assertEqual(dist.sample().shape[1:], dist_input.shape[1:])
        self.assertIsInstance(dist.sample(), tf.Tensor)
        with self.assertRaises(NotImplementedError):
            dist.entropy()

    def test_custom_multi_action_distribution(self):
        class Model:
            pass

        ray.init(
            object_store_memory=1000 * 1024 * 1024, ignore_reinit_error=True
        )  # otherwise fails sometimes locally
        # registration
        ModelCatalog.register_custom_action_dist("test", CustomMultiActionDistribution)
        s1 = Discrete(5)
        s2 = Box(0, 1, shape=(3,), dtype=np.float32)
        spaces = dict(action_1=s1, action_2=s2)
        action_space = Dict(spaces)
        # test retrieving it
        model_config = MODEL_DEFAULTS.copy()
        model_config["custom_action_dist"] = "test"
        dist_cls, param_shape = ModelCatalog.get_action_dist(action_space, model_config)
        self.assertIsInstance(dist_cls, partial)
        self.assertEqual(param_shape, s1.n + 2 * s2.shape[0])

        # test the class works as a distribution
        dist_input = tf1.placeholder(tf.float32, (None, param_shape))
        model = Model()
        model.model_config = model_config
        dist = dist_cls(dist_input, model=model)
        self.assertIsInstance(dist.sample(), dict)
        self.assertIn("action_1", dist.sample())
        self.assertIn("action_2", dist.sample())
        self.assertEqual(dist.sample()["action_1"].dtype, tf.int64)
        self.assertEqual(dist.sample()["action_2"].shape[1:], s2.shape)

        with self.assertRaises(NotImplementedError):
            dist.entropy()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
