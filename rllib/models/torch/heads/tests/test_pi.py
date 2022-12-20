from email.policy import Policy
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.torch.heads.pi import TorchPi
from ray.rllib.models.configs.pi import PolicyGradientPiConfig
import unittest
import gym
import torch
import numpy as np
import itertools

from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.nested_dict import NestedDict


class TestPi(unittest.TestCase):
    def setUp(self):
        self.primitive_spaces = [
            # Discrete
            # gym.spaces.Discrete(1, dtype=np.int8),
            # gym.spaces.Discrete(1, dtype=np.int16),
            # gym.spaces.Discrete(1, dtype=np.int32),
            # gym.spaces.Discrete(1, dtype=np.uint8),
            # gym.spaces.Discrete(1, dtype=np.uint16),
            # gym.spaces.Discrete(1, dtype=np.uint32),
            # gym.spaces.MultiBinary(2),
            gym.spaces.MultiDiscrete([1, 2]),
            # Continuous
            gym.spaces.Box(shape=(1,), dtype=np.float16, low=2, high=3),
            gym.spaces.Box(shape=(1,), dtype=np.float32, low=-1, high=0),
            gym.spaces.Box(shape=(1,), dtype=np.float64, low=10, high=10),
        ]

    def test_pg_build_default(self):
        """Ensure that the default config builds."""
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=2)})
        action_space = gym.spaces.Discrete(1)
        PolicyGradientPiConfig().build(input_spec, action_space)

    def test_all_primitive_spaces_individually(self):
        """Test that we can build action spaces for all the gym spaces,
        and that we produce sample()'able distributions."""
        input_spec = ModelSpec({"bork": TorchTensorSpec("b, c", c=5)})
        c = PolicyGradientPiConfig()
        for s in self.primitive_spaces:
            inputs = NestedDict({"bork": torch.ones(4, 5)})
            m = c.build(input_spec, s)
            outputs, _ = m.unroll(inputs, NestedDict())
            dist = outputs[c.output_key]
            # action_dims = m.dist_configs.
            actions = dist.sample().numpy().astype(s.dtype)
            for action in actions:
                self.assertTrue(
                    s.contains(action), f"Space {s} does not contain action {action}"
                )

    '''
    def test_tuple_spaces(self):
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=5)})
        c = PolicyGradientPiConfig()
        space = gym.spaces.Tuple(self.primitive_spaces)
        inputs = NestedDict({"bork": torch.ones(3, 4, 5)})
        m = c.build(input_spec, s)
        outputs, _ = m.unroll(inputs, NestedDict())
        dist = outputs[c.output_key]
        action = dist.sample()



    def test_wrapped_individual_primitive_spaces(self):
        """Test that we can build action spaces for e.g. Tuple(Discrete(2))"""
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=5)})
        c = PolicyGradientPiConfig()
        for s in self.primitive_spaces:
            inputs = NestedDict({"bork": torch.ones(3, 4, 5)})
            m = c.build(input_spec, s)
            outputs, _ = m.unroll(inputs, NestedDict())
            dist = outputs[c.output_key]
            action = dist.sample()

    def test_combined_spaces(self):
        combined_spaces = itertools.permutations(self.spaces)


    def test_categorical(self):
        """Test """
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=5)})
        action_space = gym.spaces.Discrete(2)
        c = PolicyGradientPiConfig()
        m = c.build(input_spec, action_space)
        inputs = NestedDict({"bork": torch.ones(3, 4, 5)})
        outputs, _ = m.unroll(inputs, NestedDict())
        dist = outputs[c.output_key]
        action = dist.sample()
    '''

    def test_squashed_gaussian(self):
        """Ensure the continuous actions are squashed correctly"""
        input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=5)})
        action_space = (gym.spaces.Box(shape=(1,), dtype=np.float32, low=5, high=10),)
        c = PolicyGradientPiConfig()
        m = c.build(input_spec, action_space)
        inputs = NestedDict({"bork": torch.ones(3, 4, 5)})
        outputs, _ = m.unroll(inputs, NestedDict())
        dist = outputs[c.output_key]
        action = dist.sample()
        self.assertEqual(action.shape, (3, 4, 1))
        self.assertTrue(torch.all(action >= 5))
        self.assertTrue(torch.all(action <= 10))
