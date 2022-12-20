import pytest
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.configs.pi import PolicyGradientPiConfig
import gym
import math
import torch
import numpy as np

from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.nested_dict import NestedDict


primitive_spaces = [
    # Discrete
    gym.spaces.Discrete(3),
    gym.spaces.MultiBinary(4),
    gym.spaces.MultiDiscrete([1, 7, 3]),
    # Continuous
    gym.spaces.Box(shape=(1,), dtype=np.float16, low=2, high=3),
    gym.spaces.Box(shape=(1,), dtype=np.float32, low=-1, high=0),
    gym.spaces.Box(shape=(1,), dtype=np.float64, low=10, high=10),
]

container_spaces = [
    gym.spaces.Tuple(tuple(primitive_spaces)),
    gym.spaces.Dict({i: space for i, space in enumerate(primitive_spaces)}),
]


@pytest.mark.parametrize("action_space", primitive_spaces)
def test_pg_primitive_build(action_space):
    """Ensure that the default config builds for all primitive action spaces."""
    input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=2)})
    PolicyGradientPiConfig().build(input_spec, action_space)


@pytest.mark.parametrize("container_space", container_spaces)
def test_pg_container_build(container_space):
    """Ensure that the default config builds for all container action spaces."""
    input_spec = ModelSpec({"bork": TorchTensorSpec("a, b, c", c=2)})
    PolicyGradientPiConfig().build(input_spec, container_space)


@pytest.mark.parametrize("space", primitive_spaces)
def test_pg_primitive_space(space):
    """Test that we can build action spaces for all the gym spaces,
    and that we produce sample()'able distributions."""
    input_spec = ModelSpec({"bork": TorchTensorSpec("b, c", c=5)})
    c = PolicyGradientPiConfig()
    inputs = NestedDict({"bork": torch.ones(100, 5)})
    m = c.build(input_spec, space)
    outputs, _ = m.unroll(inputs, NestedDict())
    dist = outputs[c.output_key]
    actions = dist.sample().numpy().astype(space.dtype)
    for action in actions:
        if isinstance(space, gym.spaces.Discrete):
            # We always output [batch, action]
            # but gym expects a single output (e.g. int(3)) for Discrete space
            action = action.item()
        assert space.contains(action), f"Space {space} does not contain action {action}"


@pytest.mark.parametrize("space", container_spaces)
def test_pg_container_space(space):
    """Test that we can build action spaces for all the gym spaces,
    and that we produce sample()'able distributions."""
    input_spec = ModelSpec({"bork": TorchTensorSpec("b, c", c=5)})
    c = PolicyGradientPiConfig()
    inputs = NestedDict({"bork": torch.ones(100, 5)})
    m = c.build(input_spec, space)
    outputs, _ = m.unroll(inputs, NestedDict())
    dist = outputs[c.output_key]
    actions = dist.sample()
    for action in actions:
        if isinstance(space, gym.spaces.Tuple):
            space_vals = space
        elif isinstance(space, gym.spaces.Dict):
            space_vals = space.values()
        # Compute the output action shapes. We use this instead of
        # gym.spaces.utils.flatdim because it will flatten
        # Discrete(4) -> OneHot(4) while we output randint(0, 3)
        # for efficiency purposes
        shapes = [math.prod(s.shape) if s.shape != () else 1 for s in space_vals]
        subactions = action.split(shapes, dim=-1)
        for subaction, subspace in zip(subactions, space_vals):
            subaction = subaction.numpy().astype(subspace.dtype)
            if isinstance(subspace, gym.spaces.Discrete):
                # We always output [batch, action]
                # but gym expects a single output (e.g. int(3)) for Discrete space
                subaction = subaction.item()
            # With a mix of continuous and discrete actions, the output vector will
            # be a float. This fails when checking the action space.
            # Convert to correct dtypes
            assert subspace.contains(
                subaction
            ), f"Space {subspace} does not contain action {subaction}"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
