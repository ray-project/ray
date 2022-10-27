from abc import ABCMeta, abstractmethod
from gym.spaces import Discrete
import numpy as np
from pathlib import Path
import unittest

from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.test_utils import check, framework_iterator

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class DummyComponent:
    """A simple class that can be used for testing framework-agnostic logic.

    Implements a simple `add()` method for adding a value to
    `self.prop_b`.
    """

    def __init__(self, prop_a, prop_b=0.5, prop_c=None, framework="tf", **kwargs):
        self.framework = framework
        self.prop_a = prop_a
        self.prop_b = prop_b
        self.prop_c = prop_c or "default"
        self.prop_d = kwargs.pop("prop_d", 4)
        self.kwargs = kwargs

    def add(self, value):
        if self.framework == "tf":
            return self._add_tf(value)
        return self.prop_b + value

    def _add_tf(self, value):
        return tf.add(self.prop_b, value)


class NonAbstractChildOfDummyComponent(DummyComponent):
    pass


class AbstractDummyComponent(DummyComponent, metaclass=ABCMeta):
    """Used for testing `from_config()`."""

    @abstractmethod
    def some_abstract_method(self):
        raise NotImplementedError


class TestFrameWorkAgnosticComponents(unittest.TestCase):
    """
    Tests the Component base class to implement framework-agnostic functional
    units.
    """

    def test_dummy_components(self):

        # Bazel makes it hard to find files specified in `args`
        # (and `data`).
        # Use the true absolute path.
        script_dir = Path(__file__).parent
        abs_path = script_dir.absolute()

        for fw, sess in framework_iterator(session=True):
            # Try to create from an abstract class w/o default constructor.
            # Expect None.
            test = from_config({"type": AbstractDummyComponent, "framework": fw})
            check(test, None)

            # Create a Component via python API (config dict).
            component = from_config(
                dict(
                    type=DummyComponent, prop_a=1.0, prop_d="non_default", framework=fw
                )
            )
            check(component.prop_d, "non_default")

            # Create a tf Component from json file.
            config_file = str(abs_path.joinpath("dummy_config.json"))
            component = from_config(config_file, framework=fw)
            check(component.prop_c, "default")
            check(component.prop_d, 4)  # default
            value = component.add(3.3)
            if sess:
                value = sess.run(value)
            check(value, 5.3)  # prop_b == 2.0

            # Create a torch Component from yaml file.
            config_file = str(abs_path.joinpath("dummy_config.yml"))
            component = from_config(config_file, framework=fw)
            check(component.prop_a, "something else")
            check(component.prop_d, 3)
            value = component.add(1.2)
            if sess:
                value = sess.run(value)
            check(value, np.array([2.2]))  # prop_b == 1.0

            # Create tf Component from json-string (e.g. on command line).
            component = from_config(
                '{"type": "ray.rllib.utils.tests.'
                'test_framework_agnostic_components.DummyComponent", '
                '"prop_a": "A", "prop_b": -1.0, "prop_c": "non-default", '
                '"framework": "' + fw + '"}'
            )
            check(component.prop_a, "A")
            check(component.prop_d, 4)  # default
            value = component.add(-1.1)
            if sess:
                value = sess.run(value)
            check(value, -2.1)  # prop_b == -1.0

            # Test recognizing default module path.
            component = from_config(
                DummyComponent,
                '{"type": "NonAbstractChildOfDummyComponent", '
                '"prop_a": "A", "prop_b": -1.0, "prop_c": "non-default",'
                '"framework": "' + fw + '"}',
            )
            check(component.prop_a, "A")
            check(component.prop_d, 4)  # default
            value = component.add(-1.1)
            if sess:
                value = sess.run(value)
            check(value, -2.1)  # prop_b == -1.0

            # Test recognizing default package path.
            scope = None
            if sess:
                scope = tf1.variable_scope("exploration_object")
                scope.__enter__()
            component = from_config(
                Exploration,
                {
                    "type": "EpsilonGreedy",
                    "action_space": Discrete(2),
                    "framework": fw,
                    "num_workers": 0,
                    "worker_index": 0,
                    "policy_config": {},
                    "model": None,
                },
            )
            if scope:
                scope.__exit__(None, None, None)
            check(component.epsilon_schedule.outside_value, 0.05)  # default

            # Create torch Component from yaml-string.
            component = from_config(
                "type: ray.rllib.utils.tests."
                "test_framework_agnostic_components.DummyComponent\n"
                "prop_a: B\nprop_b: -1.5\nprop_c: non-default\nframework: "
                "{}".format(fw)
            )
            check(component.prop_a, "B")
            check(component.prop_d, 4)  # default
            value = component.add(-5.1)
            if sess:
                value = sess.run(value)
            check(value, np.array([-6.6]))  # prop_b == -1.5

    def test_unregistered_envs(self):
        """Tests, whether an Env can be specified simply by its absolute class."""
        env_cls = "ray.rllib.examples.env.stateless_cartpole.StatelessCartPole"
        env = from_config(env_cls, {"config": 42.0})
        state = env.reset()
        self.assertTrue(state.shape == (2,))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
