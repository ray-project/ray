from abc import ABCMeta, abstractmethod
import unittest

from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf = try_import_tf()
tf.enable_eager_execution()

torch, _ = try_import_torch()


class TestFrameWorkAgnosticComponents(unittest.TestCase):
    """
    Tests the Component base class to implement framework-agnostic functional
    units.
    """

    def test_dummy_components(self):
        # Switch on eager for testing purposes.
        tf.enable_eager_execution()

        # Try to create from an abstract class w/o default constructor.
        # Expect None.
        test = from_config({
            "type": AbstractDummyComponent,
            "framework": "torch"
        })
        check(test, None)

        # Create a Component via python API (config dict).
        component = from_config(
            dict(type=DummyComponent, prop_a=1.0, prop_d="non_default"))
        check(component.prop_d, "non_default")

        # Create a tf Component from json file.
        component = from_config("dummy_config.json")
        check(component.prop_c, "default")
        check(component.prop_d, 4)  # default
        check(component.add(3.3).numpy(), 5.3)  # prop_b == 2.0

        # Create a torch Component from yaml file.
        component = from_config("dummy_config.yml")
        check(component.prop_a, "something else")
        check(component.prop_d, 3)
        check(component.add(1.2), torch.Tensor([2.2]))  # prop_b == 1.0

        # Create tf Component from json-string (e.g. on command line).
        component = from_config(
            '{"type": "ray.rllib.utils.tests.'
            'test_framework_agnostic_components.DummyComponent", '
            '"prop_a": "A", "prop_b": -1.0, "prop_c": "non-default"}')
        check(component.prop_a, "A")
        check(component.prop_d, 4)  # default
        check(component.add(-1.1).numpy(), -2.1)  # prop_b == -1.0

        # Create torch Component from yaml-string.
        component = from_config(
            "type: ray.rllib.utils.tests."
            "test_framework_agnostic_components.DummyComponent\n"
            "prop_a: B\nprop_b: -1.5\nprop_c: non-default\nframework: torch")
        check(component.prop_a, "B")
        check(component.prop_d, 4)  # default
        check(component.add(-5.1), torch.Tensor([-6.6]))  # prop_b == -1.5


class DummyComponent:
    """
    A simple DummyComponent that can be used for testing framework-agnostic
    logic. Implements a simple `add()` method for adding a value to
    `self.prop_b`.
    """

    def __init__(self,
                 prop_a,
                 prop_b=0.5,
                 prop_c=None,
                 framework="tf",
                 **kwargs):
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


class AbstractDummyComponent(DummyComponent, metaclass=ABCMeta):
    """
    Used for testing `from_config()`.
    """

    @abstractmethod
    def some_abstract_method(self):
        raise NotImplementedError
