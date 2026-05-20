import unittest
from unittest.mock import patch

from ray.rllib.models.utils import get_activation_fn


class _DummyTorchNN:
    class GELU:
        pass

    class LeakyReLU:
        pass


class TestModelUtils(unittest.TestCase):
    def test_get_torch_activation_fn_supports_lowercase_aliases(self):
        with patch(
            "ray.rllib.models.utils.try_import_torch",
            return_value=(None, _DummyTorchNN),
        ):
            self.assertIs(
                get_activation_fn("gelu", framework="torch"), _DummyTorchNN.GELU
            )
            self.assertIs(
                get_activation_fn("leaky_relu", framework="torch"),
                _DummyTorchNN.LeakyReLU,
            )
            self.assertIs(
                get_activation_fn("leakyrelu", framework="torch"),
                _DummyTorchNN.LeakyReLU,
            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
