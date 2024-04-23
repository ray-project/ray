import pytest

from ray.data.preprocessors import BatchMapper
from ray.tests.conftest import *  # noqa


def test_constructor_raises_deprecation_error():
    with pytest.raises(DeprecationWarning):
        BatchMapper(lambda x: x, batch_format=None)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
