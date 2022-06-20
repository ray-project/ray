# coding: utf-8
import logging
import pytest
import sys
import ray.serialization_new as ser_new

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def test_bytes():
    b = b"10086"
    res = ser_new._serialize(b)
    deserialized = ser_new._deserialize(res)
    assert deserialized == b


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-sv", __file__]))
