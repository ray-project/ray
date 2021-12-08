import sys
import pytest
import ray


def test_object_store_memory_check():
    prefix_msg = "After taking into account object.*"
    with pytest.raises(ValueError, match=prefix_msg):
        ray.init(object_store_memory=int(999 * 1024 * 1024 * 1024))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
