import sys
import pytest

from ci.ray_ci.doc.module import Module


def test_walk():
    module = Module("ci.ray_ci.doc.mock_module")
    assert module.get_class_apis() == {
        "ci.ray_ci.doc.mock_module.MockClass",
    }
    assert module.get_function_apis() == {
        "ci.ray_ci.doc.mock_module.mock_function",
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
