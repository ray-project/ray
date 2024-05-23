import sys
import pytest

from ci.ray_ci.doc.module import Module


def test_walk():
    module = Module("ci.ray_ci.doc.mock_module")
    apis = module.get_apis()
    assert apis[0].name == "ci.ray_ci.doc.mock_module.MockClass"
    assert apis[0].annotation_type.value == "PublicAPI"
    assert apis[0].code_type.value == "Class"
    assert apis[1].name == "ci.ray_ci.doc.mock_module.mock_function"
    assert apis[1].annotation_type.value == "Deprecated"
    assert apis[1].code_type.value == "Function"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
