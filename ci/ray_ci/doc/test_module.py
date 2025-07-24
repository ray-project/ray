import sys
import pytest

from ci.ray_ci.doc.module import Module
from ci.ray_ci.doc.api import AnnotationType, CodeType


def test_walk():
    module = Module("ci.ray_ci.doc.mock.mock_module")
    apis = module.get_apis()
    assert apis[0].name == "ci.ray_ci.doc.mock.mock_module.MockClass"
    assert apis[0].annotation_type.value == AnnotationType.PUBLIC_API.value
    assert apis[0].code_type.value == CodeType.CLASS.value
    assert apis[1].name == "ci.ray_ci.doc.mock.mock_module.mock_function"
    assert apis[1].annotation_type.value == AnnotationType.DEPRECATED.value
    assert apis[1].code_type.value == CodeType.FUNCTION.value
    assert module._module.__hash__ in module._visited
    assert module._module not in module._visited


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
