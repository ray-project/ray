import sys

import pytest

from ci.ray_ci.doc.api import AnnotationType, CodeType
from ci.ray_ci.doc.module import Module

_MOCK = "ci.ray_ci.doc.mock.mock_module"


def test_walk():
    module = Module(_MOCK)
    # Keyed by name rather than indexed: the walk's order follows dir(), so any
    # annotated symbol added to the fixture would renumber positional asserts.
    apis = {api.name: api for api in module.get_apis()}

    mock_class = apis[f"{_MOCK}.MockClass"]
    assert mock_class.annotation_type.value == AnnotationType.PUBLIC_API.value
    assert mock_class.code_type.value == CodeType.CLASS.value

    mock_func = apis[f"{_MOCK}.mock_function"]
    assert mock_func.annotation_type.value == AnnotationType.DEPRECATED.value
    assert mock_func.code_type.value == CodeType.FUNCTION.value

    assert module._module.__hash__ in module._visited
    assert module._module not in module._visited


def test_walk_ignores_inherited_api_annotations():
    module = Module(_MOCK)
    names = {api.name for api in module.get_apis()}

    # Undecorated subclasses inherit `_annotated` as a plain attribute; neither
    # the public nor the deprecated one is an API the walk owns.
    assert f"{_MOCK}.InheritedAnnotation" not in names
    assert f"{_MOCK}.MockDeprecatedSubclass" not in names
    # Positive control: their directly-annotated bases are still found, so the
    # assertions above cannot pass by the walk finding nothing at all.
    assert f"{_MOCK}.MockClass" in names
    assert f"{_MOCK}.MockDeprecatedClass" in names


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
