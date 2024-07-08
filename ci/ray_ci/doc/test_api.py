import sys
import pytest

from ci.ray_ci.doc.api import (
    API,
    AnnotationType,
    CodeType,
    _SPHINX_AUTOCLASS_HEADER,
    _SPHINX_AUTOSUMMARY_HEADER,
)
from ci.ray_ci.doc.mock.mock_module import mock_function


def test_from_autosummary():
    test_data = [
        {
            "input": {
                "doc": (
                    f"{_SPHINX_AUTOSUMMARY_HEADER}\n"
                    "\t:toc\n"
                    "\n"
                    "\tfun_01\n"
                    "\t.. this is a comment\n"
                    "\tfun_02\n"
                    "something else"
                ),
                "module": "mymodule",
            },
            "output": [
                API(
                    name="mymodule.fun_01",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
                API(
                    name="mymodule.fun_02",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
            ],
        },
        {
            "input": {
                "doc": "invalid string",
                "module": "mymodule",
            },
            "output": [],
        },
    ]

    for test in test_data:
        assert str(
            API.from_autosummary(
                test["input"]["doc"],
                test["input"]["module"],
            )
        ) == str(test["output"])


def test_from_autoclasss():
    test_data = [
        # valid input, no module
        {
            "input": {
                "doc": f"{_SPHINX_AUTOCLASS_HEADER} myclass",
                "module": None,
            },
            "output": API(
                name="myclass",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.CLASS,
            ),
        },
        # valid input, with module
        {
            "input": {
                "doc": f"{_SPHINX_AUTOCLASS_HEADER} myclass",
                "module": "mymodule",
            },
            "output": API(
                name="mymodule.myclass",
                annotation_type=AnnotationType.PUBLIC_API,
                code_type=CodeType.CLASS,
            ),
        },
        # invalid input
        {
            "input": {
                "doc": "invalid",
                "module": None,
            },
            "output": None,
        },
    ]

    for test in test_data:
        assert str(
            API.from_autoclass(
                test["input"]["doc"],
                test["input"]["module"],
            )
        ) == str(test["output"])


def test_get_canonical_name():
    api = API(
        name="ci.ray_ci.doc.mock.mock_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    )
    assert (
        api.get_canonical_name()
        == f"{mock_function.__module__}.{mock_function.__qualname__}"
    )


def test_is_private_name():
    test_data = [
        {
            "input": "a.b._private_function",
            "output": True,
        },
        {
            "input": "a.b._internal.public_function",
            "output": True,
        },
        {
            "input": "b.c.public_class",
            "output": False,
        },
    ]
    for test in test_data:
        assert (
            API(
                name=test["input"],
                annotation_type=AnnotationType.UNKNOWN,
                code_type=CodeType.FUNCTION,
            )._is_private_name()
            == test["output"]
        )


def test_is_public():
    assert not API(
        name="a.b._private_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_public()
    assert not API(
        name="a.b._internal.public_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_public()
    assert not API(
        name="a.b.public_function",
        annotation_type=AnnotationType.DEPRECATED,
        code_type=CodeType.FUNCTION,
    ).is_public()
    assert API(
        name="a.b.public_function",
        annotation_type=AnnotationType.PUBLIC_API,
        code_type=CodeType.FUNCTION,
    ).is_public()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
