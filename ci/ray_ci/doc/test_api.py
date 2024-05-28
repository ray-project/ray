import sys
import pytest

from ci.ray_ci.doc.api import (
    API,
    AnnotationType,
    CodeType,
    SPHINX_AUTOCLASS_HEADER,
    SPHINX_AUTOSUMMARY_HEADER,
)


def test_from_autosummary():
    test_harness = [
        {
            "input": {
                "doc": (
                    f"{SPHINX_AUTOSUMMARY_HEADER}\n"
                    "\t:toc\n"
                    "\n"
                    "\tfun_01\n"
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

    for test in test_harness:
        assert str(
            API.from_autosummary(
                test["input"]["doc"],
                test["input"]["module"],
            )
        ) == str(test["output"])


def test_from_autoclasss():
    test_harness = [
        # valid input, no module
        {
            "input": {
                "doc": f"{SPHINX_AUTOCLASS_HEADER} myclass",
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
                "doc": f"{SPHINX_AUTOCLASS_HEADER} myclass",
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

    for test in test_harness:
        assert str(
            API.from_autoclass(
                test["input"]["doc"],
                test["input"]["module"],
            )
        ) == str(test["output"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
