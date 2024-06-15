import os
import tempfile
import sys
import pytest

from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.mock.mock_module import MockClass, mock_function, mock_w00t
from ci.ray_ci.doc.api import API, AnnotationType, CodeType


def test_walk():
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "head.rst"), "w") as f:
            f.write(".. toctree::\n\n")
            f.write("\tapi_01.rst\n")
            f.write("\tapi_02.rst\n")
        with open(os.path.join(tmp, "api_01.rst"), "w") as f:
            f.write(".. currentmodule:: ci.ray_ci.doc\n")
            f.write(".. autosummary::\n\n")
            f.write("\t~mock.mock_function\n")
            f.write("\tmock.mock_module.mock_w00t\n")
        with open(os.path.join(tmp, "api_02.rst"), "w") as f:
            f.write(".. currentmodule:: ci.ray_ci.doc.mock\n")
            f.write(".. autoclass:: MockClass\n")

        autodoc = Autodoc(os.path.join(tmp, "head.rst"))
        apis = autodoc.get_apis()
        assert str(apis) == str(
            [
                API(
                    name="ci.ray_ci.doc.mock.mock_function",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
                API(
                    name="ci.ray_ci.doc.mock.mock_module.mock_w00t",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
                API(
                    name="ci.ray_ci.doc.mock.MockClass",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.CLASS,
                ),
            ]
        )
        assert (
            apis[0].get_canonical_name()
            == f"{mock_function.__module__}.{mock_function.__qualname__}"
        )
        assert (
            apis[1].get_canonical_name()
            == f"{mock_w00t.__module__}.{mock_w00t.__qualname__}"
        )
        assert (
            apis[2].get_canonical_name()
            == f"{MockClass.__module__}.{MockClass.__qualname__}"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
