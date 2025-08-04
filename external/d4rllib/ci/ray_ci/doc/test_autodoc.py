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
            f.write(".. include:: api_03.rst\n")
            f.write(".. currentmodule:: ci.ray_ci.doc\n")
            f.write(".. autosummary::\n")
            f.write("\tmock.mock_module.mock_w00t\n")
        with open(os.path.join(tmp, "api_02.rst"), "w") as f:
            f.write(".. currentmodule:: ci.ray_ci.doc.mock\n")
            f.write(".. autoclass:: MockClass\n")
        with open(os.path.join(tmp, "api_03.rst"), "w") as f:
            f.write(".. currentmodule:: ci.ray_ci.doc\n")
            f.write(".. autosummary::\n")
            f.write("\t~mock.mock_function\n")

        autodoc = Autodoc(os.path.join(tmp, "head.rst"))
        apis = sorted(autodoc.get_apis(), key=lambda x: x.name)
        assert str(apis) == str(
            [
                API(
                    name="ci.ray_ci.doc.mock.MockClass",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.CLASS,
                ),
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
            ]
        )
        assert (
            apis[0].get_canonical_name()
            == f"{MockClass.__module__}.{MockClass.__qualname__}"
        )
        assert (
            apis[1].get_canonical_name()
            == f"{mock_function.__module__}.{mock_function.__qualname__}"
        )
        assert (
            apis[2].get_canonical_name()
            == f"{mock_w00t.__module__}.{mock_w00t.__qualname__}"
        )


def test_get_autodoc_rsts_in_file():
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "head.rst"), "w") as f:
            f.write(".. include:: api_00.rst\n")
            f.write(".. toctree::\n\n")
            f.write("\tapi_01.rst\n")
            f.write("\tapi_02.rst\n")

        autodoc = Autodoc("head.rst")
        sorted(autodoc._get_autodoc_rsts_in_file(os.path.join(tmp, "head.rst"))) == {
            os.path.join(tmp, "api_00.rst"),
            os.path.join(tmp, "api_01.rst"),
            os.path.join(tmp, "api_02.rst"),
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
