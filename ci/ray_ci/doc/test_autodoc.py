import os
import tempfile
import sys
import pytest

from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.api import API, AnnotationType, CodeType


def test_walk():
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "head.rst"), "w") as f:
            f.write(".. toctree::\n\n")
            f.write("\tapi_01.rst\n")
            f.write("\tapi_02.rst\n")
        with open(os.path.join(tmp, "api_01.rst"), "w") as f:
            f.write(".. autosummary::\n\n")
            f.write("\tfunc_01\n")
            f.write("\tfunc_02\n")
        with open(os.path.join(tmp, "api_02.rst"), "w") as f:
            f.write(".. currentmodule:: mymodule\n")
            f.write(".. autoclass:: class_01\n")

        autodoc = Autodoc(os.path.join(tmp, "head.rst"))
        apis = autodoc.get_apis()
        assert str(apis) == str(
            [
                API(
                    name="func_01",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
                API(
                    name="func_02",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                ),
                API(
                    name="mymodule.class_01",
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.CLASS,
                ),
            ]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
