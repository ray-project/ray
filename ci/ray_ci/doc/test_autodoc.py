import os
import sys
import tempfile

import pytest

from ci.ray_ci.doc.api import API, AnnotationType, CodeType
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.mock.mock_module import MockClass, mock_function, mock_w00t


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


def test_get_autodoc_rsts_in_myst_file():
    """A MyST landing page's toctree fence yields the same children as the RST form.

    Entries sit at column 0 inside the fence rather than indented, so the RST
    block parser stops on the first one. Guards the conversion of an API landing
    page from .rst to .md against silently walking no children at all.
    """
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "head.md"), "w") as f:
            f.write("# Head\n\n")
            f.write("```{toctree}\n")
            f.write(":maxdepth: 2\n\n")
            f.write("api_01.rst\n")
            f.write("api_02.rst\n")
            f.write("```\n\n")
            f.write("```{eval-rst}\n")
            f.write(".. currentmodule:: ci.ray_ci.doc.mock\n")
            f.write(".. autoclass:: MockClass\n")
            f.write("```\n")

        autodoc = Autodoc(os.path.join(tmp, "head.md"))
        assert autodoc._get_autodoc_rsts_in_file(os.path.join(tmp, "head.md")) == {
            os.path.join(tmp, "api_01.rst"),
            os.path.join(tmp, "api_02.rst"),
        }
        assert [api.name for api in autodoc.get_apis()] == [
            "ci.ray_ci.doc.mock.MockClass"
        ]


def test_toctree_entry_forms():
    """Every toctree entry form resolves, in both the RST and the MyST toctree.

    A bare docname and a "Title <docname>" pair are the forms MyST pages use, and
    a bare docname must resolve to whichever source extension exists. Without
    this, converting a child page from .rst to .md drops it out of the walk and
    its autosummary entries read as undocumented public APIs.
    """
    with tempfile.TemporaryDirectory() as tmp:
        for name in ("plain.rst", "converted.md", "titled.md"):
            with open(os.path.join(tmp, name), "w") as f:
                f.write("\n")

        with open(os.path.join(tmp, "head.md"), "w") as f:
            f.write("```{toctree}\n")
            f.write(":maxdepth: 2\n\n")
            f.write("explicit.rst\n")  # explicit suffix, file need not exist
            f.write("plain\n")  # bare docname resolving to .rst
            f.write("converted\n")  # bare docname resolving to .md
            f.write("Some Title <titled>\n")  # titled entry
            f.write("/root/relative\n")  # unresolvable, must not be added
            f.write("```\n")

        with open(os.path.join(tmp, "head.rst"), "w") as f:
            f.write(".. toctree::\n\n")
            f.write("\tplain\n")
            f.write("\tconverted\n")

        autodoc = Autodoc(os.path.join(tmp, "head.md"))
        assert autodoc._get_autodoc_rsts_in_file(os.path.join(tmp, "head.md")) == {
            os.path.join(tmp, "explicit.rst"),
            os.path.join(tmp, "plain.rst"),
            os.path.join(tmp, "converted.md"),
            os.path.join(tmp, "titled.md"),
        }

        # The RST toctree resolves a bare docname to a converted .md child too:
        # a landing page still in RST can toctree into an already-converted page.
        assert autodoc._get_autodoc_rsts_in_file(os.path.join(tmp, "head.rst")) == {
            os.path.join(tmp, "plain.rst"),
            os.path.join(tmp, "converted.md"),
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
