"""Hermetic test for the in-repo ``myst_eval_rst_doctree`` Sphinx extension.

Builds a tiny Sphinx project in a temp dir (in-process, no network, no Ray
import) with a Markdown page whose ``{eval-rst}`` block holds a bullet list and
a Python domain signature, then loads the pickled doctree and checks the
following:

* Without the extension, nested nodes still reference MyST's throwaway
  document, and the pickle carries a ``BuildEnvironment``. This guards the test
  itself: if a MyST release fixes the leak, this half fails, and the extension
  can be deleted.
* With the extension, every node references the page's own doctree, and the
  pickle carries no ``BuildEnvironment``.

Run directly (``python doc/source/_ext/test_myst_eval_rst_doctree.py``) or under
pytest. It needs ``sphinx`` + ``myst-parser`` but not the full Ray docs
toolchain.
"""

import io
import pickle
import tempfile
from pathlib import Path

_EXT_DIR = str(Path(__file__).resolve().parent)

# Both constructs build nodes that docutils gives an explicit `document`
# reference while MyST parses them into its throwaway document: a bullet list's
# items and a Python domain signature. (Nodes that only inherit `document` from
# their parent, such as a list nested in a note, don't leak.)
PAGE = """\
# Page

```{eval-rst}
* ``first`` item
* second item with :ref:`a reference <page-target>`

.. py:function:: join(a, b)

   Join *a* and ``b``.
```

(page-target)=

## Target
"""


def _build(tmp: Path, with_extension: bool):
    from sphinx.application import Sphinx

    extensions = ["myst_parser"] + (["myst_eval_rst_doctree"] if with_extension else [])
    src = tmp / "src"
    src.mkdir()
    (src / "conf.py").write_text(
        f"import sys\nsys.path.insert(0, {_EXT_DIR!r})\n"
        f"extensions = {extensions!r}\nproject = 'TestProj'\nhtml_theme = 'basic'\n",
        encoding="utf-8",
    )
    (src / "index.md").write_text(PAGE, encoding="utf-8")
    doctrees = tmp / "doctrees"
    app = Sphinx(
        str(src),
        str(src),
        str(tmp / "out"),
        str(doctrees),
        "html",
        status=io.StringIO(),
        warning=io.StringIO(),
        freshenv=True,
    )
    app.build()
    raw = (doctrees / "index.doctree").read_bytes()
    return raw, pickle.loads(raw)


def _foreign_nodes(doctree):
    return [
        node
        for node in doctree.findall()
        if getattr(node, "document", None) is not None and node.document is not doctree
    ]


def _check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def test_leak_reproduces_without_extension():
    with tempfile.TemporaryDirectory() as tmp:
        raw, doctree = _build(Path(tmp), with_extension=False)
    _check(
        _foreign_nodes(doctree),
        "no node references a nested document without the extension; MyST may "
        "have fixed the eval-rst leak, so the extension may be removable",
    )
    _check(
        b"BuildEnvironment" in raw,
        "pickled doctree carries no BuildEnvironment without the extension",
    )


def test_extension_removes_leak():
    with tempfile.TemporaryDirectory() as tmp:
        raw, doctree = _build(Path(tmp), with_extension=True)
    foreign = _foreign_nodes(doctree)
    _check(
        not foreign,
        f"{len(foreign)} nodes still reference a nested document: "
        f"{sorted({n.tagname for n in foreign})}",
    )
    _check(
        b"BuildEnvironment" not in raw,
        "pickled doctree still carries a BuildEnvironment",
    )


if __name__ == "__main__":
    test_leak_reproduces_without_extension()
    test_extension_removes_leak()
    print(
        "PASS: myst_eval_rst_doctree re-points nested eval-rst nodes, and the "
        "pickled doctree no longer carries the build environment."
    )
