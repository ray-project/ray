import sys

import pytest

from ci.ray_ci.doc.check_docstring_only import (
    _parse_name_status,
    check_changes,
    check_file,
    cython_fingerprint,
)

_BEFORE = '''"""Module docstring."""

import os


class Thing:
    """A thing."""

    def run(self, x):
        """Run it.

        Examples:
            >>> Thing().run(1)
            1
        """
        return x  # identity


async def fetch():
    """Fetch."""
    return os.getcwd()


def only_docstring():
    """Nothing else here."""
'''


def test_docstring_edits_pass():
    after = (
        _BEFORE.replace('"""Module docstring."""', '"""A new module docstring."""')
        .replace("Run it.", "Run it, now with more words.")
        .replace(">>> Thing().run(1)\n            1", ">>> _ = Thing().run(1)")
        .replace(
            '"""Fetch."""', '"""Fetch the working directory.\n\n    More.\n    """'
        )
    )
    assert check_file("m.py", _BEFORE, after) is None


def test_comment_edits_pass():
    after = _BEFORE.replace("# identity", "# returns its input unchanged")
    assert check_file("m.py", _BEFORE, after) is None


def test_adding_and_removing_docstrings_pass():
    after = _BEFORE.replace('    """A thing."""\n\n', "")
    after = after.replace(
        'def only_docstring():\n    """Nothing else here."""\n',
        "def only_docstring():\n    pass\n",
    )
    assert check_file("m.py", _BEFORE, after) is None


@pytest.mark.parametrize(
    "old,new",
    [
        ("", '"""Package."""\n'),
        ('"""Package."""\n', ""),
        ("# License header.\n", '# License header.\n"""Package."""\n'),
    ],
)
def test_empty_module_docstring_edits_pass(old, new):
    assert check_file("pkg/__init__.py", old, new) is None


def test_empty_module_to_pass_is_code():
    assert check_file("pkg/__init__.py", "", "pass\n") is not None


@pytest.mark.parametrize(
    "old,new",
    [
        ("return x  # identity", "return x + 1"),
        ("import os", "import sys"),
        ("def run(self, x):", "def run(self, x, y=None):"),
        ("class Thing:", "class Thing(object):"),
        ('    """Fetch."""\n', '    """Fetch."""\n    print("side effect")\n'),
    ],
)
def test_code_edits_fail(old, new):
    assert old in _BEFORE
    reason = check_file("m.py", _BEFORE, _BEFORE.replace(old, new))
    assert reason == "changes code, not only docstrings or comments"


def test_non_leading_string_is_code():
    # Only the first statement of a body is a docstring. A string expression
    # later in the body is code, even though it does nothing.
    after = _BEFORE.replace("return x  # identity", '"note"\n        return x')
    assert check_file("m.py", _BEFORE, after) is not None


def test_dunder_doc_assignment_is_code():
    after = _BEFORE + '\nThing.__doc__ = "Replaced."\n'
    assert check_file("m.py", _BEFORE, after) is not None


def test_parse_error_fails():
    reason = check_file("m.py", _BEFORE, _BEFORE + "\ndef broken(:\n")
    assert reason.startswith("doesn't parse")


def test_change_kinds():
    files = {"a.py": _BEFORE}
    failures = check_changes(
        [
            ("M", "a.py"),
            ("A", "new.py"),
            ("D", "gone.py"),
            ("R", "renamed.py"),
            ("M", "README.md"),
            ("M", "src/ray/core.cc"),
        ],
        read_old=files.get,
        read_new=files.get,
    )
    assert [path for path, _ in failures] == [
        "new.py",
        "gone.py",
        "renamed.py",
        "README.md",
        "src/ray/core.cc",
    ]


def test_stub_docstring_edits_pass():
    old = 'class ObjectRef:\n    def hex(self) -> str:\n        """Hex."""\n'
    new = old.replace('"""Hex."""', '"""Return the ID as a hex string."""')
    assert check_file("python/ray/includes/object_ref.pyi", old, new) is None
    assert (
        check_changes(
            [("M", "python/ray/includes/object_ref.pyi")],
            read_old=lambda _: old,
            read_new=lambda _: new,
        )
        == []
    )


# Cython that the stdlib `ast` can't parse, with the two constructs the token
# comparison must treat as code: build comments and a verbatim C block.
_CYTHON_BEFORE = '''# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

"""Binding of C++ ray::gcs::GcsClient."""

from libc.stdint cimport int64_t

cdef extern from *:
    """
    #define RAY_FLAG 1
    """
    int RAY_FLAG


cdef class InnerGcsClient:
    """Client to the GCS server."""

    cdef:
        int64_t timeout_ms

    def drain_nodes(
        self, node_ids, timeout=None
    ):
        """returns a list of node_ids that are successfully drained."""
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
        return self._drain(node_ids, timeout_ms)  # binary IDs

    def get_node(self, node_id):
        cdef c_string cnode_id = node_id
        return self.inner.get().GetNodeInfo(cnode_id)

    cdef int64_t _drain(self, node_ids, int64_t timeout_ms) except? -1:
        """Send the request."""
        return <int64_t>len(node_ids)
'''


def test_cython_docstring_edits_pass():
    # The shape of the edits in ray-project/ray#67592: a rewritten docstring,
    # a docstring added where there was none, and a comment edit.
    after = (
        _CYTHON_BEFORE.replace(
            '"""returns a list of node_ids that are successfully drained."""',
            '"""Drain the given nodes.\n\n        Args:\n'
            "            node_ids: Node IDs as binary bytes (not hex strings).\n"
            '        """',
        )
        .replace(
            "    def get_node(self, node_id):\n",
            "    def get_node(self, node_id):\n"
            '        """Get info for the node whose ID is a hex string."""\n',
        )
        .replace("# binary IDs", "# node IDs are binary")
        .replace('"""Binding of', '"""Cython binding of')
        .replace('"""Send the request."""', '"""Send the drain request."""')
        .replace('"""Client to', '"""A client to')
    )
    assert (
        check_file("python/ray/includes/gcs_client.pxi", _CYTHON_BEFORE, after) is None
    )


@pytest.mark.parametrize(
    "old,new",
    [
        ("# cython: embedsignature = True", "# cython: embedsignature = False"),
        ("# distutils: language = c++", "# distutils: language = c"),
        ("#define RAY_FLAG 1", "#define RAY_FLAG 2"),
        ("int64_t timeout_ms\n", "int timeout_ms\n"),
        ("except? -1", "except *"),
        ("<int64_t>len", "<int>len"),
        ("cimport int64_t", "cimport int32_t"),
        ("return self._drain(", "return self._drain_all("),
        # A string after the first statement of a body isn't a docstring.
        (
            "        cdef c_string cnode_id = node_id\n",
            '        cdef c_string cnode_id = node_id\n        "note"\n',
        ),
        (
            '        """returns a list of node_ids that are successfully drained."""',
            '        f"""returns {self!r}."""',
        ),
    ],
)
def test_cython_code_edits_fail(old, new):
    assert old in _CYTHON_BEFORE
    reason = check_file("m.pyx", _CYTHON_BEFORE, _CYTHON_BEFORE.replace(old, new))
    assert reason == "changes code, not only docstrings or comments"


def test_cython_indentation_is_code():
    old = "        return <int64_t>len(node_ids)\n"
    assert old in _CYTHON_BEFORE
    after = _CYTHON_BEFORE.replace(old, "    return <int64_t>len(node_ids)\n")
    assert check_file("m.pyx", _CYTHON_BEFORE, after) is not None


def test_cython_line_wrapping_passes():
    old = "        return self._drain(node_ids, timeout_ms)"
    new = "        return self._drain(\n            node_ids, timeout_ms\n        )"
    after = _CYTHON_BEFORE.replace(old, new)
    assert check_file("m.pyx", _CYTHON_BEFORE, after) is None


def test_cython_tokenize_error_fails():
    reason = check_file("m.pxd", _CYTHON_BEFORE, _CYTHON_BEFORE + 'x = """open\n')
    assert reason.startswith("doesn't tokenize")


def test_cython_fingerprint_keeps_build_comments():
    assert "# cython: embedsignature = True" in cython_fingerprint(_CYTHON_BEFORE)


def test_parse_name_status():
    output = "M\tpython/ray/data/a.py\nR100\told.py\tnew.py\nA\tb.py\n"
    assert _parse_name_status(output) == [
        ("M", "python/ray/data/a.py"),
        ("R", "new.py"),
        ("A", "b.py"),
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
