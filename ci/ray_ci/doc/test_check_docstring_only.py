import sys

import pytest

from ci.ray_ci.doc.check_docstring_only import (
    _parse_name_status,
    check_changes,
    check_file,
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
        ],
        read_old=files.get,
        read_new=files.get,
    )
    assert [path for path, _ in failures] == [
        "new.py",
        "gone.py",
        "renamed.py",
        "README.md",
    ]


def test_parse_name_status():
    output = "M\tpython/ray/data/a.py\nR100\told.py\tnew.py\nA\tb.py\n"
    assert _parse_name_status(output) == [
        ("M", "python/ray/data/a.py"),
        ("R", "new.py"),
        ("A", "b.py"),
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
