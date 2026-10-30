import subprocess
import sys

import pytest

from ci.ray_ci.doc.check_docstring_only import (
    _parse_name_status,
    check_changes,
    check_file,
    main,
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


def _git_repo(path):
    def git(*args):
        subprocess.check_call(
            ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=path
        )

    git("init", "-q", "-b", "master")
    return git


def test_base_diffs_from_the_merge_base(tmp_path, monkeypatch, capsys):
    git = _git_repo(tmp_path)
    (tmp_path / "lib.py").write_text(_BEFORE)
    (tmp_path / "other.py").write_text("X = 1\n")
    git("add", ".")
    git("commit", "-q", "-m", "base")
    git("checkout", "-q", "-b", "feature")
    (tmp_path / "lib.py").write_text(_BEFORE.replace("Run it.", "Run it again."))
    git("commit", "-q", "-am", "docstrings only")
    # A code change lands on master after the branch point. Diffing against
    # master literally would count it as this branch's change.
    git("checkout", "-q", "master")
    (tmp_path / "other.py").write_text("X = 2\n")
    git("commit", "-q", "-am", "code on master")
    git("checkout", "-q", "feature")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_docstring_only.py", "--base", "master"])
    assert main() == 0
    assert "1 file(s), docstrings only." in capsys.readouterr().out


def test_parse_name_status():
    output = "M\tpython/ray/data/a.py\nR100\told.py\tnew.py\nA\tb.py\n"
    assert _parse_name_status(output) == [
        ("M", "python/ray/data/a.py"),
        ("R", "new.py"),
        ("A", "b.py"),
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
