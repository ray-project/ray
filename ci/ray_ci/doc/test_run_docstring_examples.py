import sys

import pytest

from ci.ray_ci.doc.run_docstring_examples import (
    _EXCLUDED_DIRS,
    extract,
    is_skipped,
    module_name,
    run_docstring,
)

_SOURCE = '''"""Module docstring.

Examples:
    >>> VALUE + 1
    2
"""

VALUE = 1


def helper():
    """No examples here."""


class Widget:
    """A widget.

    Examples:
        >>> w = Widget()
        >>> w.size()
        3
    """

    def size(self):
        """Return the size.

        Examples:
            >>> Widget().size() == 3
            True
        """
        return 3

    def broken(self):
        """Raises.

        Examples:
            >>> x = 1
            >>> raise ValueError("boom")
        """


def skipped_statement():
    """Skips one statement.

    Examples:
        >>> raise RuntimeError("not run")  # doctest: +SKIP
        >>> ok = True
    """


def render_only():
    """Render only.

    Examples:
        >>> connect_to_tpu()  # doctest: +SKIP_EXAMPLE
    """


def wrong_output():
    """Prints something other than what it shows.

    Examples:
        >>> print("actual")
        expected
    """
'''


@pytest.fixture
def docstrings():
    return {d.qualname: d for d in extract(_SOURCE, "m.py")}


@pytest.fixture
def module_globals():
    namespace = {}
    exec(compile(_SOURCE, "m.py", "exec"), namespace)
    return namespace


def test_extract_finds_only_docstrings_with_examples(docstrings):
    assert sorted(docstrings) == [
        "<module>",
        "Widget",
        "Widget.broken",
        "Widget.size",
        "render_only",
        "skipped_statement",
        "wrong_output",
    ]
    assert len(docstrings["Widget"].examples) == 2


def test_examples_run_against_module_globals(docstrings, module_globals):
    for name in ("<module>", "Widget", "Widget.size"):
        assert run_docstring(docstrings[name], module_globals, "m.py") is None


def test_raising_example_fails_with_location(docstrings, module_globals):
    failure = run_docstring(docstrings["Widget.broken"], module_globals, "m.py")
    assert "m.py:" in failure and "(Widget.broken)" in failure
    assert 'raise ValueError("boom")' in failure
    assert "ValueError: boom" in failure


def test_skip_directive_skips_one_statement(docstrings, module_globals):
    assert (
        run_docstring(docstrings["skipped_statement"], module_globals, "m.py") is None
    )


def test_skip_example_skips_the_docstring(docstrings):
    assert is_skipped(docstrings["render_only"])
    assert not is_skipped(docstrings["Widget"])


def test_expected_output_is_not_checked(docstrings, module_globals, capsys):
    # The runner checks that example code runs, not what it prints.
    assert run_docstring(docstrings["wrong_output"], module_globals, "m.py") is None


def test_runs_do_not_leak_names_between_docstrings(docstrings, module_globals):
    run_docstring(docstrings["Widget"], module_globals, "m.py")
    assert "w" not in module_globals


@pytest.mark.parametrize(
    "path,name",
    [
        ("python/ray/data/aggregate.py", "ray.data.aggregate"),
        ("python/ray/data/__init__.py", "ray.data"),
        (
            "python/ray/data/datasource/file_datasink.py",
            "ray.data.datasource.file_datasink",
        ),
    ],
)
def test_module_name(path, name):
    assert module_name(path) == name


def test_tests_and_examples_are_excluded():
    assert _EXCLUDED_DIRS.search("python/ray/data/tests/test_x.py")
    assert _EXCLUDED_DIRS.search("python/ray/data/examples/demo.py")
    assert not _EXCLUDED_DIRS.search("python/ray/data/aggregate.py")


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
