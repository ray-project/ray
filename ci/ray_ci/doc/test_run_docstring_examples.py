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


def output_variants():
    """Output that should match.

    Examples:
        >>> [1, 2, 3]
        [1, 2, 3]
        >>> print("a long line with an id 12345")
        a long line ... 12345
        >>> print("a   b")  # doctest: +NORMALIZE_WHITESPACE
        a b
        >>> _ = print("unshown output only has to run")
    """


def expected_exception():
    """Shows a traceback.

    Examples:
        >>> int("x")
        Traceback (most recent call last):
            ...
        ValueError: invalid literal for int() with base 10: 'x'
    """


def wrong_exception():
    """Shows a different exception than it raises.

    Examples:
        >>> int("x")
        Traceback (most recent call last):
            ...
        TypeError: nope
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
        "expected_exception",
        "output_variants",
        "render_only",
        "skipped_statement",
        "wrong_exception",
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


def test_wrong_output_fails_with_a_diff(docstrings, module_globals):
    failure = run_docstring(docstrings["wrong_output"], module_globals, "m.py")
    assert "(wrong_output)" in failure
    assert "Expected:" in failure and "expected" in failure
    assert "Got:" in failure and "actual" in failure


def test_matching_output_passes(docstrings, module_globals):
    # Covers a repr echo, an ELLIPSIS match (the default flag), a per-example
    # directive, and printed output where none is shown.
    assert run_docstring(docstrings["output_variants"], module_globals, "m.py") is None


def test_expected_exception_passes(docstrings, module_globals):
    assert (
        run_docstring(docstrings["expected_exception"], module_globals, "m.py") is None
    )


def test_wrong_exception_fails(docstrings, module_globals):
    failure = run_docstring(docstrings["wrong_exception"], module_globals, "m.py")
    assert "Expected exception" in failure and "ValueError" in failure


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
