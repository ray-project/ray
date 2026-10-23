import sys

import pytest

import ci.ray_ci.doc.run_docstring_examples as runner
from ci.ray_ci.doc.run_docstring_examples import (
    _EXCLUDED_DIRS,
    MissingLibrary,
    covered_files,
    extract,
    is_skipped,
    label_path,
    main,
    missing_library,
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


@pytest.mark.skipif(sys.version_info < (3, 11), reason="add_note is 3.11+")
def test_expected_exception_with_note_passes():
    source = '''def noted():
    """Raises with a note.

    Examples:
        >>> e = ValueError("boom")
        >>> e.add_note("a note")
        >>> raise e
        Traceback (most recent call last):
            ...
        ValueError: boom
        a note
    """
'''
    (docstring,) = extract(source, "n.py")
    assert run_docstring(docstring, {}, "n.py") is None


def test_expected_syntax_error_passes():
    source = '''def bad_syntax():
    """Shows a SyntaxError.

    Examples:
        >>> 1 +
        Traceback (most recent call last):
            ...
        SyntaxError: invalid syntax
    """
'''
    (docstring,) = extract(source, "s.py")
    assert run_docstring(docstring, {}, "s.py") is None


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


@pytest.mark.parametrize(
    "label,path",
    [
        ("//python/ray/data:aggregate.py", "python/ray/data/aggregate.py"),
        (
            "//python/ray/_private:accelerators/amd_gpu.py",
            "python/ray/_private/accelerators/amd_gpu.py",
        ),
        ("//:setup.py", "setup.py"),
    ],
)
def test_label_path(label, path):
    assert label_path(label) == path


def test_covered_files_keeps_only_files_a_label_lists():
    labels = [
        "//bazel:default_doctest_pytest_plugin.py\n",
        "//python/ray/data:dataset.py\n",
        "//python/ray/util:actor_pool.py\n",
        "\n",
    ]
    kept, dropped = covered_files(
        [
            "python/ray/data/dataset.py",
            "python/ray/util/iter.py",
            "python/ray/util/actor_pool.py",
        ],
        labels,
    )
    assert kept == ["python/ray/data/dataset.py", "python/ray/util/actor_pool.py"]
    assert dropped == ["python/ray/util/iter.py"]


def test_list_changed_with_doctest_labels(tmp_path, monkeypatch, capsys):
    labels = tmp_path / "labels.txt"
    labels.write_text("//python/ray/data:dataset.py\n")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_docstring_examples.py",
            "--doctest-labels",
            str(labels),
            "--list-changed",
            "python/ray/data/dataset.py",
            "python/ray/util/iter.py",
        ],
    )
    assert main() == 0
    captured = capsys.readouterr()
    # stdout stays a plain file list for run_docstring_examples.sh.
    assert captured.out == "python/ray/data/dataset.py\n"
    assert "python/ray/util/iter.py: no CPU doctest() target covers it" in (
        captured.err
    )


def test_tests_and_examples_are_excluded():
    assert _EXCLUDED_DIRS.search("python/ray/data/tests/test_x.py")
    assert _EXCLUDED_DIRS.search("python/ray/data/examples/demo.py")
    assert not _EXCLUDED_DIRS.search("python/ray/data/aggregate.py")


@pytest.fixture
def nothing_installed(monkeypatch):
    """Report every library as uninstalled, whatever the test environment has."""
    monkeypatch.setattr(runner, "_installed", lambda module: False)


def _chained(outer: ImportError, inner: BaseException) -> ImportError:
    """Raise outer while handling inner, as Ray Data's import checks do."""
    try:
        try:
            raise inner
        except BaseException:
            raise outer
    except ImportError as e:
        return e


def test_missing_library_from_module_not_found(nothing_installed):
    assert missing_library(ModuleNotFoundError("x", name="torch")) == "torch"
    assert missing_library(ModuleNotFoundError("x", name="torch.nn")) == "torch"


def test_missing_library_follows_the_exception_chain(nothing_installed):
    error = _chained(
        ImportError("`TFRecordDatasink` depends on a module Ray Data can't import"),
        ModuleNotFoundError("No module named 'tensorflow'", name="tensorflow"),
    )
    assert missing_library(error) == "tensorflow"


def test_missing_library_stops_at_raise_from_none(nothing_installed):
    try:
        try:
            raise ModuleNotFoundError("No module named 'torch'", name="torch")
        except ImportError:
            raise ImportError("an unrelated import failure") from None
    except ImportError as e:
        error = e
    assert missing_library(error) is None


def test_missing_library_from_a_bare_import_error_message(nothing_installed):
    error = ImportError(
        "PyTorch was specified as the framework to use (via "
        "`config.framework('torch')`)! However, no installation was found."
    )
    assert missing_library(error) == "torch"
    assert (
        missing_library(ModuleNotFoundError("No module named 'tensorflow_datasets'"))
        == "tensorflow_datasets"
    )


def test_missing_library_ignores_libraries_the_image_keeps(nothing_installed):
    # A typo or a dependency the image is meant to have still fails the run.
    assert missing_library(ModuleNotFoundError("x", name="pandsa")) is None
    error = _chained(
        ImportError("`ImageDatasource` depends on 'PIL'"),
        ModuleNotFoundError("No module named 'PIL'", name="PIL"),
    )
    assert missing_library(error) is None
    assert missing_library(ValueError("torch")) is None


def test_missing_library_never_matches_an_installed_library(monkeypatch):
    monkeypatch.setattr(runner, "_installed", lambda module: True)
    assert missing_library(ModuleNotFoundError("x", name="torch")) is None
    assert missing_library(ImportError("PyTorch was specified")) is None


_NEEDS_TORCH = '''def uses_torch():
    """Needs torch.

    Examples:
        >>> import torch
        >>> torch.ones(1)
        tensor([1.])
    """


def expects_import_error():
    """Shows the error.

    Examples:
        >>> import torch
        Traceback (most recent call last):
            ...
        ModuleNotFoundError: ...
    """
'''


def test_example_missing_an_omitted_library_raises_missing_library(
    monkeypatch, nothing_installed
):
    # None in sys.modules makes `import torch` raise ModuleNotFoundError.
    monkeypatch.setitem(sys.modules, "torch", None)
    docstrings = {d.qualname: d for d in extract(_NEEDS_TORCH, "t.py")}
    with pytest.raises(MissingLibrary) as info:
        run_docstring(docstrings["uses_torch"], {}, "t.py")
    assert info.value.library == "torch"
    # An example that shows the error as its expected output still passes.
    assert run_docstring(docstrings["expects_import_error"], {}, "t.py") is None


def _write_package(root, files):
    """Write python/<pkg>/<name>.py files and put python/ on sys.path."""
    paths = []
    for name, source in files.items():
        path = root / "python" / "docpkg" / f"{name}.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
        paths.append(f"python/docpkg/{name}.py")
    (root / "python" / "docpkg" / "__init__.py").write_text("")
    return paths


@pytest.fixture
def package(tmp_path, monkeypatch, nothing_installed):
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path / "python"))
    monkeypatch.setitem(sys.modules, "torch", None)
    monkeypatch.setitem(sys.modules, "pandsa", None)
    for name in [m for m in sys.modules if m.startswith("docpkg")]:
        monkeypatch.delitem(sys.modules, name)
    return tmp_path


def _run_main(monkeypatch, paths):
    monkeypatch.setattr(sys, "argv", ["run_docstring_examples.py", *paths])
    return main()


_PASSES = '''def ok():
    """Runs.

    Examples:
        >>> 1 + 1
        2
    """
'''

_MODULE_NEEDS_TORCH = '''import torch


def gone():
    """Never runs here.

    Examples:
        >>> torch.ones(1)
        tensor([1.])
    """


def render_only():
    """Render only.

    Examples:
        >>> torch.cuda.device_count()  # doctest: +SKIP_EXAMPLE
    """
'''

_MODULE_TYPO = '''import pandsa


def broken():
    """Imports a typo.

    Examples:
        >>> 1
        1
    """
'''


def test_main_skips_docstrings_and_modules_that_need_omitted_libraries(
    package, monkeypatch, capsys
):
    paths = _write_package(
        package,
        {"ok": _PASSES, "examples": _NEEDS_TORCH, "module": _MODULE_NEEDS_TORCH},
    )
    assert _run_main(monkeypatch, paths) == 0
    out = capsys.readouterr().out
    assert "PASS python/docpkg/ok.py:ok" in out
    assert "SKIP python/docpkg/examples.py:uses_torch (needs torch)" in out
    assert "PASS python/docpkg/examples.py:expects_import_error" in out
    assert "SKIP python/docpkg/module.py (module needs torch)" in out
    assert (
        "2 docstring(s) run, 0 failed, 1 skipped, "
        "2 skipped for a library this image omits (2 need torch), in 3 file(s)."
    ) in out


def test_main_fails_on_any_other_import_error(package, monkeypatch, capsys):
    paths = _write_package(package, {"ok": _PASSES, "typo": _MODULE_TYPO})
    assert _run_main(monkeypatch, paths) == 1
    out = capsys.readouterr().out
    # The failing module doesn't stop the run.
    assert "PASS python/docpkg/ok.py:ok" in out
    assert "FAIL python/docpkg/typo.py (import)" in out
    assert "importing docpkg.typo failed" in out
    assert "1 docstring(s) run, 1 failed, 0 skipped, in 2 file(s)." in out


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
