import sys
import textwrap
from typing import List

import pytest

from ci.ray_ci.doc import cmd_check_api_param_coverage as cmd
from ci.ray_ci.doc.api_param_coverage import (
    ClassIndex,
    Violation,
    build_class_index,
    documented_params,
    new_violations_for_file,
    public_callables,
    signature_params,
)


def _src(text: str) -> str:
    return textwrap.dedent(text)


def _check(base: str, head: str, extra_files=()) -> List[Violation]:
    """New violations for one changed file, given before/after source.

    ``extra_files`` supplies additional ``(path, source)`` pairs that populate
    the class index (for docstring-inheritance cases). ``base=None`` models a
    file that did not exist at the base revision.
    """
    files = list(extra_files) + [("python/ray/mod.py", head)]
    index = build_class_index(files)
    return new_violations_for_file("python/ray/mod.py", base, head, index, index)


# --- documented_params / signature_params units -------------------------------


def test_documented_params_reads_args_block():
    doc = _src(
        """
        Summary.

        Args:
            a: the a.
            b (int): the b.  # noqa: this typed arg is intentional test data

        Returns:
            nothing.
        """
    )
    assert documented_params(doc) == {"a", "b"}


def test_documented_params_empty_without_args():
    assert documented_params("Just a summary.") == set()
    assert documented_params(None) == set()


def test_documented_params_stops_on_dedent():
    # A dedented line below the first param's indent ends the Args block, so a
    # "name:"-looking line in trailing prose is not counted as a param.
    doc = _src(
        """
        Summary.

        Args:
            a: the a.

          note: this trailing line is dedented and must not count.
        """
    )
    assert documented_params(doc) == {"a"}


def test_signature_params_excludes_self_and_varargs():
    tree = _src(
        """
        def f(self, a, b=1, *args, c, **kwargs):
            pass
        """
    )
    import ast

    func = ast.parse(tree).body[0]
    assert signature_params(func) == ["a", "b", "c"]


# --- new_violations_for_file: core diff semantics -----------------------------


def test_new_public_function_undocumented_param_fails():
    head = _src(
        '''
        @PublicAPI
        def new_api(alpha, beta):
            """Summary."""
        '''
    )
    violations = _check(base=None, head=head)
    assert len(violations) == 1
    assert violations[0].qualname == "new_api"
    assert violations[0].params == ["alpha", "beta"]


def test_new_public_function_documented_param_passes():
    head = _src(
        '''
        @PublicAPI
        def new_api(alpha, beta):
            """Summary.

            Args:
                alpha: the alpha.
                beta: the beta.
            """
        '''
    )
    assert _check(base=None, head=head) == []


def test_new_param_on_existing_api_fails():
    base = _src(
        '''
        @PublicAPI
        def api(alpha):
            """Summary.

            Args:
                alpha: the alpha.
            """
        '''
    )
    head = _src(
        '''
        @PublicAPI
        def api(alpha, beta):
            """Summary.

            Args:
                alpha: the alpha.
            """
        '''
    )
    violations = _check(base=base, head=head)
    assert len(violations) == 1
    assert violations[0].params == ["beta"]


def test_preexisting_gap_is_grandfathered():
    # alpha was already undocumented at base; it must not fire.
    base = _src(
        '''
        @PublicAPI
        def api(alpha):
            """Summary."""
        '''
    )
    head = _src(
        '''
        @PublicAPI
        def api(alpha):
            """Summary, now with a body change but still no Args."""
        '''
    )
    assert _check(base=base, head=head) == []


def test_removing_doc_entry_for_existing_param_fails():
    base = _src(
        '''
        @PublicAPI
        def api(alpha):
            """Summary.

            Args:
                alpha: the alpha.
            """
        '''
    )
    head = _src(
        '''
        @PublicAPI
        def api(alpha):
            """Summary."""
        '''
    )
    violations = _check(base=base, head=head)
    assert len(violations) == 1
    assert violations[0].params == ["alpha"]


def test_non_public_function_ignored():
    head = _src(
        '''
        def not_public(alpha):
            """Summary."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_private_function_ignored_even_if_public_api():
    head = _src(
        '''
        @PublicAPI
        def _private(alpha):
            """Summary."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_publicapi_call_form_is_detected():
    head = _src(
        '''
        @PublicAPI(stability="beta")
        def api(alpha):
            """Summary."""
        '''
    )
    violations = _check(base=None, head=head)
    assert len(violations) == 1
    assert violations[0].params == ["alpha"]


# --- class / __init__ / inheritance -------------------------------------------


def test_init_documented_on_class_docstring_passes():
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary.

            Args:
                alpha: the alpha.
            """

            def __init__(self, alpha):
                pass
        '''
    )
    assert _check(base=None, head=head) == []


def test_method_of_public_class_undocumented_fails():
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            def method(self, alpha):
                """Does a thing."""
        '''
    )
    violations = _check(base=None, head=head)
    assert len(violations) == 1
    assert violations[0].qualname == "C.method"
    assert violations[0].params == ["alpha"]


def test_inherited_method_docstring_recovers_param():
    # Override has no own docstring; the base class documents `alpha`, so
    # Sphinx re-injects it and the check must not flag it.
    base_class_file = (
        "python/ray/base_mod.py",
        _src(
            '''
            class Base:
                def method(self, alpha):
                    """Base.

                    Args:
                        alpha: the alpha.
                    """
            '''
        ),
    )
    head = _src(
        """
        @PublicAPI
        class Child(Base):
            def method(self, alpha):
                pass
        """
    )
    assert _check(base=None, head=head, extra_files=[base_class_file]) == []


def test_multiple_callables_sorted_by_location():
    head = _src(
        '''
        @PublicAPI
        def a_api(x):
            """S."""

        @PublicAPI
        def b_api(y):
            """S."""
        '''
    )
    violations = _check(base=None, head=head)
    assert [v.qualname for v in violations] == ["a_api", "b_api"]
    assert violations[0].lineno < violations[1].lineno


def test_developer_api_method_of_public_class_ignored():
    # Regression: a method of an @PublicAPI class that carries its own
    # @DeveloperAPI is not part of the rendered public surface. Ray's
    # Dataset.map_batches_internal (#64963) is the real-world case.
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            @DeveloperAPI
            def method_internal(self, alpha, beta):
                """Internal helper."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_deprecated_method_of_public_class_ignored():
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            @Deprecated
            def old_method(self, alpha):
                """Old."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_developer_api_call_form_on_method_ignored():
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            @DeveloperAPI(stability="alpha")
            def method_internal(self, alpha):
                """Internal helper."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_explicit_public_api_on_method_wins_over_developer_api():
    # An explicit @PublicAPI on the method keeps it in scope even alongside
    # @DeveloperAPI, so the more specific public annotation is not lost.
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            @DeveloperAPI
            @PublicAPI
            def method(self, alpha):
                """Does a thing."""
        '''
    )
    violations = _check(base=None, head=head)
    assert len(violations) == 1
    assert violations[0].qualname == "C.method"
    assert violations[0].params == ["alpha"]


def test_developer_api_module_function_still_ignored():
    # Module-level scope is decided by @PublicAPI presence, so a @DeveloperAPI
    # function was already out of scope; guard against the filter regressing it.
    head = _src(
        '''
        @DeveloperAPI
        def helper(alpha):
            """Helper."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_inherited_docstring_through_subscripted_base():
    # A generic base (`class Impl(Base[T])`) must still resolve for docstring
    # inheritance; otherwise the override is flagged for a documented param.
    base_class_file = (
        "python/ray/base_mod.py",
        _src(
            '''
            class Base:
                """Base.

                Args:
                    alpha: The alpha param.
                """

                def method(self, alpha):
                    """Base method.

                    Args:
                        alpha: The alpha param.
                    """
            '''
        ),
    )
    head = _src(
        '''
        @PublicAPI
        class Impl(Base[T]):
            """Impl."""

            def method(self, alpha):
                pass
        '''
    )
    violations = _check(base=None, head=head, extra_files=[base_class_file])
    assert violations == []


# --- changed-file listing (rename detection) ----------------------------------


def _fake_git(monkeypatch, output: str) -> None:
    monkeypatch.setattr(cmd, "_git", lambda *a, **k: output)


def test_changed_files_maps_rename_to_old_path(monkeypatch):
    # A rename must compare against the old path's content. Treating it as a new
    # file would report every pre-existing gap in it as new debt.
    _fake_git(
        monkeypatch,
        "R100\tpython/ray/old_mod.py\tpython/ray/new_mod.py\n"
        "M\tpython/ray/other.py\n",
    )
    assert cmd._changed_python_files("/repo", "base") == [
        ("python/ray/new_mod.py", "python/ray/old_mod.py"),
        ("python/ray/other.py", "python/ray/other.py"),
    ]


def test_changed_files_maps_copy_to_source_path(monkeypatch):
    _fake_git(monkeypatch, "C75\tpython/ray/src.py\tpython/ray/copy.py\n")
    assert cmd._changed_python_files("/repo", "base") == [
        ("python/ray/copy.py", "python/ray/src.py")
    ]


def test_changed_files_added_and_modified_use_same_path(monkeypatch):
    _fake_git(monkeypatch, "A\tpython/ray/added.py\nM\tpython/ray/mod.py\n")
    assert cmd._changed_python_files("/repo", "base") == [
        ("python/ray/added.py", "python/ray/added.py"),
        ("python/ray/mod.py", "python/ray/mod.py"),
    ]


def test_changed_files_filters_out_of_scope_paths(monkeypatch):
    _fake_git(
        monkeypatch,
        "M\tpython/ray/tests/test_thing.py\n"
        "M\tpython/ray/mod.txt\n"
        "M\tdoc/source/index.md\n"
        "M\tpython/ray/keep.py\n",
    )
    assert cmd._changed_python_files("/repo", "base") == [
        ("python/ray/keep.py", "python/ray/keep.py")
    ]


def test_changed_files_rename_out_of_scope_destination_skipped(monkeypatch):
    # Scope is decided by the head path: a file renamed into tests/ drops out.
    _fake_git(
        monkeypatch,
        "R100\tpython/ray/mod.py\tpython/ray/tests/test_mod.py\n",
    )
    assert cmd._changed_python_files("/repo", "base") == []


def test_syntax_error_source_yields_no_callables():
    assert public_callables("def broken(:", ClassIndex()) == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
