import sys
import textwrap
from typing import List

import pytest

from ci.ray_ci.doc import cmd_check_api_param_coverage as cmd
from ci.ray_ci.doc.api_param_coverage import (
    MISSING_DOCSTRING,
    MISSING_RETURNS,
    ClassIndex,
    Violation,
    build_class_index,
    documented_params,
    has_returns_section,
    new_presence_violations_for_file,
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


def test_private_param_ignored():
    # A leading-underscore kwarg is private by convention. Ray's
    # Deployment.options(_direct_http=...) (#66255) is the real-world case.
    base = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            def options(self, alpha):
                """Options.

                Args:
                    alpha: The alpha.
                """
        '''
    )
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            def options(self, alpha, _private_flag=False):
                """Options.

                Args:
                    alpha: The alpha.
                """
        '''
    )
    assert _check(base=base, head=head) == []


def test_private_param_ignored_but_public_param_still_fails():
    head = _src(
        '''
        @PublicAPI
        def new_api(alpha, _beta=None):
            """Summary."""
        '''
    )
    violations = _check(base=None, head=head)
    assert len(violations) == 1
    assert violations[0].params == ["alpha"]


@pytest.mark.parametrize(
    "decorators",
    [
        ['@field_validator("max_retries")', "@classmethod"],
        ['@model_validator(mode="after")'],
        ['@pydantic.field_validator("max_retries")', "@classmethod"],
        ['@validator("max_retries")'],
        ["@root_validator"],
    ],
)
def test_pydantic_validator_of_public_class_ignored(decorators):
    # The framework passes the field value; it isn't a user-facing parameter.
    # Ray's RequestRouterConfig.validate_max_request_retries(cls, v) (#64399) is
    # the real-world case.
    decos = "\n".join(f"    {d}" for d in decorators)
    head = (
        "@PublicAPI\n"
        "class C(BaseModel):\n"
        '    """Summary."""\n'
        "\n"
        f"{decos}\n"
        "    def validate_max_retries(cls, v):\n"
        "        return v\n"
    )
    assert _check(base=None, head=head) == []


@pytest.mark.parametrize(
    "decorator",
    ["@click.command()", "@cli.command(name='start')", "@click.group()", "@command"],
)
def test_click_command_ignored(decorator):
    # Click options carry their own help= text. Ray's `ray start`
    # (#67258) is the real-world case.
    head = _src(
        f'''
        {decorator}
        @click.option("--alpha", help="The alpha.")
        @PublicAPI
        def start(alpha):
            """Start Ray processes."""
        '''
    )
    assert _check(base=None, head=head) == []


def test_property_setter_ignored():
    # A setter's value is documented on the getter, not in its own Args:.
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            @property
            def x(self):
                """The x property."""
                return 1

            @x.setter
            def x(self, value):
                pass
        '''
    )
    assert _check(base=None, head=head) == []


# --- docstring and Returns: presence ------------------------------------------


def _check_presence(base, head, extra_files=()):
    """New presence violations as ``{qualname: missing}`` for one changed file."""
    files = list(extra_files) + [("python/ray/mod.py", head)]
    index = build_class_index(files)
    return {
        v.qualname: v.missing
        for v in new_presence_violations_for_file(
            "python/ray/mod.py", base, head, index, index
        )
    }


DOC = MISSING_DOCSTRING
RET = MISSING_RETURNS


def test_has_returns_section_forms():
    assert has_returns_section("Summary.\n\nReturns:\n    The value.")
    assert has_returns_section("Summary.\n\nYields:\n    Each value.")
    assert has_returns_section("Summary.\n\nReturns\n-------\nint")
    assert has_returns_section("Summary.\n\n:returns: The value.")
    assert not has_returns_section("Summary that returns the value.")
    assert not has_returns_section(None)


def test_new_zero_arg_method_without_docstring_flagged():
    # The parameter check passes a method with no parameters, whatever its
    # docstring. PopulationBasedTraining.reset_stats is the real-world case.
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            def reset_stats(self):
                pass
        '''
    )
    assert _check_presence(base=None, head=head) == {"C.reset_stats": [DOC]}


def test_new_public_function_and_class_without_docstring_flagged():
    head = _src(
        """
        @PublicAPI
        def f(a) -> int:
            return a

        @PublicAPI
        class C:
            def __init__(self, a):
                pass
        """
    )
    # __init__ is out of scope: the class docstring documents its parameters.
    assert _check_presence(base=None, head=head) == {"f": [DOC, RET], "C": [DOC]}


def test_return_annotation_without_returns_section_flagged():
    head = _src(
        '''
        @PublicAPI
        def f() -> int:
            """Summary."""
            return 1
        '''
    )
    assert _check_presence(base=None, head=head) == {"f": [RET]}


@pytest.mark.parametrize(
    "annotation", ["", " -> None", " -> 'None'", " -> NoReturn", " -> typing.NoReturn"]
)
def test_no_return_value_needs_no_returns_section(annotation):
    head = _src(
        f'''
        @PublicAPI
        def f(){annotation}:
            """Summary."""
        '''
    )
    assert _check_presence(base=None, head=head) == {}


def test_yields_section_satisfies_returns():
    head = _src(
        '''
        @PublicAPI
        def f() -> Iterator[int]:
            """Summary.

            Yields:
                Each value.
            """
        '''
    )
    assert _check_presence(base=None, head=head) == {}


@pytest.mark.parametrize("decorator", ["@property", "@functools.cached_property"])
def test_property_needs_docstring_but_not_returns(decorator):
    # Autodoc renders a property's type from its annotation.
    head = _src(
        f'''
        @PublicAPI
        class C:
            """Summary."""

            {decorator}
            def documented(self) -> int:
                """The value."""

            {decorator}
            def undocumented(self) -> int:
                pass
        '''
    )
    assert _check_presence(base=None, head=head) == {"C.undocumented": [DOC]}


def test_preexisting_missing_docstring_grandfathered():
    src = _src(
        """
        @PublicAPI
        def f() -> int:
            return 1
        """
    )
    assert _check_presence(base=src, head=src) == {}


def test_adding_docstring_without_returns_to_grandfathered_callable_passes():
    # A docstring-only PR that documents an old callable mustn't be blocked for
    # also leaving its pre-existing Returns: gap.
    base = _src(
        """
        @PublicAPI
        def f() -> int:
            return 1
        """
    )
    head = _src(
        '''
        @PublicAPI
        def f() -> int:
            """Summary."""
            return 1
        '''
    )
    assert _check_presence(base=base, head=head) == {}


def test_removed_docstring_flagged():
    base = _src(
        '''
        @PublicAPI
        def f():
            """Summary."""
        '''
    )
    head = _src(
        """
        @PublicAPI
        def f():
            pass
        """
    )
    assert _check_presence(base=base, head=head) == {"f": [DOC]}


def test_added_return_annotation_flagged():
    base = _src(
        '''
        @PublicAPI
        def f():
            """Summary."""
        '''
    )
    head = _src(
        '''
        @PublicAPI
        def f() -> int:
            """Summary."""
        '''
    )
    assert _check_presence(base=base, head=head) == {"f": [RET]}


def test_presence_out_of_scope_callables_ignored():
    head = _src(
        '''
        @PublicAPI
        class C:
            """Summary."""

            def _private(self) -> int:
                pass

            @DeveloperAPI
            def dev(self) -> int:
                pass

            @field_validator("x")
            def check_x(cls, v) -> int:
                return v

            @overload
            def f(self, a: int) -> int: ...

            def f(self, a) -> int:
                """Summary.

                Returns:
                    The value.
                """

            @property
            def x(self) -> int:
                """The x."""

            @x.setter
            def x(self, value) -> None:
                pass

        @DeveloperAPI
        def g() -> int:
            pass
        '''
    )
    assert _check_presence(base=None, head=head) == {}


def test_inherited_docstring_counts_for_presence_and_returns():
    base_file = (
        "python/ray/base.py",
        _src(
            '''
            class Base:
                """Base."""

                def f(self) -> int:
                    """Summary.

                    Returns:
                        The value.
                    """

                def g(self) -> int:
                    """Summary without a Returns section."""
            '''
        ),
    )
    head = _src(
        """
        @PublicAPI
        class C(Base):
            def f(self) -> int:
                return 1

            def g(self) -> int:
                return 1
        """
    )
    # C inherits Base's class docstring, f inherits a Returns: section, and g
    # inherits a docstring without one.
    assert _check_presence(base=None, head=head, extra_files=[base_file]) == {
        "C.g": [RET]
    }


_SESSION_FILE = (
    "python/ray/train/_internal/session.py",
    _src(
        '''
        def get_trial_name() -> str:
            """Trial name for the corresponding trial.

            Returns:
                The trial name.
            """

        def get_world_size() -> int:
            """Get the current world size."""
        '''
    ),
)
_TRAIN_CONTEXT_FILE = (
    "python/ray/train/context.py",
    _src(
        '''
        from ray.train._internal import session
        from ray.train.utils import _copy_doc

        class TrainContext:
            """Context for Ray training executions."""

            @_copy_doc(session.get_world_size)
            def get_world_size(self) -> int:
                return session.get_world_size()
        '''
    ),
)


def test_copy_doc_resolves_module_function():
    # TuneContext.get_trial_name copies its docstring from a function in an
    # imported module at runtime.
    head = _src(
        """
        from ray.train._internal import session
        from ray.train.utils import _copy_doc

        @PublicAPI
        class TuneContext:
            \"\"\"Context.\"\"\"

            @_copy_doc(session.get_trial_name)
            def get_trial_name(self) -> str:
                return session.get_trial_name()
        """
    )
    assert _check_presence(base=None, head=head, extra_files=[_SESSION_FILE]) == {}


def test_copy_doc_resolves_aliased_class_method_through_a_chain():
    # TuneContext.get_world_size copies from TrainV1Context.get_world_size, an
    # import alias of TrainContext, whose method copies from the session module.
    # The final docstring has no Returns: section.
    head = _src(
        """
        from ray.train.context import TrainContext as TrainV1Context
        from ray.train.utils import _copy_doc

        @PublicAPI
        class TuneContext(TrainV1Context):
            \"\"\"Context.\"\"\"

            @_copy_doc(TrainV1Context.get_world_size)
            def get_world_size(self) -> int:
                return 1
        """
    )
    assert _check_presence(
        base=None, head=head, extra_files=[_SESSION_FILE, _TRAIN_CONTEXT_FILE]
    ) == {"TuneContext.get_world_size": [RET]}


def test_copy_doc_unresolved_target_fails_open():
    head = _src(
        """
        @PublicAPI
        class C:
            \"\"\"Summary.\"\"\"

            @_copy_doc(somewhere.unknown)
            def f(self) -> int:
                return 1
        """
    )
    assert _check_presence(base=None, head=head) == {}


def test_copy_doc_on_class():
    head = _src(
        """
        from ray.train import CheckpointConfig as _CheckpointConfig

        @PublicAPI
        @_copy_doc(_CheckpointConfig)
        class CheckpointConfig(_CheckpointConfig):
            pass
        """
    )
    other = ("python/ray/train/config.py", 'class CheckpointConfig:\n    """Doc."""\n')
    assert _check_presence(base=None, head=head, extra_files=[other]) == {}


def test_parse_args_check_option():
    assert cmd._parse_args(["/ray"]).check == cmd.CHECK_PARAMS
    assert cmd._parse_args(["/ray", "--check", "presence"]).check == cmd.CHECK_PRESENCE
    with pytest.raises(SystemExit):
        cmd._parse_args(["/ray", "--check", "bogus"])


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
