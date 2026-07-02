import os
import sys
import tempfile

import pytest

from ci.ray_ci.doc import cmd_check_api_discrepancy as cmd
from ci.ray_ci.doc.mock.mock_module import MockClass, mock_function, mock_w00t

_MOCK = "ci.ray_ci.doc.mock.mock_module"
_CANONICAL_W00T = f"{mock_w00t.__module__}.{mock_w00t.__qualname__}"
_CANONICAL_MOCKCLASS = f"{MockClass.__module__}.{MockClass.__qualname__}"
_CANONICAL_DEPRECATED = f"{mock_function.__module__}.{mock_function.__qualname__}"


def _run_check_team(
    monkeypatch,
    autosummary_entries,
    autoclass_entries=(),
    white_list_apis=frozenset(),
    doc_only_whitelist=frozenset(),
    intentional_duplicate_apis=frozenset(),
):
    """Build a one-off team config over the mock module + a temp head doc.

    Returns the _check_team boolean for the synthesized "mock" team. The mock
    module's public surface is {MockClass, mock_w00t} (mock_function is
    @Deprecated), so Policy 01 passes only when both are documented.
    """
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "head.rst"), "w") as f:
            f.write(f".. currentmodule:: {_MOCK}\n")
            for entry in autoclass_entries:
                f.write(f".. autoclass:: {entry}\n")
            if autosummary_entries:
                f.write(".. autosummary::\n\n")
                for entry in autosummary_entries:
                    f.write(f"\t{entry}\n")

        config = {
            "head_modules": {_MOCK},
            "head_doc_file": "head.rst",
            "white_list_apis": set(white_list_apis),
            "doc_only_whitelist": set(doc_only_whitelist),
            "intentional_duplicate_apis": set(intentional_duplicate_apis),
        }
        monkeypatch.setitem(cmd.TEAM_API_CONFIGS, "mock", config)
        return cmd._check_team(tmp, "mock")


def test_all_policies_pass(monkeypatch):
    # Both public APIs documented exactly once, all resolve, no duplicates.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t"],
        autoclass_entries=["MockClass"],
    )


def test_policy_01_missing_public_api_fails(monkeypatch):
    # mock_w00t (public) is undocumented -> Policy 01 fails.
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=[],
        autoclass_entries=["MockClass"],
    )


def test_policy_02_unresolved_doc_entry_fails(monkeypatch):
    # A documented name that does not resolve (renamed / deleted / typo).
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "renamed_away"],
        autoclass_entries=["MockClass"],
    )


def test_policy_02_deprecated_doc_entry_fails(monkeypatch):
    # Documenting a @Deprecated object is a non-public doc entry.
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "mock_function"],
        autoclass_entries=["MockClass"],
    )


def test_policy_02_deprecated_doc_entry_passes_when_whitelisted(monkeypatch):
    # The same deprecated entry is allowed when explicitly white-listed.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "mock_function"],
        autoclass_entries=["MockClass"],
        doc_only_whitelist={_CANONICAL_DEPRECATED},
    )


def test_policy_02_documented_method_passes(monkeypatch):
    # A documented but un-annotated method must not be flagged non-public.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "MockClass.mock_method"],
        autoclass_entries=["MockClass"],
    )


def test_policy_04_duplicate_doc_entry_fails(monkeypatch):
    # mock_w00t documented in both an autosummary and an autoclass block.
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t"],
        autoclass_entries=["MockClass", "mock_w00t"],
    )


def test_policy_04_intentional_duplicate_passes(monkeypatch):
    # The same duplicate is allowed when added to the intentional list.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t"],
        autoclass_entries=["MockClass", "mock_w00t"],
        intentional_duplicate_apis={_CANONICAL_W00T},
    )


# --- Unwalked-subpackage coverage guard --------------------------------------

_PKG = "ci.ray_ci.doc.mock"


def test_unwalked_violations_covered_is_ignored():
    # A child reached by some walk is covered, regardless of its API surface.
    assert (
        cmd._unwalked_violations(
            {"ray.data.foo": (True, True)},
            covered={"ray.data.foo"},
            allowlist=set(),
        )
        == []
    )


def test_unwalked_violations_allowlisted_is_ignored():
    # Neither an unimportable nor an annotated-but-unwalked child fails when it is on
    # the reviewed allowlist (this is the ray.data.llm case).
    assert (
        cmd._unwalked_violations(
            {"ray.data.llm": (False, False), "ray.serve.foo": (True, True)},
            covered=set(),
            allowlist={"ray.data.llm", "ray.serve.foo"},
        )
        == []
    )


def test_unwalked_violations_annotated_not_walked_fails():
    # Imports fine, exposes public API, but no walk reaches it -> coverage hole.
    assert cmd._unwalked_violations(
        {"ray.serve.llm": (True, True)},
        covered=set(),
        allowlist=set(),
    ) == [("ray.serve.llm", "annotated-not-walked")]


def test_unwalked_violations_import_error_fails():
    # Cannot be imported here, so its surface cannot be verified -> must be explicit.
    assert cmd._unwalked_violations(
        {"ray.data.llm": (False, False)},
        covered=set(),
        allowlist=set(),
    ) == [("ray.data.llm", "unverifiable-import-error")]


def test_unwalked_violations_importable_without_api_is_ignored():
    # A plain (unannotated) module that nobody walks is not a coverage hole.
    assert (
        cmd._unwalked_violations(
            {"ray.data.util": (True, False)},
            covered=set(),
            allowlist=set(),
        )
        == []
    )


def test_unwalked_violations_are_sorted():
    result = cmd._unwalked_violations(
        {
            "ray.z.mod": (True, True),
            "ray.a.mod": (False, False),
        },
        covered=set(),
        allowlist=set(),
    )
    assert result == [
        ("ray.a.mod", "unverifiable-import-error"),
        ("ray.z.mod", "annotated-not-walked"),
    ]


def test_immediate_child_modules_lists_submodules():
    children = cmd._immediate_child_modules(_PKG)
    assert f"{_PKG}.mock_module" in children


def test_immediate_child_modules_of_plain_module_is_empty():
    # mock_module is a module, not a package: it has no submodules to enumerate.
    assert cmd._immediate_child_modules(f"{_PKG}.mock_module") == []


def test_import_status_detects_public_api():
    # mock_module defines @PublicAPI classes/functions in its own namespace.
    assert cmd._import_status(f"{_PKG}.mock_module") == (True, True)


def test_import_status_unimportable_module():
    assert cmd._import_status(f"{_PKG}.does_not_exist") == (False, False)


def test_import_status_survives_exploding_lazy_attribute(monkeypatch):
    # A module that imports fine but whose attribute access triggers a heavy optional
    # import (the PEP 562 __getattr__ pattern) must not crash the check: the bad
    # attribute is skipped, the safe one is still inspected.
    class _Exploding:
        __name__ = "fake_exploding_module"

        def __dir__(self):
            return ["boom", "safe"]

        @property
        def boom(self):
            raise ModuleNotFoundError("No module named 'transformers'")

        safe = 123

    monkeypatch.setitem(sys.modules, "fake_exploding_module", _Exploding())
    assert cmd._import_status("fake_exploding_module") == (True, False)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
