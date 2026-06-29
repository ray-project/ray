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
    @Deprecated), so the coverage check passes only when both are documented.
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


def test_all_checks_pass(monkeypatch):
    # Both public APIs documented exactly once, all resolve, no duplicates.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t"],
        autoclass_entries=["MockClass"],
    )


def test_undocumented_public_api_fails(monkeypatch):
    # mock_w00t (public) is undocumented -> the coverage check fails.
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=[],
        autoclass_entries=["MockClass"],
    )


def test_unresolved_doc_entry_fails(monkeypatch):
    # A documented name that does not resolve (renamed / deleted / typo).
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "renamed_away"],
        autoclass_entries=["MockClass"],
    )


def test_deprecated_doc_entry_fails(monkeypatch):
    # Documenting a @Deprecated object is a non-public doc entry.
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "mock_function"],
        autoclass_entries=["MockClass"],
    )


def test_deprecated_doc_entry_passes_when_whitelisted(monkeypatch):
    # The same deprecated entry is allowed when explicitly white-listed.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "mock_function"],
        autoclass_entries=["MockClass"],
        doc_only_whitelist={_CANONICAL_DEPRECATED},
    )


def test_documented_method_passes(monkeypatch):
    # A documented but un-annotated method must not be flagged non-public.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t", "MockClass.mock_method"],
        autoclass_entries=["MockClass"],
    )


def test_duplicate_doc_entry_fails(monkeypatch):
    # mock_w00t documented in both an autosummary and an autoclass block.
    assert not _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t"],
        autoclass_entries=["MockClass", "mock_w00t"],
    )


def test_intentional_duplicate_passes(monkeypatch):
    # The same duplicate is allowed when added to the intentional list.
    assert _run_check_team(
        monkeypatch,
        autosummary_entries=["mock_w00t"],
        autoclass_entries=["MockClass", "mock_w00t"],
        intentional_duplicate_apis={_CANONICAL_W00T},
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
