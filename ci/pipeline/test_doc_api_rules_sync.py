"""Keep the ``doc_api`` conditional-testing rule in sync with the API-page set.

The ``doc_api`` tag in ``.buildkite/test.rules.txt`` selects the API-consistency
CI checks ("doc: check API annotations" and "doc: check API doc consistency" in
``.buildkite/doc.rayci.yml``). It covers two populations: the API reference
pages, and the autodoc machinery that determines what those pages contain
(``conf.py``, ``api_autogen.py``, ``api_mock_imports.py``, the in-repo Sphinx
extensions, and the docs dependency locks).

This test polices the first population only. Its directory list must track
``API_PATH_PREFIXES`` in ``doc/source/_ext/api_sidebar.py``, which is the source
of truth for "this page documents public API surface." Machinery directories are
listed in ``_NON_PAGE_DOC_API_DIRS`` and skipped; machinery routed as individual
files never reaches the comparison at all.

If a library adds an API reference directory to one and not the other, the
checks silently stop firing on the new pages (or a stale rule fires on pages
that are no longer API surface). A hardcoded per-page test can't catch that: it
asserts the paths it already lists, not the ones a reorg introduces. This test
compares the two directory sets directly, so it fails when they drift, modulo a
small set of documented, intentional exceptions.
"""
import ast
import sys
from pathlib import Path

import pytest
from determine_tests_to_run import TagRuleSet

_RAYCI_VERSION_FILE = ".rayciversion"
_DOC_SOURCE = "doc/source/"

# Intentional, documented divergences between API_PATH_PREFIXES and the doc_api
# rule's directory list. Keep both sets short; every entry is a deliberate
# decision, not a place to let drift hide.

# In API_PATH_PREFIXES but deliberately NOT a doc_api trigger. The APIs landing
# page (apis/index) is curated navigation, not an autosummary surface, so the
# consistency check has nothing to validate there and shouldn't pay a docbuild
# to run on edits to it.
_ALLOWED_ONLY_IN_API_PREFIXES = {"apis/"}

# In the doc_api rule but NOT in API_PATH_PREFIXES. Ray Core's api/index.rst
# toctrees into ray-observability/reference/api.rst, so those pages are part of
# the Core API surface the check walks even though they live outside
# ray-core/api/.
_ALLOWED_ONLY_IN_RULE = {"ray-observability/reference/"}

# Directories routed to doc_api that are not API reference pages. The doc_api
# tag covers two populations: the API reference pages (which this test keeps
# aligned with API_PATH_PREFIXES) and the machinery that determines what those
# pages contain. Only the first population participates in the comparison, so
# machinery directories are excluded from it entirely rather than compared as
# doc/source pages. Stored without a trailing slash to match TagRuleSet's
# rule.dirs.
#
#   - ci/ray_ci/doc     the API-consistency checker's own source; emits doc_api
#                       so editing the checker re-runs the checks it implements
#   - doc/source/_ext   the in-repo Sphinx extensions. api_sidebar.py lives here
#                       and *defines* API_PATH_PREFIXES, so it is the input to
#                       this test's comparison, never a row in it. Comparing it
#                       as a page would assert `_ext/` is an API path prefix,
#                       which it is not.
#
# Machinery routed as individual files rather than directories (conf.py,
# api_autogen.py, api_mock_imports.py, custom_directives.py,
# requirements-doc.txt, doc.rayci.yml, the docs deplocks) needs no entry here:
# TagRuleSet keeps files and patterns separate from dirs, and this test only
# walks rule.dirs.
_NON_PAGE_DOC_API_DIRS = {"ci/ray_ci/doc", "doc/source/_ext"}


# conf.py imports that are deliberately NOT autodoc machinery, so they route to
# `doc` alone and do not trigger the API-consistency checks. Every entry is a
# judgment that this module cannot change what the API reference documents.
#
#   - template_collections  fetches example templates at build time; it affects
#                           which template pages render, never which symbols
#                           autodoc resolves
#
# Adding an entry here is the explicit way to say "this new conf.py import can't
# affect the API surface." Leaving a genuinely API-affecting module out of the
# doc_api rules and out of this list is what this test exists to prevent.
_CONF_IMPORTS_NOT_API_MACHINERY = {"template_collections"}

# Fail-closed floor for the conf.py import scan. The scan decides "is this a
# local module" by checking whether the file exists under doc/source/, so it
# degrades to an empty set -- and a vacuous pass -- if the test's data deps ever
# stop delivering those files into the bazel sandbox. These four are conf.py's
# current local imports; requiring them to be found turns that silent
# degradation into a hard failure.
#
# If a legitimate refactor removes one of these imports, delete it from this set
# in the same change. Do not delete the set.
_MACHINERY_IMPORT_FLOOR = {
    "api_autogen",
    "api_mock_imports",
    "custom_directives",
    "template_collections",
}


def _find_ray_root() -> Path:
    """Walk up from this file and cwd looking for .rayciversion."""
    start = Path(__file__).resolve()
    for parent in start.parents:
        if (parent / _RAYCI_VERSION_FILE).exists():
            return parent
    if (Path.cwd() / _RAYCI_VERSION_FILE).exists():
        return Path.cwd()
    raise FileNotFoundError("Could not find Ray root (missing .rayciversion).")


def _api_path_prefixes(root: Path) -> set:
    """Read API_PATH_PREFIXES out of api_sidebar.py without importing it.

    Importing the module pulls in Sphinx and bs4, which aren't available in the
    CI tooling environment, so parse the assignment out of the AST instead.
    """
    source = (root / "doc" / "source" / "_ext" / "api_sidebar.py").read_text()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "API_PATH_PREFIXES"
            for t in node.targets
        ):
            return set(ast.literal_eval(node.value))
    raise AssertionError("API_PATH_PREFIXES not found in api_sidebar.py")


def _doc_api_rule_dirs(root: Path) -> set:
    """Directories that emit the ``doc_api`` tag in test.rules.txt, normalized
    to be relative to doc/source/ so they compare against API_PATH_PREFIXES."""
    rules = TagRuleSet((root / ".buildkite" / "test.rules.txt").read_text())
    dirs = set()
    for rule in rules.rules:
        if "doc_api" not in rule.tags:
            continue
        for d in rule.dirs:  # stored without a trailing slash
            if d in _NON_PAGE_DOC_API_DIRS:
                # Not an API page (e.g. the checker's own source); intentionally
                # outside the page<->API_PATH_PREFIXES alignment this test checks.
                continue
            assert d.startswith(_DOC_SOURCE), (
                f"doc_api rule directory {d!r} is not under {_DOC_SOURCE!r}; the "
                "sync check assumes API reference pages live under doc/source/."
            )
            dirs.add(d[len(_DOC_SOURCE) :] + "/")
    return dirs


def test_doc_api_rule_matches_api_path_prefixes():
    root = _find_ray_root()

    expected = _api_path_prefixes(root) - _ALLOWED_ONLY_IN_API_PREFIXES
    actual = _doc_api_rule_dirs(root) - _ALLOWED_ONLY_IN_RULE

    missing_from_rule = expected - actual
    extra_in_rule = actual - expected

    assert not missing_from_rule and not extra_in_rule, (
        "The doc_api rule in .buildkite/test.rules.txt has drifted from "
        "API_PATH_PREFIXES in doc/source/_ext/api_sidebar.py.\n"
        f"  API pages with no doc_api rule (add these dirs to test.rules.txt so "
        f"the API checks fire on them): {sorted(missing_from_rule)}\n"
        f"  doc_api rule dirs not in API_PATH_PREFIXES (remove them, or, if the "
        f"divergence is intentional, add them to the documented exception list "
        f"in this test with a comment): {sorted(extra_in_rule)}"
    )


def _conf_py_local_imports(root: Path) -> set:
    """Module names conf.py imports that resolve to a file under doc/source/.

    Parsed out of the AST rather than imported: importing conf.py executes the
    whole Sphinx configuration, which isn't available in the CI tooling image.

    Membership is decided by the file existing under doc/source/, which means
    this function is only correct when those files are present. Under bazel they
    are present because //ci/pipeline:test_doc_api_rules_sync declares the
    //doc:doc_source_modules glob as a data dep. _MACHINERY_IMPORT_FLOOR below
    is the guard against that wiring silently regressing -- without it, a sandbox
    missing the files would return an empty set and the test would pass
    vacuously.
    """
    conf = root / "doc" / "source" / "conf.py"
    names = set()
    for node in ast.walk(ast.parse(conf.read_text())):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return {
        name
        for name in names
        if (root / "doc" / "source" / (name.replace(".", "/") + ".py")).exists()
    }


def test_conf_py_imports_route_to_doc_api():
    """Every local module conf.py imports is classified, not left to a catch-all.

    The autodoc machinery block in test.rules.txt is a hand-enumerated list, and
    a `doc/source/*.py` pattern can't replace it (fnmatch `*` crosses `/`, so it
    would swallow every example script under doc/source/<lib>/doc_code/). That
    makes the list drift-prone in a way that fails silently and badly: a new
    conf.py import that nobody adds to the rules lands on the `doc/*.py`
    catch-all, which emits `core_doc`. It would run Ray Core's doctests and
    example tests, and never the documentation build or the API checks.

    So require every local conf.py import to be either routed to `doc_api` or
    named in _CONF_IMPORTS_NOT_API_MACHINERY as an explicit rendering-only
    exception.
    """
    root = _find_ray_root()
    rules = TagRuleSet((root / ".buildkite" / "test.rules.txt").read_text())

    found = _conf_py_local_imports(root)
    missing_floor = _MACHINERY_IMPORT_FLOOR - found
    assert not missing_floor, (
        "The conf.py import scan did not find these known local imports: "
        f"{sorted(missing_floor)}.\n"
        "Either the test's data deps stopped delivering doc/source/*.py into the "
        "sandbox (check //doc:doc_source_modules in doc/BUILD.bazel and the data "
        "attr of //ci/pipeline:test_doc_api_rules_sync), or conf.py legitimately "
        "dropped the import and _MACHINERY_IMPORT_FLOOR needs updating in the "
        "same change. Without this assertion an empty scan would pass vacuously."
    )

    unclassified = {}
    for module in sorted(found):
        if module in _CONF_IMPORTS_NOT_API_MACHINERY:
            continue
        path = f"doc/source/{module.replace('.', '/')}.py"
        tags, _ = rules.match_tags(path)
        if "doc_api" not in tags:
            unclassified[path] = sorted(tags)

    assert not unclassified, (
        "conf.py imports these modules, but test.rules.txt does not route them "
        "to doc_api, so editing them would not run the API-consistency "
        "checks:\n"
        + "\n".join(f"  {p} currently emits {t}" for p, t in unclassified.items())
        + "\n\nAdd each one to the autodoc machinery block in "
        ".buildkite/test.rules.txt, or, if it genuinely cannot affect the "
        "documented API surface, to _CONF_IMPORTS_NOT_API_MACHINERY in this "
        "test with a comment saying why."
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
