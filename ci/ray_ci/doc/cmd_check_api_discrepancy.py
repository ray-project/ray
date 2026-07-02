import importlib
import pkgutil
import sys
from typing import Dict, List, Set, Tuple

import click

from ci.ray_ci.doc.api import API
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.module import Module

TEAM_API_CONFIGS = {
    "data": {
        "head_modules": {"ray.data", "ray.data.grouped_data"},
        "head_doc_file": "doc/source/data/api/api.rst",
        # List of APIs that are not following our API policy, and we will be fixing, or
        # we cannot deprecate them although we want to
        "white_list_apis": {
            # not sure what to do
            "ray.data.dataset.MaterializedDataset",
            # special case where we cannot deprecate although we want to
            "ray.data.random_access_dataset.RandomAccessDataset",
        },
    },
    "serve": {
        # ray.serve.llm is a public API surface (ray.serve.llm.LLMConfig,
        # build_openai_app, ...) that ray/serve/__init__.py does not import, so the
        # ray.serve walk never reaches it. It is import-safe under the docbuild image
        # (transformers is a soft try_import; vllm is only imported lazily), so it can
        # be a walk root of its own. The unwalked-subpackage guard would otherwise flag
        # it. (Its sibling ray.data.llm is NOT import-safe -- see UNWALKED_*_ALLOWLIST.)
        "head_modules": {"ray.serve", "ray.serve.llm"},
        "head_doc_file": "doc/source/serve/api/index.md",
        "white_list_apis": {
            # private versions of request router APIs
            "ray.serve._private.common.ReplicaID",
            "ray.serve._private.request_router.common.PendingRequest",
            "ray.serve._private.request_router.pow_2_router.PowerOfTwoChoicesRequestRouter",
            "ray.serve._private.request_router.request_router.RequestRouter",
            "ray.serve._private.request_router.replica_wrapper.RunningReplica",
            "ray.serve._private.request_router.request_router.FIFOMixin",
            "ray.serve._private.request_router.request_router.LocalityMixin",
            "ray.serve._private.request_router.request_router.MultiplexMixin",
        },
    },
    "core": {
        "head_modules": {"ray"},
        "head_doc_file": "doc/source/ray-core/api/index.rst",
        "white_list_apis": {
            # These APIs will be documented in near future
            "ray.util.scheduling_strategies.DoesNotExist",
            "ray.util.scheduling_strategies.Exists",
            "ray.util.scheduling_strategies.NodeLabelSchedulingStrategy",
            "ray.util.scheduling_strategies.In",
            "ray.util.scheduling_strategies.NotIn",
            # TODO(jjyao): document this API
            "ray.ObjectRefGenerator",
            # TODO(jjyao): document or deprecate these APIs
            "ray.experimental.compiled_dag_ref.CompiledDAGFuture",
            "ray.experimental.compiled_dag_ref.CompiledDAGRef",
            "ray.cross_language.cpp_actor_class",
            "ray.cross_language.cpp_function",
            "ray.client_builder.ClientContext",
            "ray.remote_function.RemoteFunction",
        },
        # Canonical names that are intentionally documented in more than one
        # place (Policy 04). ActorMethod.bind is documented once in the Ray Core
        # API and once in the Compiled Graph API; conf.py's DuplicateObjectFilter
        # mirrors this exemption for the Sphinx render. ray.remote (canonical
        # ray._private.worker.remote) is cross-listed under both Tasks and
        # Actors in ray-core/api/core.rst, since @ray.remote defines both.
        "intentional_duplicate_apis": {
            "ray.actor.ActorMethod.bind",
            "ray._private.worker.remote",
        },
    },
    "train": {
        "head_modules": {"ray.train"},
        "head_doc_file": "doc/source/train/api/api.rst",
        "white_list_apis": {
            # NOTE: These APIs are documented in a separate file (deprecated.rst).
            # These are deprecated APIs, so just white-listing them here for CI.
            "ray.train.error.SessionMisuseError",
            "ray.train.base_trainer.TrainingFailedError",
            "ray.train.TrainingFailedError",
            "ray.train.context.TrainContext",
            "ray.train.context.get_context",
        },
    },
    "tune": {
        "head_modules": {"ray.tune"},
        "head_doc_file": "doc/source/tune/api/api.rst",
        "white_list_apis": {
            # Already documented as ray.tune.search.ConcurrencyLimiter
            "ray.tune.search.searcher.ConcurrencyLimiter",
            # TODO(ml-team): deprecate these APIs
            "ray.tune.utils.log.Verbosity",
        },
    },
    "rllib": {
        "head_modules": {"ray.rllib"},
        "head_doc_file": "doc/source/rllib/package_ref/index.rst",
        "white_list_apis": {},
    },
}

# Annotated public subpackages that no team's walk reaches AND that we intentionally
# do not add to any head_modules, keyed to the reason. The coverage guard
# (_check_unwalked_annotated_subpackages) fails on any unwalked annotated subpackage
# that is not listed here, so this list is the single reviewed record of what is
# knowingly left out of the code<->docs consistency check and why.
UNWALKED_ANNOTATED_ALLOWLIST: Dict[str, str] = {
    # ray.data.llm eagerly imports the vLLM/SGLang engine processor configs, which do
    # a hard `import transformers` at module load. transformers is absent from the
    # docbuild image (python/deplocks/docs/docbuild_depset_py3.11.lock), so importing
    # ray.data.llm here raises ModuleNotFoundError -- it cannot be a walk root. Its
    # public surface is documented in doc/source/data/api/llm.rst.
    "ray.data.llm": (
        "Eager `import transformers` (via the vLLM/SGLang engine processor configs) "
        "is not installed in the docbuild image, so ray.data.llm cannot be imported "
        "by this check. Documented in doc/source/data/api/llm.rst."
    ),
}


def _team_head_modules() -> Set[str]:
    """Every module named as a walk root across all teams."""
    heads = set()
    for config in TEAM_API_CONFIGS.values():
        heads.update(config["head_modules"])
    return heads


def _covered_module_names() -> Set[str]:
    """Union of the module names every team's walk actually reaches.

    A subpackage is "covered" only if some walk truly reaches it -- not merely if its
    name is prefixed by a head module. That distinction is the point: ray.data.llm is
    prefixed by the ray.data head yet is never imported by ray/data/__init__.py, so it
    is not covered and the guard can catch it.
    """
    covered = set()
    for head in _team_head_modules():
        # A head module that cannot be imported (optional deps absent) contributes
        # nothing to the covered set; the guard reports it as its own team's failure.
        try:
            covered.update(Module(head).get_reachable_modules())
        except Exception:  # noqa: BLE001 - import-time failure of a configured head
            continue
    return covered


def _immediate_child_modules(package: str) -> List[str]:
    """Fully-qualified names of the immediate submodules of an importable package.

    Private (leading-underscore) children are skipped: they are never public API
    surfaces, and they are the ones most likely to carry exotic optional-dep imports.
    Returns [] when `package` is not importable or is a plain module (no submodules).
    """
    try:
        pkg = importlib.import_module(package)
    except Exception:  # noqa: BLE001 - handled by the head-module check elsewhere
        return []
    if not hasattr(pkg, "__path__"):
        return []
    children = []
    try:
        entries = list(pkgutil.iter_modules(pkg.__path__, prefix=f"{package}."))
    except Exception:  # noqa: BLE001 - a bad/inaccessible __path__ entry
        # Enumerate nothing rather than crash the whole consistency check; a package
        # we cannot list simply contributes no children to guard.
        return []
    for info in entries:
        if info.name.rsplit(".", 1)[-1].startswith("_"):
            continue
        children.append(info.name)
    return children


def _import_status(module: str) -> Tuple[bool, bool]:
    """Return (importable, defines_public_api) for a module name.

    defines_public_api mirrors the walk's rule: an attribute is a public API of this
    module only if it is @PublicAPI-annotated (has `_annotated`) AND is defined within
    this module's namespace (so re-exports owned by other modules do not count).
    """
    try:
        mod = importlib.import_module(module)
    except Exception:  # noqa: BLE001 - optional-dep ImportError et al.
        return (False, False)
    for attr in dir(mod):
        # getattr can itself raise: a module with a PEP 562 __getattr__ (the very lazy
        # pattern this guard exists to reason about, e.g. the batch processor package)
        # can trigger a heavy optional-dep import on attribute access. Skip an attribute
        # we cannot inspect rather than crash the check for every team.
        try:
            obj = getattr(mod, attr, None)
            origin = getattr(obj, "__module__", None)
            if not origin or (origin != module and not origin.startswith(f"{module}.")):
                continue
            if hasattr(obj, "_annotated"):
                return (True, True)
        except Exception:  # noqa: BLE001 - lazy attribute access blew up
            continue
    return (True, False)


def _unwalked_violations(
    child_status: Dict[str, Tuple[bool, bool]],
    covered: Set[str],
    allowlist: Set[str],
) -> List[Tuple[str, str]]:
    """Pure decision core of the coverage guard.

    Given each enumerated child's (importable, defines_public_api) status, the set of
    walk-covered module names, and the reviewed allowlist, return the (name, category)
    pairs that should fail the guard, sorted by name. Categories:

      - "annotated-not-walked": imports fine and exposes @PublicAPI, but no walk
        reaches it -- a silent coverage hole. Add it to a head_modules set.
      - "unverifiable-import-error": cannot be imported here, so we cannot prove it is
        free of public API. Make the exclusion explicit on the allowlist (with a
        reason) or make it importable and walk it.
    """
    violations = []
    for name, (importable, defines_public_api) in child_status.items():
        if name in covered or name in allowlist:
            continue
        if not importable:
            violations.append((name, "unverifiable-import-error"))
        elif defines_public_api:
            violations.append((name, "annotated-not-walked"))
    return sorted(violations)


def _check_unwalked_annotated_subpackages() -> bool:
    """Guard: fail when an annotated public subpackage escapes every team's walk.

    The code<->docs consistency check only sees APIs the walk reaches, and the walk
    only follows submodules a parent __init__ actually imports. A public subpackage
    that its parent does not import (e.g. ray.data.llm, ray.serve.llm) is invisible to
    the check -- its APIs can silently rot undocumented. This guard enumerates one
    level of subpackages under every configured head module and fails on any that is
    neither walked nor knowingly excluded via UNWALKED_ANNOTATED_ALLOWLIST.
    """
    print(
        "--- Validating that annotated subpackages are reachable by some walk...",
        file=sys.stderr,
    )
    covered = _covered_module_names()

    child_status: Dict[str, Tuple[bool, bool]] = {}
    for head in _team_head_modules():
        for child in _immediate_child_modules(head):
            if child not in child_status:
                child_status[child] = _import_status(child)

    violations = _unwalked_violations(
        child_status, covered, set(UNWALKED_ANNOTATED_ALLOWLIST)
    )
    if not violations:
        return True

    for name, category in violations:
        if category == "annotated-not-walked":
            print(
                f"\t{name}: exposes public APIs but no team walk reaches it. Add it "
                "to a team's head_modules, or to UNWALKED_ANNOTATED_ALLOWLIST if the "
                "omission is intentional.",
                file=sys.stderr,
            )
        else:
            print(
                f"\t{name}: cannot be imported by this check (likely missing optional "
                "dependencies), so its public API surface cannot be verified. Make it "
                "importable and add it to head_modules, or record the exclusion in "
                "UNWALKED_ANNOTATED_ALLOWLIST with a reason.",
                file=sys.stderr,
            )
    print(
        "Some annotated subpackages escape the API consistency check. See above.",
        file=sys.stderr,
    )
    return False


def _check_team(ray_checkout_dir: str, team: str) -> bool:
    config = TEAM_API_CONFIGS[team]

    # Load all APIs from the codebase
    api_in_codes = {}
    for module in config["head_modules"]:
        module = Module(module)
        api_in_codes.update(
            {api.get_canonical_name(): api for api in module.get_apis()}
        )

    # Load all APIs from the documentation. Keep the raw list (not a set): Policy
    # 04 needs to see a canonical name that was documented more than once.
    autodoc = Autodoc(f"{ray_checkout_dir}/{config['head_doc_file']}")
    doc_apis = autodoc.get_apis()
    api_in_docs = {api.get_canonical_name() for api in doc_apis}

    # Load the white list APIs
    white_list_apis = config["white_list_apis"]

    passed = True

    # Policy 01: all public APIs should be documented (code subset of docs).
    print(
        f"--- Validating that public {team} APIs should be documented...",
        file=sys.stderr,
    )
    good_apis, bad_apis = API.split_good_and_bad_apis(
        api_in_codes, api_in_docs, white_list_apis
    )

    if good_apis:
        print("Public APIs that are documented:", file=sys.stderr)
        for api in good_apis:
            print(f"\t{api}", file=sys.stderr)

    if bad_apis:
        print("Public APIs that are NOT documented:", file=sys.stderr)
        for api in bad_apis:
            print(f"\t{api}", file=sys.stderr)
        print(
            f"Some public {team} APIs are not documented. Please document them.",
            file=sys.stderr,
        )
        passed = False

    # Policy 02: all documented APIs should resolve to public code (docs subset
    # of code). A documented name that no longer imports, or that resolves to a
    # deprecated / private object, is a stale or wrong doc entry.
    print(
        f"--- Validating that documented {team} APIs resolve to public code...",
        file=sys.stderr,
    )
    doc_only_whitelist = white_list_apis | config.get("doc_only_whitelist", set())
    unresolved_apis, non_public_apis = API.split_resolvable_and_broken_doc_apis(
        doc_apis, doc_only_whitelist
    )

    if unresolved_apis:
        print("Documented APIs that do NOT resolve to any object:", file=sys.stderr)
        for api in unresolved_apis:
            print(f"\t{api}", file=sys.stderr)
        print(
            f"Some documented {team} APIs do not resolve. Remove or fix the doc "
            "entries (deleted, renamed, or misspelled names).",
            file=sys.stderr,
        )
        passed = False

    if non_public_apis:
        print(
            "Documented APIs that resolve to deprecated / private objects:",
            file=sys.stderr,
        )
        for api in non_public_apis:
            print(f"\t{api}", file=sys.stderr)
        print(
            f"Some documented {team} APIs are not public. Stop documenting them, "
            "or white-list them if the documentation is intentional.",
            file=sys.stderr,
        )
        passed = False

    # Policy 04: no canonical name may be documented in more than one block.
    print(
        f"--- Validating that {team} APIs are documented exactly once...",
        file=sys.stderr,
    )
    intentional_duplicate_apis = config.get("intentional_duplicate_apis", set())
    duplicate_apis = API.find_duplicate_doc_apis(doc_apis, intentional_duplicate_apis)

    if duplicate_apis:
        print("APIs documented in more than one place:", file=sys.stderr)
        for api in duplicate_apis:
            print(f"\t{api}", file=sys.stderr)
        print(
            f"Some {team} APIs are documented more than once. Document each in a "
            "single place, or white-list intentional duplicates.",
            file=sys.stderr,
        )
        passed = False

    return passed


@click.command()
@click.argument("ray_checkout_dir", required=True, type=str)
@click.argument(
    "team", default="ALL", type=click.Choice(list(TEAM_API_CONFIGS.keys()) + ["ALL"])
)
def main(ray_checkout_dir: str, team: str) -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    if team != "ALL":
        if not _check_team(ray_checkout_dir, team):
            exit(1)
        return

    all_pass = True
    # Needs to do core first, otherwise, the APIs in other teams may be covered by core.
    # This is due to the side effect of "importlib" and walking through the modules.
    if not _check_team(ray_checkout_dir, "core"):
        all_pass = False
    for team in TEAM_API_CONFIGS:
        if team == "core":
            continue
        if not _check_team(ray_checkout_dir, team):
            all_pass = False
    # Cross-team guard: catch annotated subpackages that no team's walk reaches.
    if not _check_unwalked_annotated_subpackages():
        all_pass = False
    if not all_pass:
        exit(1)


if __name__ == "__main__":
    main()
