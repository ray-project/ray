import importlib
import os
import pkgutil
import sys
from contextlib import contextmanager
from typing import Dict, List, Set, Tuple

import click

from ci.ray_ci.doc.api import API, _is_directly_annotated
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.module import Module

# Each team config carries two exemption lists. Both feed the "every
# @PublicAPI symbol must be documented" check identically (they're unioned at
# check time), but they mean different things to a human reading the file:
#
#   white_list_apis  -- Permanent, intentional exemptions. These symbols are
#                       correctly absent from the team autosummary and are
#                       expected to stay on this list: documented elsewhere, an
#                       intentional alias, or genuinely un-deprecatable. Not
#                       doc debt.
#   tracked_doc_debt  -- Known documentation debt: @PublicAPI symbols still
#                       owed a document-or-deprecate decision. Clearing an entry
#                       means the decision was made and acted on (documented,
#                       deprecated, or the erroneous annotation removed).
#
# Because both sets are unioned, moving an entry between them never changes
# what the check accepts -- only whether a reviewer reads it as "correct and
# permanent" or "we still owe a decision here".
TEAM_API_CONFIGS = {
    "data": {
        # ray.data.llm is a public API surface (ray.data.llm.build_llm_processor,
        # vLLMEngineProcessorConfig, ...) that ray/data/__init__.py does not import, so
        # the ray.data walk never reaches it. It is walked as its own root here: its
        # eager `import transformers` (via the vLLM/SGLang engine processor configs)
        # resolves under _mock_uninstalled_backends, which mocks the backends the
        # docbuild image lacks. Its surface is documented in doc/source/data/api/llm.rst
        # (reachable from api.rst's toctree).
        "head_modules": {"ray.data", "ray.data.grouped_data", "ray.data.llm"},
        "head_doc_file": "doc/source/data/api/api.rst",
        "white_list_apis": {
            # special case where we cannot deprecate although we want to
            "ray.data.random_access_dataset.RandomAccessDataset",
        },
        "tracked_doc_debt": {
            # not sure what to do
            "ray.data.dataset.MaterializedDataset",
            # Deprecated but still documented. Remove from the docs, or move to a
            # deprecated-only page, then drop these.
            "ray.data.aggregate.AggregateFn",
            "ray.data.dataset.Dataset.iter_tf_batches",
            "ray.data.read_api.read_unity_catalog",
            # Private-named accessor classes documented under expressions.rst
            # "Expression namespaces". Document the public accessor surface, or
            # promote these to public names, then drop them.
            "ray.data.namespace_expressions.arr_namespace._ArrayNamespace",
            "ray.data.namespace_expressions.dt_namespace._DatetimeNamespace",
            "ray.data.namespace_expressions.list_namespace._ListNamespace",
            "ray.data.namespace_expressions.string_namespace._StringNamespace",
            "ray.data.namespace_expressions.struct_namespace._StructNamespace",
            # Public @PublicAPI surface reached by walking ray.data.llm but not yet
            # documented in llm.rst; document-or-deprecate decision pending Ray Data.
            "ray.data.llm.HttpRequestStageConfig",
            "ray.data.llm.ServeDeploymentProcessorConfig",
        },
        # Documented public APIs whose canonical name resolves under a private
        # (._internal.) module: the class is re-exported from ray.data.__all__
        # while its implementation lives in _internal. They resolve fine and are
        # correctly documented; only the resolve check's private-name heuristic
        # flags them. doc_only_whitelist exempts them from that check
        # (split_resolvable_and_broken_doc_apis) without touching the
        # must-be-documented check. Permanent (implementation location, not debt).
        "doc_only_whitelist": {
            "ray.data._internal.compute.ActorPoolStrategy",
            "ray.data._internal.compute.TaskPoolStrategy",
            "ray.data._internal.execution.interfaces.execution_options.ExecutionOptions",
            "ray.data._internal.execution.interfaces.execution_options.ExecutionResources",
            "ray.data._internal.logical.operators.n_ary_operator.MixStoppingCondition",
            "ray.data._internal.random_config.RandomSeedConfig",
            # Same pattern under ray.data.llm: the @PublicAPI Processor is
            # re-exported through ray.data.llm (documented in data/api/llm.rst)
            # while its implementation lives under ray.llm._internal.
            "ray.llm._internal.batch.processor.base.Processor",
        },
        # Canonical names intentionally documented in more than one place. Each
        # is listed both in the generated ray.data.Dataset.rst method table
        # (included by dataset.rst) and in saving_data.rst's save-topic grouping.
        # to_arrow_refs / to_numpy_refs / to_pandas_refs are the original three;
        # the to_* / write_* conversion and write methods are the same pattern.
        "intentional_duplicate_apis": {
            "ray.data.dataset.Dataset.to_arrow_refs",
            "ray.data.dataset.Dataset.to_numpy_refs",
            "ray.data.dataset.Dataset.to_pandas_refs",
            "ray.data.dataset.Dataset.to_daft",
            "ray.data.dataset.Dataset.to_dask",
            "ray.data.dataset.Dataset.to_mars",
            "ray.data.dataset.Dataset.to_modin",
            "ray.data.dataset.Dataset.to_pandas",
            "ray.data.dataset.Dataset.to_spark",
            "ray.data.dataset.Dataset.write_csv",
            "ray.data.dataset.Dataset.write_iceberg",
            "ray.data.dataset.Dataset.write_images",
            "ray.data.dataset.Dataset.write_json",
            "ray.data.dataset.Dataset.write_mongo",
            "ray.data.dataset.Dataset.write_numpy",
            "ray.data.dataset.Dataset.write_parquet",
            "ray.data.dataset.Dataset.write_tfrecords",
        },
    },
    "serve": {
        # ray.serve.llm is a public API surface (ray.serve.llm.LLMConfig,
        # build_openai_app, ...) that ray/serve/__init__.py does not import, so the
        # ray.serve walk never reaches it. It is import-safe under the docbuild image
        # (transformers is a soft try_import; vllm is only imported lazily), so it can
        # be a walk root of its own. The unwalked-subpackage guard would otherwise flag
        # it. (Its sibling ray.data.llm is walked the same way, under the data team.)
        "head_modules": {"ray.serve", "ray.serve.llm"},
        "head_doc_file": "doc/source/serve/api/index.md",
        "white_list_apis": set(),
        "tracked_doc_debt": {
            # private versions of request router APIs
            "ray.serve._private.common.ReplicaID",
            "ray.serve._private.request_router.common.PendingRequest",
            "ray.serve._private.request_router.pow_2_router.PowerOfTwoChoicesRequestRouter",
            "ray.serve._private.request_router.request_router.RequestRouter",
            "ray.serve._private.request_router.replica_wrapper.RunningReplica",
            "ray.serve._private.request_router.request_router.FIFOMixin",
            "ray.serve._private.request_router.request_router.LocalityMixin",
            "ray.serve._private.request_router.request_router.MultiplexMixin",
            # Public @PublicAPI surface reached by walking ray.serve.llm but not yet
            # documented; document-or-deprecate decision pending Ray Serve.
            "ray.serve.llm.build_dp_deployment",
            "ray.serve.llm.build_dp_openai_app",
            "ray.serve.llm.build_pd_openai_app",
        },
    },
    "core": {
        "head_modules": {"ray"},
        "head_doc_file": "doc/source/ray-core/api/index.rst",
        "white_list_apis": set(),
        "tracked_doc_debt": {
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
        # place. ActorMethod.bind is documented once in the Ray Core
        # API and once in the Compiled Graph API; conf.py's DuplicateObjectFilter
        # mirrors this exemption for the Sphinx render. ray.remote (canonical
        # ray._private.worker.remote) is cross-listed under both Tasks and
        # Actors in ray-core/api/core.rst, since @ray.remote defines both.
        # ray.get / ray.put / ray.method are additionally cross-listed in
        # direct-transport.rst (their Ray Direct Transport usage) beyond core.rst.
        "intentional_duplicate_apis": {
            "ray.actor.ActorMethod.bind",
            "ray._private.worker.remote",
            "ray._private.worker.get",
            "ray._private.worker.put",
            "ray.actor.method",
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
        },
        "tracked_doc_debt": {
            # TODO(ml-team): deprecate these APIs
            "ray.tune.utils.log.Verbosity",
            # Documented dunder on a public class; flagged non-public. Document
            # the class-level behavior instead of the dunder, then drop this.
            "ray.tune.stopper.stopper.Stopper.__call__",
        },
        # Documented in more than one place (scheduler overview and the
        # per-scheduler page).
        "intentional_duplicate_apis": {
            "ray.tune.schedulers.async_hyperband.AsyncHyperBandScheduler",
        },
    },
    "rllib": {
        "head_modules": {"ray.rllib"},
        "head_doc_file": "doc/source/rllib/package_ref/index.rst",
        # Private-by-name methods RLlib intentionally documents as a public
        # override / customization contract. The RLModule._forward* hooks that
        # were whitelisted here are now exempted generically by their
        # @OverrideToImplementCustomLogic marker (see API._is_override_hook in
        # api.py), so only the Learner / offline hooks that lack that marker
        # still need an explicit entry.
        "white_list_apis": {
            "ray.rllib.core.learner.learner.Learner._make_module",
            # OfflinePreLearner / OfflineData methods documented as the
            # offline-RL customization surface in rllib-offline.rst (which has a
            # worked example of overriding _map_to_episodes).
            "ray.rllib.offline.offline_data.OfflineData.__init__",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner.__call__",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner._map_to_episodes",
        },
        # RLModule instance attributes (observation_space, action_space,
        # inference_only, model_config) are assigned in setup(), not declared on
        # the class, so the checker's import-walk resolves them to None and
        # treats them as unresolved. They are legitimately documented via
        # autosummary, so exempt them from the doc-resolves-to-code check only.
        "doc_only_whitelist": {
            "ray.rllib.core.rl_module.rl_module.RLModule.observation_space",
            "ray.rllib.core.rl_module.rl_module.RLModule.action_space",
            "ray.rllib.core.rl_module.rl_module.RLModule.inference_only",
            "ray.rllib.core.rl_module.rl_module.RLModule.model_config",
        },
        # Canonical names intentionally documented in more than one place:
        # build_learner / build_learner_group / learners appear on both the
        # AlgorithmConfig page (algorithm-config.rst) and the learner/offline
        # pages; save_to_path / restore_from_path are inherited from
        # Checkpointable and shown on each Checkpointable subclass's API page.
        "intentional_duplicate_apis": {
            "ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_learner",
            "ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_learner_group",
            "ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners",
            "ray.rllib.utils.checkpoints.Checkpointable.save_to_path",
            "ray.rllib.utils.checkpoints.Checkpointable.restore_from_path",
        },
    },
}

# Annotated public subpackages that no team's walk reaches, keyed to the reason.
# The coverage guard (_check_unwalked_annotated_subpackages) fails on any unwalked
# annotated subpackage not listed here, so this is the single reviewed record of
# what the code<->docs consistency check leaves out.
#
# These entries are tracked coverage debt, not permanent exclusions: each is a
# public surface no walk reaches yet, owing a document-or-deprecate (or
# add-to-head_modules) decision from its owning library team. They're listed so the
# guard passes against a frozen baseline while those decisions are pending; any new
# gap outside this list still fails the build, so the debt can only shrink.
UNWALKED_ANNOTATED_ALLOWLIST: Dict[str, str] = {
    # annotated-not-walked: imports cleanly and exposes @PublicAPI, but no walk
    # reaches it. Resolve by adding to a team's head_modules once its surface is
    # reviewed, or by removing the annotation.
    "ray.cluster_utils": "annotated-not-walked; pending Ray Core",
    "ray.serve.deployment": "annotated-not-walked; pending Ray Serve",
    "ray.serve.llm.deployment": "annotated-not-walked; pending Ray Serve",
    "ray.serve.llm.ingress": "annotated-not-walked; pending Ray Serve",
    "ray.serve.llm.openai_api_models": "annotated-not-walked; pending Ray Serve",
    "ray.serve.llm.request_router": "annotated-not-walked; pending Ray Serve",
    "ray.serve.task_consumer": "annotated-not-walked; pending Ray Serve",
    "ray.serve.task_processor": "annotated-not-walked; pending Ray Serve",
    "ray.serve.taskiq_task_processor": "annotated-not-walked; pending Ray Serve",
    "ray.train.horovod": "annotated-not-walked; pending Ray Train",
    # unverifiable-import: not importable under the docbuild backend mock (missing
    # optional dependency), so the surface can't be checked here. Resolve by making
    # it importable in the docbuild image or removing the annotation.
    "ray.serve.gradio_integrations": "unverifiable-import; pending Ray Serve",
    "ray.tune.automl": "unverifiable-import; pending Ray Tune",
    "ray.workflow": "unverifiable-import; pending Ray Core / Workflow",
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
    module only if it is directly @PublicAPI-annotated (owns `_annotated` rather than
    inheriting it from a base class) AND is defined within this module's namespace
    (so re-exports owned by other modules do not count).
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
            if _is_directly_annotated(obj):
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

    # Load all APIs from the documentation. Keep the raw list (not a set): the
    # duplicate-documentation check needs to see a canonical name documented
    # more than once.
    autodoc = Autodoc(f"{ray_checkout_dir}/{config['head_doc_file']}")
    doc_apis = autodoc.get_apis()
    api_in_docs = {api.get_canonical_name() for api in doc_apis}

    # Load the white list APIs. Permanent exemptions and tracked doc debt are
    # kept in separate config keys for readability; the check treats them the
    # same, so union them here.
    white_list_apis = config["white_list_apis"] | config.get("tracked_doc_debt", set())

    passed = True

    # Every public API must be documented (code is a subset of docs).
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

    # Every documented API must resolve to public code (docs is a subset of
    # code). A documented name that no longer imports, or that resolves to a
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

    # No canonical name may be documented in more than one block.
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


@contextmanager
def _mock_uninstalled_backends(ray_checkout_dir: str):
    """Mock the third-party backends the docbuild image doesn't install.

    The check imports documented names for real (``API.resolve`` /
    ``get_canonical_name`` on the doc side, ``Module.get_apis`` on the code
    side). Optional-dependency modules such as ``ray.data.llm`` /
    ``ray.serve.llm`` / ``ray.train.lightning`` eagerly import backends like
    vLLM, transformers, torch, or pytorch_lightning, which are absent on the CPU
    docbuild runner. Without this they read as unresolved even though the
    rendered docs -- built under the same mocks via conf.py's
    ``autodoc_mock_imports`` -- show them fine. This mirrors that mock so the
    check sees the same API surface the render produces.

    Only third-party modules are mocked; ``ray.*`` is imported for real, so the
    resolve/dedup policy keeps its teeth on Ray's own symbols. The mock list is
    read from doc/source/api_mock_imports.py, the single source of truth shared
    with conf.py.
    """
    from sphinx.ext.autodoc.mock import mock

    doc_source = os.path.abspath(os.path.join(ray_checkout_dir, "doc", "source"))
    sys.path.insert(0, doc_source)
    try:
        from api_mock_imports import absent_mock_modules

        modules_to_mock = absent_mock_modules()
    finally:
        sys.path.remove(doc_source)
        # api_mock_imports is checkout-specific and unqualified, so a copy left
        # in sys.modules would be reused by a later invocation with a different
        # ray_checkout_dir even after doc_source leaves sys.path. Evict it so
        # each invocation re-imports from its own checkout.
        sys.modules.pop("api_mock_imports", None)

    # Mock only the genuinely-absent optional backends, not the full
    # autodoc_mock_imports list: shadowing an installed library (e.g. pandas)
    # would make resolve()'s ``import ray.data`` fail and mass-flag every data
    # entry as unresolved. ray.* is never mocked.
    with mock(modules_to_mock):
        yield


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
    with _mock_uninstalled_backends(ray_checkout_dir):
        if team != "ALL":
            if not _check_team(ray_checkout_dir, team):
                exit(1)
            return

        all_pass = True
        # Needs to do core first, otherwise, the APIs in other teams may be
        # covered by core. This is due to the side effect of "importlib" and
        # walking through the modules.
        if not _check_team(ray_checkout_dir, "core"):
            all_pass = False
        for team in TEAM_API_CONFIGS:
            if team == "core":
                continue
            if not _check_team(ray_checkout_dir, team):
                all_pass = False
        # Cross-team guard: catch annotated subpackages that no team's walk reaches.
        # It runs inside _mock_uninstalled_backends so its coverage check and import
        # probes see the same backend mocks as the walks -- an optional-dependency
        # subpackage promoted to a head module (e.g. ray.data.llm) imports cleanly
        # here and is correctly counted as covered rather than flagged.
        if not _check_unwalked_annotated_subpackages():
            all_pass = False
        if not all_pass:
            exit(1)


if __name__ == "__main__":
    main()
