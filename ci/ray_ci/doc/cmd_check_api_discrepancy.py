import os
import sys
from contextlib import contextmanager

import click

from ci.ray_ci.doc.api import API
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
        "head_modules": {"ray.data", "ray.data.grouped_data"},
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
        "head_modules": {"ray.serve"},
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
        "white_list_apis": set(),
        # RLlib carries the largest un-triaged surface. These are parked as
        # tracked debt to green the now-honest check; each still wants a real
        # resolution from the RLlib team (fix the doc entry, deprecate, or
        # de-annotate). Grouped by failure mode.
        "tracked_doc_debt": {
            # Documented autosummary entries that don't resolve to a live object:
            # instance attributes with no class-level object, and malformed
            # entries whose module path is doubled (a full-path name written
            # under a matching .. currentmodule::). Fix the source pages (or the
            # check's name handling), then drop these.
            "ray.rllib.core.rl_module.rl_module.RLModuleSpec.module_class",
            "ray.rllib.core.rl_module.rl_module.RLModuleSpec.observation_space",
            "ray.rllib.core.rl_module.rl_module.RLModuleSpec.action_space",
            "ray.rllib.core.rl_module.rl_module.RLModuleSpec.model_config",
            "ray.rllib.core.rl_module.rl_module.RLModule.observation_space",
            "ray.rllib.core.rl_module.rl_module.RLModule.action_space",
            "ray.rllib.core.rl_module.rl_module.RLModule.inference_only",
            "ray.rllib.core.rl_module.rl_module.RLModule.model_config",
            "ray.rllib.env.external.rllink.ray.rllib.env.external.rllink.RLlink",
            "ray.rllib.core.rl_module.apis.ray.rllib.core.rl_module.apis.inference_only_api.InferenceOnlyAPI",
            "ray.rllib.core.rl_module.apis.ray.rllib.core.rl_module.apis.q_net_api.QNetAPI",
            "ray.rllib.core.rl_module.apis.ray.rllib.core.rl_module.apis.self_supervised_loss_api.SelfSupervisedLossAPI",
            "ray.rllib.core.rl_module.apis.ray.rllib.core.rl_module.apis.target_network_api.TargetNetworkAPI",
            "ray.rllib.core.rl_module.apis.ray.rllib.core.rl_module.apis.value_function_api.ValueFunctionAPI",
            "ray.rllib.connectors.connector_v2.ray.rllib.connectors.connector_v2.ConnectorV2",
            "ray.rllib.connectors.connector_v2.ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2",
            "ray.rllib.connectors.env_to_module.observation_preprocessor.ray.rllib.connectors.env_to_module.observation_preprocessor.SingleAgentObservationPreprocessor",
            "ray.rllib.connectors.env_to_module.observation_preprocessor.ray.rllib.connectors.env_to_module.observation_preprocessor.MultiAgentObservationPreprocessor",
            # Documented private / dunder members flagged non-public. Document
            # the public surface instead, then drop these.
            "ray.rllib.core.learner.learner.Learner._check_is_built",
            "ray.rllib.core.learner.learner.Learner._make_module",
            "ray.rllib.core.learner.learner.Learner._check_registered_optimizer",
            "ray.rllib.core.learner.learner.Learner._set_optimizer_lr",
            "ray.rllib.core.learner.learner.Learner._get_clip_function",
            "ray.rllib.utils.schedules.scheduler.Scheduler._create_tensor_variable",
            "ray.rllib.core.rl_module.rl_module.RLModule._forward",
            "ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration",
            "ray.rllib.core.rl_module.rl_module.RLModule._forward_inference",
            "ray.rllib.core.rl_module.rl_module.RLModule._forward_train",
            "ray.rllib.offline.offline_data.OfflineData.__init__",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner.__init__",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner.__call__",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner._map_to_episodes",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner._map_sample_batch_to_episode",
            "ray.rllib.offline.offline_prelearner.OfflinePreLearner._should_module_be_updated",
            "ray.rllib.env.multi_agent_episode.MultiAgentEpisode.__len__",
            "ray.rllib.env.single_agent_episode.SingleAgentEpisode.__len__",
        },
        # Documented in more than one place (a hand-written topic page plus the
        # generated class stub).
        "intentional_duplicate_apis": {
            "ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_learner",
            "ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_learner_group",
            "ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners",
            "ray.rllib.utils.checkpoints.Checkpointable.restore_from_path",
            "ray.rllib.utils.checkpoints.Checkpointable.save_to_path",
        },
    },
}


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
        if not all_pass:
            exit(1)


if __name__ == "__main__":
    main()
