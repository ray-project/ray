import sys

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
        },
        # Canonical names intentionally documented in more than one place:
        # to_arrow_refs / to_numpy_refs / to_pandas_refs appear on both the
        # Dataset API page (dataset.rst) and the saving-data page (saving_data.rst).
        "intentional_duplicate_apis": {
            "ray.data.dataset.Dataset.to_arrow_refs",
            "ray.data.dataset.Dataset.to_numpy_refs",
            "ray.data.dataset.Dataset.to_pandas_refs",
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
        },
    },
    "rllib": {
        "head_modules": {"ray.rllib"},
        "head_doc_file": "doc/source/rllib/package_ref/index.rst",
        "white_list_apis": set(),
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
    if not all_pass:
        exit(1)


if __name__ == "__main__":
    main()
