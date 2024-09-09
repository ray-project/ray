import click

from ci.ray_ci.doc.module import Module
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.api import API
from ci.ray_ci.utils import logger

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
        "head_modules": {"ray.serve"},
        "head_doc_file": "doc/source/serve/api/index.md",
        "white_list_apis": {},
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
            # TODO(jjyao): document or deprecate these APIs
            "ray.experimental.compiled_dag_ref.CompiledDAGFuture",
            "ray.experimental.compiled_dag_ref.CompiledDAGRef",
            "ray.cross_language.cpp_actor_class",
            "ray.cross_language.cpp_function",
            "ray.client_builder.ClientContext",
            "ray.remote_function.RemoteFunction",
        },
    },
    "train": {
        "head_modules": {"ray.train"},
        "head_doc_file": "doc/source/train/api/api.rst",
        "white_list_apis": {},
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


@click.command()
@click.argument("ray_checkout_dir", required=True, type=str)
@click.argument("team", required=True, type=click.Choice(list(TEAM_API_CONFIGS.keys())))
def main(ray_checkout_dir: str, team: str) -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """

    # Load all APIs from the codebase
    api_in_codes = {}
    for module in TEAM_API_CONFIGS[team]["head_modules"]:
        module = Module(module)
        api_in_codes.update(
            {api.get_canonical_name(): api for api in module.get_apis()}
        )

    # Load all APIs from the documentation
    autodoc = Autodoc(f"{ray_checkout_dir}/{TEAM_API_CONFIGS[team]['head_doc_file']}")
    api_in_docs = {api.get_canonical_name() for api in autodoc.get_apis()}

    # Load the white list APIs
    white_list_apis = TEAM_API_CONFIGS[team]["white_list_apis"]

    # Policy 01: all public APIs should be documented
    logger.info("Validating that public APIs should be documented...")
    good_apis, bad_apis = API.split_good_and_bad_apis(
        api_in_codes, api_in_docs, white_list_apis
    )

    logger.info("Public APIs that are documented:")
    for api in good_apis:
        logger.info(f"\t{api}")

    logger.info("Public APIs that are NOT documented:")
    for api in bad_apis:
        logger.info(f"\t{api}")

    assert not bad_apis, "Some public APIs are not documented. Please document them."

    return


if __name__ == "__main__":
    main()
