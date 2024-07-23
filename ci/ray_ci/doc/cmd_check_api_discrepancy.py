import click
from typing import Dict, Set

from ci.ray_ci.doc.module import Module
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.api import API

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
    print("Validating that public APIs should be documented...")
    _validate_documented_public_apis(api_in_codes, api_in_docs, white_list_apis)

    return


def _validate_documented_public_apis(
    api_in_codes: Dict[str, API], api_in_docs: Set[str], white_list_apis: Set[str]
) -> None:
    """
    Validate APIs that are public and documented.
    """
    for name, api in api_in_codes.items():
        if not api.is_public():
            continue

        if name in white_list_apis:
            continue

        assert name in api_in_docs, f"\tAPI {api.name} is public but not documented."
        print(f"\tAPI {api.name} is public and documented.")


if __name__ == "__main__":
    main()
