import click
from typing import Dict, Set, Tuple

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
            # TODO(can): fix these
            "ray.data.datasource.bigquery_datasource.BigQueryDatasource",
            "ray.data.datasource.binary_datasource.BinaryDatasource",
            "ray.data.datasource.csv_datasource.CSVDatasource",
            "ray.data.datasource.json_datasource.JSONDatasource",
            "ray.data.datasource.mongo_datasource.MongoDatasource",
            "ray.data.datasource.numpy_datasource.NumpyDatasource",
            "ray.data.datasource.parquet_bulk_datasource.ParquetBulkDatasource",
            "ray.data.datasource.parquet_datasource.ParquetDatasource",
            "ray.data.datasource.range_datasource.RangeDatasource",
            "ray.data.datasource.sql_datasource.SQLDatasource",
            "ray.data.datasource.text_datasource.TextDatasource",
            "ray.data.datasource.webdataset_datasource.WebDatasetDatasource",
            "ray.data.datasource.tfrecords_datasource.TFRecordDatasource",
            "ray.data.datasource.tfrecords_datasource.TFXReadOptions",
        },
    },
    "core": {
        "head_modules": {"ray"},
        "head_doc_file": "doc/source/ray-core/api/index.rst",
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

    # Policy 01+02: all public and deprecated APIs should be documented
    print("Validating that public and deprecated APIs should be documented...")
    good_apis, bad_public_apis, bad_deprecated_apis = _validate_documented_public_apis(
        api_in_codes, api_in_docs, white_list_apis
    )
    print("\nGood APIs:")
    for api in good_apis:
        print(api)
    print("\nBad public APIs:")
    for api in bad_public_apis:
        print(api)
    print("\nBad deprecated APIs:")
    for api in bad_deprecated_apis:
        print(api)

    return


def _validate_documented_public_apis(
    api_in_codes: Dict[str, API], api_in_docs: Set[str], white_list_apis: Set[str]
) -> Tuple[Set[str]]:
    """
    Validate APIs that are public and documented.
    """
    good_apis = set()
    bad_public_apis = set()
    bad_deprecated_apis = set()
    for name, api in api_in_codes.items():
        if not api.is_public() and not api.is_deprecated():
            continue

        if name in white_list_apis:
            continue

        if name not in api_in_docs:
            bad_public_apis.add(name) if api.is_public() else bad_deprecated_apis.add(
                name
            )
        else:
            good_apis.add(name)

    return good_apis, bad_public_apis, bad_deprecated_apis


if __name__ == "__main__":
    main()
