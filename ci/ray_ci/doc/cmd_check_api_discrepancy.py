import click
from typing import List, Dict, Set

from ci.ray_ci.doc.module import Module
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.api import AnnotationType, API


@click.command()
@click.argument("head_modules", required=True, type=str, nargs=-1)
@click.argument("head_rst_file", required=True, type=str, nargs=1)
def main(head_modules: List[str], head_rst_file: str) -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    api_in_codes = {}
    for module in head_modules:
        module = Module(module)
        api_in_codes.update({api.name: api for api in module.get_apis()})

    autodoc = Autodoc(head_rst_file)
    api_in_docs = {api.name for api in autodoc.get_apis()}

    # All public APIs should be documented
    print("Validating public APIs...")
    _validate_public_apis(api_in_codes, api_in_docs)

    # All deprecated APIs should be documented
    print("Validating deprecated APIs...")
    _validate_deprecated_apis(api_in_codes, api_in_docs)

    # APIs that are consistently documented
    print("Consistently documented APIs...")
    _validate_consistent_apis(api_in_codes, api_in_docs)

    return


def _validate_consistent_apis(
    api_in_codes: Dict[str, API], api_in_docs: Set[str]
) -> None:
    """
    Validate APIs that are public and documented.
    """
    for api in api_in_codes.values():
        if (
            api.annotation_type == AnnotationType.PUBLIC_API
            or api.annotation_type == AnnotationType.DEPRECATED
        ) and api.name in api_in_docs:
            print(f"\t{api.name}")


def _validate_public_apis(api_in_codes: Dict[str, API], api_in_docs: Set[str]) -> None:
    """
    Validate that all public APIs in the code are documented.
    """
    for api in api_in_codes.values():
        if (
            api.annotation_type == AnnotationType.PUBLIC_API
            and api.name not in api_in_docs
        ):
            print(f"\t{api.name}")


def _validate_deprecated_apis(
    api_in_codes: Dict[str, API], api_in_docs: Set[str]
) -> None:
    """
    Validate that all deprecated APIs in the code are documented.
    """
    for api in api_in_codes.values():
        if (
            api.annotation_type == AnnotationType.DEPRECATED
            and api.name not in api_in_docs
        ):
            print(f"\t{api.name}")


if __name__ == "__main__":
    main()
