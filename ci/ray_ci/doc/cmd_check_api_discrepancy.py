import click

from ci.ray_ci.doc.module import Module
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.api import AnnotationType


@click.command()
@click.argument("module", required=True, type=str)
@click.argument("head_rst_file", required=True, type=str)
def main(module: str, head_rst_file: str) -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    module = Module(module)
    api_in_codes = {api.name: api for api in module.get_apis()}

    autodoc = Autodoc(head_rst_file)
    api_in_docs = {api.name: api for api in autodoc.get_apis()}

    # check that all apis in doc are public api
    print("Finding invalid APIs in doc:\n")
    for api_name, api_in_doc in api_in_docs.items():
        api_in_code = api_in_codes.get(api_name)
        if not api_in_code or api_in_code.annotation_type != AnnotationType.PUBLIC_API:
            print(f"\t{api_in_doc}")
    print("\n")

    # check that all public apis are in doc
    print("Finding missing APIs in doc:\n")
    for api_name, api_in_code in api_in_codes.items():
        if (
            api_in_code.annotation_type == AnnotationType.PUBLIC_API
            and api_name not in api_in_docs
        ):
            print(f"\t{api_in_code}")


if __name__ == "__main__":
    main()
