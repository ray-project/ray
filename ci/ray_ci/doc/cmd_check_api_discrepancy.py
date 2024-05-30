import click
from typing import List

from ci.ray_ci.doc.module import Module
from ci.ray_ci.doc.autodoc import Autodoc
from ci.ray_ci.doc.api import AnnotationType


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
    api_in_docs = {api.name: api for api in autodoc.get_apis()}

    # check that all apis in doc are public api
    print("Finding invalid APIs in doc:\n")
    for api_name, api_in_doc in api_in_docs.items():
        api_in_code = api_in_codes.get(api_name)
        api_class_in_code = api_in_codes.get(api_name.rsplit(".", 1)[0])
        annotation_type = (
            "Unknown" if not api_in_code else api_in_code.annotation_type.value
        )
        if api_in_code and not api_in_code.annotation_type == AnnotationType.PUBLIC_API:
            print(f"\t{api_name}\t{annotation_type}")
        elif not api_in_code:
            if (
                not api_class_in_code
                or not api_class_in_code.annotation_type == AnnotationType.PUBLIC_API
            ):
                print(f"\t{api_name}\t{annotation_type}")
    print("\n")

    # check that all public apis are in doc
    print("Finding missing APIs in doc:\n")
    for api_name, api_in_code in api_in_codes.items():
        if (
            api_in_code.annotation_type == AnnotationType.PUBLIC_API
            and api_name not in api_in_docs
        ):
            annotation_type = (
                "Unknown" if not api_in_code else api_in_code.annotation_type.value
            )
            print(f"\t{api_name}\t{annotation_type}")
    print("\n")

    # consistent apis between code and doc
    print("Consistent APIs between code and doc:\n")
    for api_name, api_in_code in api_in_codes.items():
        api_in_doc = api_in_docs.get(api_name)
        if api_in_doc and api_in_code.annotation_type == AnnotationType.PUBLIC_API:
            annotation_type = (
                "Unknown" if not api_in_code else api_in_code.annotation_type.value
            )
            print(f"\t{api_name}\t{annotation_type}")
    print("\n")


if __name__ == "__main__":
    main()
