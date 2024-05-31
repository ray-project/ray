import click

from ci.ray_ci.doc.module import Module


@click.command()
@click.argument("module", required=True, type=str)
def main(module: str) -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    module = Module(module)
    for api in module.get_apis():
        print(
            f"API: {api.name}, "
            f"Annotation Type: {api.annotation_type}, "
            f"Code Type: {api.code_type}"
        )


if __name__ == "__main__":
    main()
