import click

from ci.ray_ci.doc.module import Module
from ci.ray_ci.utils import logger

@click.command()
@click.argument("module", required=True, type=str)
def main() -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    module = Module(module)
    for api in module.get_apis():
        logger.info(f"API: {api.name}, Annotation Type: {api.annotation_type}, Code Type: {api.code_type}")
