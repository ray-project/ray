#!/usr/bin/env python

import click
import inspect

import ray
from ray.util.annotations import _is_annotated


VISITED = set()
ANNOTATED_CLASSES = set()
ANNOTATED_FUNCTIONS = set()


@click.command()
def main() -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    walk(ray.data)  # TODO(can): parameterize the module to walk

    print("ANNOTATED CLASSES")
    for classes in sorted(ANNOTATED_CLASSES):
        print(classes)
    print("\n")
    print("ANNOTATED FUNCTIONS")
    for functions in sorted(ANNOTATED_FUNCTIONS):
        print(functions)


def walk(module) -> None:
    """
    Depth-first search through the module and its children to find annotated classes
    and functions.
    """
    if module in VISITED:
        return
    VISITED.add(module)
    if not _fullname(module).startswith("ray.data"):
        return

    for child in dir(module):
        attribute = getattr(module, child)

        if inspect.ismodule(attribute):
            walk(attribute)
        if inspect.isclass(attribute):
            if _is_annotated(attribute):
                ANNOTATED_CLASSES.add(_fullname(attribute))
            walk(attribute)
        if inspect.isfunction(attribute) and _is_annotated(attribute):
            ANNOTATED_FUNCTIONS.add(_fullname(attribute))
    return


def _fullname(attr):
    return inspect.getmodule(attr).__name__ + "." + attr.__name__


if __name__ == "__main__":
    main()
