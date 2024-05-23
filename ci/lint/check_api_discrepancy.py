#!/usr/bin/env python

import click
import inspect
from typing import Dict, Set

import ray
from ray.util.annotations import _is_annotated, _get_annotation_type


VISITED = set()
ANNOTATED_CLASSES = {}
ANNOTATED_FUNCTIONS = {}


@click.command()
def main() -> None:
    """
    This script checks for annotated classes and functions in a module, and finds
    discrepancies between the annotations and the documentation.
    """
    walk(ray.data)  # TODO(can): parameterize the module to walk
    _print(ANNOTATED_CLASSES, "classes")
    _print(ANNOTATED_FUNCTIONS, "functions")


def _print(annotated: Dict[str, Set[str]], prefix: str) -> None:
    for type, objs in annotated.items():
        print(f"{prefix} {type}".upper())
        for obj in sorted(objs):
            print(obj)
        print("\n")


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
            _add_if_annotated(attribute, ANNOTATED_CLASSES)
            walk(attribute)
        if inspect.isfunction(attribute) and _is_annotated(attribute):
            _add_if_annotated(attribute, ANNOTATED_FUNCTIONS)
    return


def _add_if_annotated(attribute, result: Dict[str, Set[str]]):
    if not _is_annotated(attribute):
        return
    type = _get_annotation_type(attribute)
    if type not in result:
        result[type] = set()
    result[type].add(_fullname(attribute))


def _fullname(attr):
    return inspect.getmodule(attr).__name__ + "." + attr.__name__


if __name__ == "__main__":
    main()
