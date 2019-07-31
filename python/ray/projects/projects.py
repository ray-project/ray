import json
import jsonschema
import os
import yaml


def find_root(dir):
    """Find root directory of the ray project.

    Args:
        dir (str): Directory to start the search in.

    Returns:
        Path of the parent directory containing the .rayproject or
        None if no such project is found.
    """
    prev, dir = None, os.path.abspath(dir)
    while prev != dir:
        if os.path.isdir(os.path.join(dir, ".rayproject")):
            return dir
        prev, dir = dir, os.path.abspath(os.path.join(dir, os.pardir))
    return None

def validate_project(project_file):
    """Validate a project file against the official ray project schema.

    Raises an exception if the project file is not valid.

    Args:
        Path to the project .yaml file.
    """
    dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(dir, "schema.json")) as f:
        schema = json.load(f)
    with open(project_file) as f:
        instance = yaml.load(f)
    jsonschema.validate(instance=instance, schema=schema)
