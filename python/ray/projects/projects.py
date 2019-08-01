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


def validate_project_schema(project_definition):
    """Validate a project file against the official ray project schema.

    Raises an exception if the project file is not valid.

    Args:
        project_definition (dict): Parsed project yaml.
    """
    dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(dir, "schema.json")) as f:
        schema = json.load(f)

    jsonschema.validate(instance=project_definition, schema=schema)


def load_project(current_dir):
    """Finds .rayproject folder for current project, parse and validates it.

    Args:
        current_dir (str): Path from which to search for .rayproject.

    Returns:
        Dictionary containing the project definition.
    """
    project_root = find_root(current_dir)

    if not project_root:
        raise Exception("No project root found")

    project_file = os.path.join(project_root, ".rayproject", "project.yaml")

    if not os.path.exists(project_file):
        raise Exception("Project file {} not found".format(project_file))

    with open(project_file) as f:
        project_definition = yaml.load(f)

    validate_project_schema(project_definition)

    # Make sure the cluster yaml file exists
    if "cluster" in project_definition:
        cluster_file = os.path.join(project_root,
                                    project_definition["cluster"])
        assert os.path.exists(cluster_file)

    if "environment" in project_definition:
        if "requirements" in project_definition["environment"]:
            requirements_file = os.path.join(
                project_root,
                project_definition["environment"]["requirements"])
            assert os.path.exists(requirements_file)

        if "dockerfile" in project_definition["environment"]:
            docker_file = os.path.join(
                project_root, project_definition["environment"]["dockerfile"])
            assert os.path.exists(docker_file)

    return project_definition
