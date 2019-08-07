import json
import jsonschema
import os
import yaml


def find_root(directory):
    """Find root directory of the ray project.

    Args:
        directory (str): Directory to start the search in.

    Returns:
        Path of the parent directory containing the .rayproject or
        None if no such project is found.
    """
    prev, directory = None, os.path.abspath(directory)
    while prev != directory:
        if os.path.isdir(os.path.join(directory, ".rayproject")):
            return directory
        prev, directory = directory, os.path.abspath(
            os.path.join(directory, os.pardir))
    return None


def validate_project_schema(project_definition):
    """Validate a project file against the official ray project schema.

    Args:
        project_definition (dict): Parsed project yaml.

    Raises:
        jsonschema.exceptions.ValidationError: This exception is raised
            if the project file is not valid.
    """
    dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(dir, "schema.json")) as f:
        schema = json.load(f)

    jsonschema.validate(instance=project_definition, schema=schema)


def check_project_definition(project_root, project_definition):
    """Checks if the project definition is valid.

    Args:
        project_root (str): Path containing the .rayproject
        project_definition (dict): Project definition

    Raises:
        jsonschema.exceptions.ValidationError: This exception is raised
            if the project file is not valid.
        ValueError: This exception is raised if there are other errors in
            the project definition (e.g. files not existing).
    """

    validate_project_schema(project_definition)

    # Make sure the cluster yaml file exists
    if "cluster" in project_definition:
        cluster_file = os.path.join(project_root,
                                    project_definition["cluster"])
        if not os.path.exists(cluster_file):
            raise ValueError("'cluster' file does not exist "
                             "in {}".format(project_root))

    if "environment" in project_definition:
        env = project_definition["environment"]

        if sum(["dockerfile" in env, "dockerimage" in env]) > 1:
            raise ValueError("Cannot specify both 'dockerfile' and "
                             "'dockerimage' in environment.")

        if "requirements" in env:
            requirements_file = os.path.join(project_root, env["requirements"])
            if not os.path.exists(requirements_file):
                raise ValueError("'requirements' file in 'environment' does "
                                 "not exist in {}".format(project_root))

        if "dockerfile" in env:
            docker_file = os.path.join(project_root, env["dockerfile"])
            if not os.path.exists(docker_file):
                raise ValueError("'dockerfile' file in 'environment' does "
                                 "not exist in {}".format(project_root))


def load_project(current_dir):
    """Finds .rayproject folder for current project, parse and validates it.

    Args:
        current_dir (str): Path from which to search for .rayproject.

    Returns:
        Dictionary containing the project definition.

    Raises:
        jsonschema.exceptions.ValidationError: This exception is raised
            if the project file is not valid.
        ValueError: This exception is raised if there are other errors in
            the project definition (e.g. files not existing).
    """
    project_root = find_root(current_dir)

    if not project_root:
        raise ValueError("No project root found")

    project_file = os.path.join(project_root, ".rayproject", "project.yaml")

    if not os.path.exists(project_file):
        raise ValueError("Project file {} not found".format(project_file))

    with open(project_file) as f:
        project_definition = yaml.load(f)

    check_project_definition(project_root, project_definition)

    return project_definition
