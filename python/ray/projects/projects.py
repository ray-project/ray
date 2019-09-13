from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import jsonschema
import os
import yaml


class ProjectDefinition:
    def __init__(self, current_dir):
        """Finds .rayproject folder for current project, parse and validates it.

        Args:
            current_dir (str): Path from which to search for .rayproject.

        Raises:
            jsonschema.exceptions.ValidationError: This exception is raised
                if the project file is not valid.
            ValueError: This exception is raised if there are other errors in
                the project definition (e.g. files not existing).
        """
        root = find_root(current_dir)
        if root is None:
            raise ValueError("No project root found")
        # Add an empty pathname to the end so that rsync will copy the project
        # directory to the correct target.
        self.root = os.path.join(root, "")

        # Parse the project YAML.
        project_file = os.path.join(self.root, ".rayproject", "project.yaml")
        if not os.path.exists(project_file):
            raise ValueError("Project file {} not found".format(project_file))
        with open(project_file) as f:
            self.config = yaml.safe_load(f)

        check_project_config(self.root, self.config)

    def cluster_yaml(self):
        """Return the project's cluster configuration filename."""
        return self.config["cluster"]

    def working_directory(self):
        """Return the project's working directory on a cluster session."""
        # Add an empty pathname to the end so that rsync will copy the project
        # directory to the correct target.
        directory = os.path.join("~", self.config["name"], "")
        return directory

    def get_command_to_run(self, command=None, args=tuple()):
        """Get and format a command to run.

        Args:
            command (str): Name of the command to run. The command definition
                should be available in project.yaml.
            args (tuple): Tuple containing arguments to format the command
                with.
        Returns:
            The raw shell command to run, formatted with the given arguments.

        Raises:
            ValueError: This exception is raised if the given command is not
                found in project.yaml.
        """
        command_to_run = None
        params = None

        if command is None:
            command = "default"
        for command_definition in self.config["commands"]:
            if command_definition["name"] == command:
                command_to_run = command_definition["command"]
                params = command_definition.get("params", [])
        if not command_to_run:
            raise ValueError(
                "Cannot find the command '{}' in commmands section of the "
                "project file.".format(command))

        # Build argument parser dynamically to parse parameter arguments.
        parser = argparse.ArgumentParser(prog=command)
        for param in params:
            parser.add_argument(
                "--" + param["name"],
                required=True,
                help=param.get("help"),
                choices=param.get("choices"))

        result = parser.parse_args(list(args))
        for key, val in result.__dict__.items():
            command_to_run = command_to_run.replace("{{" + key + "}}", val)

        return command_to_run

    def git_repo(self):
        return self.config.get("repo", None)


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


def validate_project_schema(project_config):
    """Validate a project config against the official ray project schema.

    Args:
        project_config (dict): Parsed project yaml.

    Raises:
        jsonschema.exceptions.ValidationError: This exception is raised
            if the project file is not valid.
    """
    dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(dir, "schema.json")) as f:
        schema = json.load(f)

    jsonschema.validate(instance=project_config, schema=schema)


def check_project_config(project_root, project_config):
    """Checks if the project definition is valid.

    Args:
        project_root (str): Path containing the .rayproject
        project_config (dict): Project config definition

    Raises:
        jsonschema.exceptions.ValidationError: This exception is raised
            if the project file is not valid.
        ValueError: This exception is raised if there are other errors in
            the project definition (e.g. files not existing).
    """
    validate_project_schema(project_config)

    # Make sure the cluster yaml file exists
    if "cluster" in project_config:
        cluster_file = os.path.join(project_root, project_config["cluster"])
        if not os.path.exists(cluster_file):
            raise ValueError("'cluster' file does not exist "
                             "in {}".format(project_root))

    if "environment" in project_config:
        env = project_config["environment"]

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
