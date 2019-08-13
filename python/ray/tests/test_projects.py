from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import jsonschema
import os
import pytest
import subprocess
import yaml
from click.testing import CliRunner
from unittest.mock import patch, DEFAULT
from contextlib import contextmanager

from ray.projects.scripts import start
import ray

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


def load_project_description(project_file):
    path = os.path.join(TEST_DIR, "project_files", project_file)
    with open(path) as f:
        return yaml.safe_load(f)


def test_validation_success():
    project_files = [
        "docker_project.yaml", "requirements_project.yaml",
        "shell_project.yaml"
    ]
    for project_file in project_files:
        project_definition = load_project_description(project_file)
        ray.projects.validate_project_schema(project_definition)


def test_validation_failure():
    project_files = ["no_project1.yaml", "no_project2.yaml"]
    for project_file in project_files:
        project_definition = load_project_description(project_file)
        with pytest.raises(jsonschema.exceptions.ValidationError):
            ray.projects.validate_project_schema(project_definition)


def test_check_failure():
    project_files = ["no_project3.yaml"]
    for project_file in project_files:
        project_definition = load_project_description(project_file)
        with pytest.raises(ValueError):
            ray.projects.check_project_definition("", project_definition)


def test_project_root():
    path = os.path.join(TEST_DIR, "project_files", "project1")
    assert ray.projects.find_root(path) == path

    path2 = os.path.join(TEST_DIR, "project_files", "project1", "subdir")
    assert ray.projects.find_root(path2) == path

    path3 = "/tmp/"
    assert ray.projects.find_root(path3) is None


def test_project_validation():
    path = os.path.join(TEST_DIR, "project_files", "project1")
    subprocess.check_call(["ray", "project", "validate"], cwd=path)


def test_project_no_validation():
    path = os.path.join(TEST_DIR, "project_files")
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(["ray", "project", "validate"], cwd=path)


@contextmanager
def _chdir_and_back(d):
    old_dir = os.getcwd()
    try:
        os.chdir(d)
        yield
    finally:
        os.chdir(old_dir)


def test_session_start_default_project():
    test_dir = os.path.join(TEST_DIR,
                            "project_files/session-tests/project-pass")

    with _chdir_and_back(test_dir):
        runner = CliRunner()
        with patch.multiple(
                "ray.projects.scripts",
                create_or_update_cluster=DEFAULT,
                rsync=DEFAULT,
                exec_cluster=DEFAULT,
        ) as mock_calls:
            result = runner.invoke(start, [])
            assert result.exit_code == 0

    create_or_update_cluster_call = mock_calls["create_or_update_cluster"]
    exec_cluster_call = mock_calls["exec_cluster"]

    loaded_project = ray.projects.load_project(test_dir)

    assert create_or_update_cluster_call.call_count == 1
    _, kwargs = create_or_update_cluster_call.call_args
    assert kwargs["config_file"] == loaded_project["cluster"]

    commands_executed = []
    for _, kwargs in exec_cluster_call.call_args_list:
        commands_executed.append(kwargs["cmd"].replace(
            "cd {}; ".format(loaded_project["name"]), ""))

    commands_need_to_be_executed = loaded_project["environment"]["shell"]
    commands_need_to_be_executed += [
        command["command"] for command in loaded_project["commands"]
    ]

    for cmd in commands_need_to_be_executed:
        assert cmd in commands_executed
