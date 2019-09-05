from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import jsonschema
import os
import pytest
import subprocess
import yaml
from click.testing import CliRunner
import sys

from contextlib import contextmanager

from ray.projects.scripts import start
import ray

if sys.version_info >= (3, 3):
    from unittest.mock import patch, DEFAULT
else:
    from mock import patch, DEFAULT

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


def run_test_project(project_dir, command, args):
    # Run the CLI commands with patching
    test_dir = os.path.join(TEST_DIR, "project_files", project_dir)
    with _chdir_and_back(test_dir):
        runner = CliRunner()
        with patch.multiple(
                "ray.projects.scripts",
                create_or_update_cluster=DEFAULT,
                rsync=DEFAULT,
                exec_cluster=DEFAULT,
        ) as mock_calls:
            result = runner.invoke(command, args)

    return result, mock_calls, test_dir


def test_session_start_default_project():
    result, mock_calls, test_dir = run_test_project(
        "session-tests/project-pass", start, [])

    loaded_project = ray.projects.load_project(test_dir)
    assert result.exit_code == 0

    # Part 1/3: Cluster Launching Call
    create_or_update_cluster_call = mock_calls["create_or_update_cluster"]
    assert create_or_update_cluster_call.call_count == 1
    _, kwargs = create_or_update_cluster_call.call_args
    assert kwargs["config_file"] == loaded_project["cluster"]

    # Part 2/3: Rsync Calls
    rsync_call = mock_calls["rsync"]
    # 1 for rsyncing the project directory, 1 for rsyncing the
    # requirements.txt.
    assert rsync_call.call_count == 2
    _, kwargs = rsync_call.call_args
    assert kwargs["source"] == loaded_project["environment"]["requirements"]

    # Part 3/3: Exec Calls
    exec_cluster_call = mock_calls["exec_cluster"]
    commands_executed = []
    for _, kwargs in exec_cluster_call.call_args_list:
        commands_executed.append(kwargs["cmd"].replace(
            "cd {}; ".format(loaded_project["name"]), ""))

    expected_commands = loaded_project["environment"]["shell"]
    expected_commands += [
        command["command"] for command in loaded_project["commands"]
    ]

    if "requirements" in loaded_project["environment"]:
        assert any("pip install -r" for cmd in commands_executed)
        # pop the `pip install` off commands executed
        commands_executed = [
            cmd for cmd in commands_executed if "pip install -r" not in cmd
        ]

    assert expected_commands == commands_executed


def test_session_start_docker_fail():
    result, _, _ = run_test_project("session-tests/with-docker-fail", start,
                                    [])

    assert result.exit_code == 1
    assert ("Docker support in session is currently "
            "not implemented") in result.output


def test_session_invalid_config_errored():
    result, _, _ = run_test_project("session-tests/invalid-config-fail", start,
                                    [])

    assert result.exit_code == 1
    assert "validation failed" in result.output
    # check that we are displaying actional error message
    assert "ray project validate" in result.output


def test_session_create_command():
    result, mock_calls, test_dir = run_test_project(
        "session-tests/commands-test", start,
        ["first", "--a", "1", "--b", "2"])

    # Verify the project can be loaded.
    ray.projects.load_project(test_dir)
    assert result.exit_code == 0

    exec_cluster_call = mock_calls["exec_cluster"]
    found_command = False
    for _, kwargs in exec_cluster_call.call_args_list:
        if "Starting ray job with 1 and 2" in kwargs["cmd"]:
            found_command = True
    assert found_command
