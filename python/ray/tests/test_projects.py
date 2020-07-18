import jsonschema
import os
import pytest
import subprocess
import yaml
from click.testing import CliRunner
import sys
from unittest.mock import patch, DEFAULT

from contextlib import contextmanager

from ray.projects.scripts import (session_start, session_commands,
                                  session_execute)
from ray.test_utils import check_call_ray
import ray

TEST_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "project_files")


def load_project_description(project_file):
    path = os.path.join(TEST_DIR, project_file)
    with open(path) as f:
        return yaml.safe_load(f)


def test_validation():
    project_dirs = ["docker_project", "requirements_project", "shell_project"]
    for project_dir in project_dirs:
        project_dir = os.path.join(TEST_DIR, project_dir)
        ray.projects.ProjectDefinition(project_dir)

    bad_schema_dirs = ["no_project1"]
    for project_dir in bad_schema_dirs:
        project_dir = os.path.join(TEST_DIR, project_dir)
        with pytest.raises(jsonschema.exceptions.ValidationError):
            ray.projects.ProjectDefinition(project_dir)

    bad_project_dirs = ["no_project2", "noproject3"]
    for project_dir in bad_project_dirs:
        project_dir = os.path.join(TEST_DIR, project_dir)
        with pytest.raises(ValueError):
            ray.projects.ProjectDefinition(project_dir)


def test_project_root():
    path = os.path.join(TEST_DIR, "project1")
    project_definition = ray.projects.ProjectDefinition(path)
    assert os.path.normpath(project_definition.root) == os.path.normpath(path)

    path2 = os.path.join(TEST_DIR, "project1", "subdir")
    project_definition = ray.projects.ProjectDefinition(path2)
    assert os.path.normpath(project_definition.root) == os.path.normpath(path)

    path3 = ray.utils.get_user_temp_dir() + os.sep
    with pytest.raises(ValueError):
        project_definition = ray.projects.ProjectDefinition(path3)


def test_project_validation():
    with _chdir_and_back(os.path.join(TEST_DIR, "project1")):
        check_call_ray(["project", "validate"])


def test_project_no_validation():
    with _chdir_and_back(TEST_DIR):
        with pytest.raises(subprocess.CalledProcessError):
            check_call_ray(["project", "validate"])


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
    test_dir = os.path.join(TEST_DIR, project_dir)
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
        os.path.join("session-tests", "project-pass"), session_start,
        ["default"])

    loaded_project = ray.projects.ProjectDefinition(test_dir)
    assert result.exit_code == 0

    # Part 1/3: Cluster Launching Call
    create_or_update_cluster_call = mock_calls["create_or_update_cluster"]
    assert create_or_update_cluster_call.call_count == 1
    _, kwargs = create_or_update_cluster_call.call_args
    assert kwargs["config_file"] == loaded_project.cluster_yaml()

    # Part 2/3: Rsync Calls
    rsync_call = mock_calls["rsync"]
    # 1 for rsyncing the project directory, 1 for rsyncing the
    # requirements.txt.
    assert rsync_call.call_count == 2
    _, kwargs = rsync_call.call_args
    assert kwargs["source"] == loaded_project.config["environment"][
        "requirements"]

    # Part 3/3: Exec Calls
    exec_cluster_call = mock_calls["exec_cluster"]
    commands_executed = []
    for _, kwargs in exec_cluster_call.call_args_list:
        commands_executed.append(kwargs["cmd"].replace(
            "cd {}; ".format(loaded_project.working_directory()), ""))

    expected_commands = loaded_project.config["environment"]["shell"]
    expected_commands += [
        command["command"] for command in loaded_project.config["commands"]
    ]

    if "requirements" in loaded_project.config["environment"]:
        assert any("pip install -r" for cmd in commands_executed)
        # pop the `pip install` off commands executed
        commands_executed = [
            cmd for cmd in commands_executed if "pip install -r" not in cmd
        ]

    assert expected_commands == commands_executed


def test_session_execute_default_project():
    result, mock_calls, test_dir = run_test_project(
        os.path.join("session-tests", "project-pass"), session_execute,
        ["default"])

    loaded_project = ray.projects.ProjectDefinition(test_dir)
    assert result.exit_code == 0

    assert mock_calls["rsync"].call_count == 0
    assert mock_calls["create_or_update_cluster"].call_count == 0

    exec_cluster_call = mock_calls["exec_cluster"]
    commands_executed = []
    for _, kwargs in exec_cluster_call.call_args_list:
        commands_executed.append(kwargs["cmd"].replace(
            "cd {}; ".format(loaded_project.working_directory()), ""))

    expected_commands = [
        command["command"] for command in loaded_project.config["commands"]
    ]

    assert expected_commands == commands_executed

    result, mock_calls, test_dir = run_test_project(
        os.path.join("session-tests", "project-pass"), session_execute,
        ["--shell", "uptime"])
    assert result.exit_code == 0


def test_session_start_docker_fail():
    result, _, _ = run_test_project(
        os.path.join("session-tests", "with-docker-fail"), session_start, [])

    assert result.exit_code == 1
    assert ("Docker support in session is currently "
            "not implemented") in result.output


def test_session_invalid_config_errored():
    result, _, _ = run_test_project(
        os.path.join("session-tests", "invalid-config-fail"), session_start,
        [])

    assert result.exit_code == 1
    assert "validation failed" in result.output
    # check that we are displaying actional error message
    assert "ray project validate" in result.output


def test_session_create_command():
    result, mock_calls, test_dir = run_test_project(
        os.path.join("session-tests", "commands-test"), session_start,
        ["first", "--a", "1", "--b", "2"])

    # Verify the project can be loaded.
    ray.projects.ProjectDefinition(test_dir)
    assert result.exit_code == 0

    exec_cluster_call = mock_calls["exec_cluster"]
    found_command = False
    for _, kwargs in exec_cluster_call.call_args_list:
        if "Starting ray job with 1 and 2" in kwargs["cmd"]:
            found_command = True
    assert found_command


def test_session_create_multiple():
    for args in [{"a": "*", "b": "2"}, {"a": "1", "b": "*"}]:
        result, mock_calls, test_dir = run_test_project(
            os.path.join("session-tests", "commands-test"), session_start,
            ["first", "--a", args["a"], "--b", args["b"]])

        loaded_project = ray.projects.ProjectDefinition(test_dir)
        assert result.exit_code == 0

        exec_cluster_call = mock_calls["exec_cluster"]
        commands_executed = []
        for _, kwargs in exec_cluster_call.call_args_list:
            commands_executed.append(kwargs["cmd"].replace(
                "cd {}; ".format(loaded_project.working_directory()), ""))
        assert commands_executed.count("echo \"Setting up\"") == 2
        if args["a"] == "*":
            assert commands_executed.count(
                "echo \"Starting ray job with 1 and 2\"") == 1
            assert commands_executed.count(
                "echo \"Starting ray job with 2 and 2\"") == 1
        if args["b"] == "*":
            assert commands_executed.count(
                "echo \"Starting ray job with 1 and 1\"") == 1
            assert commands_executed.count(
                "echo \"Starting ray job with 1 and 2\"") == 1

    # Using multiple wildcards shouldn't work
    result, mock_calls, test_dir = run_test_project(
        os.path.join("session-tests", "commands-test"), session_start,
        ["first", "--a", "*", "--b", "*"])
    assert result.exit_code == 1


def test_session_commands():
    result, mock_calls, test_dir = run_test_project(
        os.path.join("session-tests", "commands-test"), session_commands, [])

    assert "This is the first parameter" in result.output
    assert "This is the second parameter" in result.output

    assert 'Command "first"' in result.output
    assert 'Command "second"' in result.output


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
