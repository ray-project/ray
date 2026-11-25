"""Tests that `ray up` refuses to start Ray on a head node whose Ray version
differs from the Ray version of the `ray up` command."""
import copy
import os
import subprocess
import sys
from typing import List, Optional

import click
import pytest
from packaging.version import Version

import ray
from ray.autoscaler._private import commands
from ray.autoscaler._private.command_runner import (
    is_using_login_shells,
    set_using_login_shells,
)
from ray.autoscaler._private.constants import SKIP_RAY_VERSION_CHECK_ENV
from ray.autoscaler._private.updater import (
    NodeUpdater,
    is_same_minor_version,
    parse_ray_version_output,
)
from ray.tests.autoscaler_test_utils import MockProcessRunner, MockProvider

CLUSTER_CONFIG = {
    "cluster_name": "default",
    "max_workers": 0,
    "provider": {"type": "mock"},
    "auth": {"ssh_user": "ubuntu", "ssh_private_key": os.devnull},
    "available_node_types": {
        "head": {"node_config": {}, "resources": {"CPU": 1}, "max_workers": 0},
    },
    "head_node_type": "head",
    "file_mounts": {},
    "cluster_synced_files": [],
    "initialization_commands": [],
    "setup_commands": [],
    "head_setup_commands": ["head_setup_cmd"],
    "worker_setup_commands": [],
    "head_start_ray_commands": ["start_ray_head"],
    "worker_start_ray_commands": [],
    # This test is about the version check, not token shipping.
    "auth_mode": "disabled",
}


def local_version_with(*, minor: int = 0, patch: int = 0) -> str:
    """Return the local Ray version with its minor and patch versions moved
    up by the given amounts."""
    major, local_minor, local_patch = Version(ray.__version__).release[:3]
    return f"{major}.{local_minor + minor}.{local_patch + patch}"


def ray_up(
    version_output: List[str],
    *,
    config: Optional[dict] = None,
    runner: Optional[MockProcessRunner] = None,
    provider: Optional[MockProvider] = None,
    no_restart: bool = False,
    restart_only: bool = False,
) -> MockProcessRunner:
    """Run `ray up` with the head answering `ray --version` with each of
    `version_output` in turn."""
    if runner is None:
        runner = MockProcessRunner()
    if version_output:
        runner.respond_to_call("ray --version", version_output)
    commands.get_or_create_head_node(
        copy.deepcopy(config or CLUSTER_CONFIG),
        printable_config_file="cluster.yaml",
        no_restart=no_restart,
        restart_only=restart_only,
        yes=True,
        override_cluster_name=None,
        _provider=provider or MockProvider(),
        _runner=runner,
    )
    return runner


def ray_up_fails(version_output: List[str], **kwargs) -> MockProcessRunner:
    """Run `ray up` like `ray_up`, expecting it to fail."""
    runner = kwargs.pop("runner", None) or MockProcessRunner()
    with pytest.raises((SystemExit, click.ClickException)):
        ray_up(version_output, runner=runner, **kwargs)
    return runner


def output_of(capsys) -> str:
    output = capsys.readouterr()
    return output.out + output.err


def command_index(runner: MockProcessRunner, pattern: str) -> int:
    for i, cmd in enumerate(runner.command_history()):
        if pattern in cmd:
            return i
    raise AssertionError(f"No command matches {pattern!r}")


def test_head_with_same_ray_version_starts_after_check():
    runner = ray_up([f"ray, version {ray.__version__}"])

    # The check runs after the setup commands, which can install Ray, and
    # before the start commands.
    setup = command_index(runner, "head_setup_cmd")
    check = command_index(runner, "ray --version")
    start = command_index(runner, "start_ray_head")
    assert setup < check < start


@pytest.mark.parametrize(
    "node_version", ["2.0.0", local_version_with(minor=1), "custom-build"]
)
def test_head_with_other_minor_ray_version_is_not_started(node_version, capsys):
    runner = ray_up_fails([f"ray, version {node_version}"])

    runner.assert_not_has_call("1.2.3.4", pattern="start_ray_head")
    assert "Ray version mismatch" in output_of(capsys)


def test_mismatch_error_names_both_versions_and_opt_out(capsys):
    ray_up_fails(["ray, version 2.0.0"])

    combined = output_of(capsys)
    assert "Ray version mismatch" in combined
    assert ray.__version__ in combined
    assert "2.0.0" in combined
    assert f"{SKIP_RAY_VERSION_CHECK_ENV}=1" in combined
    assert "ray down" not in combined


def test_mismatch_error_tells_docker_users_to_replace_the_container(capsys):
    runner = MockProcessRunner()
    # A container from an earlier `ray up` is running, and `ray up` keeps it
    # though its image differs from the YAML's.
    runner.respond_to_call("json .Mounts", ["[]"])
    runner.respond_to_call("json .Config.Env", ["[]"])
    runner.respond_to_call(".State.Running", ["true"] * 10)
    docker_config = {
        **CLUSTER_CONFIG,
        "docker": {"image": "example", "container_name": "mock"},
    }

    ray_up_fails(["ray, version 2.0.0"], config=docker_config, runner=runner)

    runner.assert_not_has_call("1.2.3.4", pattern="docker stop")
    combined = output_of(capsys)
    assert "Ray version mismatch" in combined
    assert "run `ray down` and then `ray up`" in combined


# Lines that a login shell or a Python environment can print before the
# version.
BANNER = "Welcome to the node!\nWARNING: Your pip is out of date.\n"


def test_noisy_output_with_same_ray_version_starts_head():
    runner = ray_up([f"{BANNER}ray, version {ray.__version__}\n"])

    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")


def test_noisy_output_with_other_minor_ray_version_is_not_started(capsys):
    runner = ray_up_fails([f"{BANNER}ray, version 2.0.0\n"])

    runner.assert_not_has_call("1.2.3.4", pattern="start_ray_head")
    assert "Ray version mismatch" in output_of(capsys)


def test_no_restart_still_checks_ray_version(capsys):
    provider = MockProvider()
    ray_up([f"ray, version {ray.__version__}"], provider=provider)

    # `--no-restart` runs no start commands, but `ray up` still refuses to
    # update a head that runs another Ray version.
    runner = ray_up_fails(["ray, version 2.0.0"], provider=provider, no_restart=True)

    runner.assert_has_call("1.2.3.4", pattern="ray --version")
    assert "Ray version mismatch" in output_of(capsys)


def test_no_restart_with_same_ray_version_starts_nothing():
    provider = MockProvider()
    ray_up([f"ray, version {ray.__version__}"], provider=provider)

    runner = ray_up(
        [f"ray, version {ray.__version__}"], provider=provider, no_restart=True
    )

    runner.assert_has_call("1.2.3.4", pattern="ray --version")
    runner.assert_not_has_call("1.2.3.4", pattern="start_ray_head")


def test_restart_only_checks_ray_version_before_restarting(capsys):
    provider = MockProvider()
    ray_up([f"ray, version {ray.__version__}"], provider=provider)

    runner = ray_up_fails(["ray, version 2.0.0"], provider=provider, restart_only=True)

    runner.assert_not_has_call("1.2.3.4", pattern="head_setup_cmd")
    runner.assert_has_call("1.2.3.4", pattern="ray --version")
    runner.assert_not_has_call("1.2.3.4", pattern="start_ray_head")
    assert "Ray version mismatch" in output_of(capsys)


def test_head_with_other_patch_ray_version_starts_with_warning(capsys):
    node_version = local_version_with(patch=1)

    runner = ray_up([f"ray, version {node_version}"])

    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")
    output = capsys.readouterr()
    assert node_version in output.out + output.err


def test_opt_out_starts_head_with_other_ray_version(monkeypatch):
    monkeypatch.setenv(SKIP_RAY_VERSION_CHECK_ENV, "1")

    runner = ray_up(["ray, version 2.0.0"])

    runner.assert_not_has_call("1.2.3.4", pattern="ray --version")
    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")


@pytest.mark.parametrize(
    "version,other,expected",
    [
        ("2.61.0", "2.61.1", True),
        ("2", "2.0", True),
        ("3.0.0.dev0", "3.0.0", True),
        ("2.61.0", "2.62.0", False),
        ("2.61.0", "3.0.0.dev0", False),
        ("2.61.0", "custom-build", False),
    ],
)
def test_is_same_minor_version(version, other, expected):
    assert is_same_minor_version(version, other) is expected


def test_version_check_handles_str_output_and_called_process_error(capsys):
    class StrCommandRunner:
        def __init__(self, result):
            self._result = result

        def run(self, *args, **kwargs):
            if isinstance(self._result, Exception):
                raise self._result
            return self._result

    def make_updater(result):
        updater = object.__new__(NodeUpdater)
        updater.expected_ray_version = ray.__version__
        updater.docker_config = None
        updater.cmd_runner = StrCommandRunner(result)
        return updater

    make_updater(f"ray, version {ray.__version__}\n")._check_ray_version()

    with pytest.raises(click.ClickException, match="Ray version mismatch"):
        make_updater("ray, version 2.0.0\n")._check_ray_version()

    make_updater(
        subprocess.CalledProcessError(1, ["ray", "--version"])
    )._check_ray_version()
    assert "Couldn't determine the Ray version" in output_of(capsys)


def test_head_whose_ray_version_is_unknown_still_starts():
    runner = ray_up(["no version here"])

    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")


@pytest.mark.parametrize("use_login_shells", [True, False])
def test_head_where_ray_version_fails_still_starts(use_login_shells, capsys):
    # For example, `ray` isn't on the PATH of the check's shell. A failed
    # command raises ClickException with login shells and ProcessRunnerError
    # without them.
    was_using_login_shells = is_using_login_shells()
    set_using_login_shells(use_login_shells)
    try:
        runner = ray_up([], runner=MockProcessRunner(fail_cmds=["ray --version"]))
    finally:
        set_using_login_shells(was_using_login_shells)

    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")
    assert "Couldn't determine the Ray version" in output_of(capsys)


def test_check_reads_the_real_ray_version_output():
    # The tests above fake the node's output. If `ray --version` changed its
    # format, the check would only warn and stop catching mismatches.
    output = subprocess.check_output(["ray", "--version"], text=True)

    assert parse_ray_version_output(output) == ray.__version__


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
