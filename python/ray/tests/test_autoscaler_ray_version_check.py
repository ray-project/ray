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
from ray.autoscaler._private.constants import SKIP_RAY_VERSION_CHECK_ENV
from ray.autoscaler._private.updater import parse_ray_version_output
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

# Lines that a login shell or a Python environment can print first.
BANNER = "Welcome to the node!\nWARNING: Your pip is out of date.\n"


def local_version_with(*, minor: int = 0, patch: int = 0) -> str:
    """Return the local Ray version with its minor and patch moved up."""
    major, local_minor, local_patch = Version(ray.__version__).release[:3]
    return f"{major}.{local_minor + minor}.{local_patch + patch}"


def ray_up(
    version_output: List[str],
    *,
    config: Optional[dict] = None,
    runner: Optional[MockProcessRunner] = None,
    provider: Optional[MockProvider] = None,
    fails: bool = False,
    **kwargs,
) -> MockProcessRunner:
    """Run `ray up` with the head answering `ray --version` with each of
    `version_output` in turn, expecting it to fail if `fails`."""
    runner = runner or MockProcessRunner()
    if version_output:
        runner.respond_to_call("ray --version", version_output)
    kwargs = {"no_restart": False, "restart_only": False, **kwargs}
    try:
        commands.get_or_create_head_node(
            copy.deepcopy(config or CLUSTER_CONFIG),
            printable_config_file="cluster.yaml",
            yes=True,
            override_cluster_name=None,
            _provider=provider or MockProvider(),
            _runner=runner,
            **kwargs,
        )
    except (SystemExit, click.ClickException):
        assert fails, "`ray up` failed"
        return runner
    assert not fails, "`ray up` succeeded"
    return runner


def test_head_with_same_ray_version_starts_after_check():
    runner = ray_up([f"{BANNER}ray, version {ray.__version__}\n"])

    # The check runs after the setup commands, which can install Ray, and
    # before the start commands.
    history = runner.command_history()
    [setup, check, start] = [
        next(i for i, cmd in enumerate(history) if pattern in cmd)
        for pattern in ["head_setup_cmd", "ray --version", "start_ray_head"]
    ]
    assert setup < check < start


@pytest.mark.parametrize(
    "output",
    [
        "ray, version 2.0.0",
        f"ray, version {local_version_with(minor=1)}",
        "ray, version custom-build",
        f"{BANNER}ray, version 2.0.0\n",
    ],
)
def test_head_with_other_minor_ray_version_is_not_started(output, capsys):
    runner = ray_up([output], fails=True)

    runner.assert_not_has_call("1.2.3.4", pattern="start_ray_head")
    out = capsys.readouterr()
    error = out.out + out.err
    assert "Ray version mismatch" in error
    assert ray.__version__ in error
    assert f"{SKIP_RAY_VERSION_CHECK_ENV}=1" in error
    assert "ray down" not in error


@pytest.mark.parametrize(
    "output,warning",
    [
        (f"ray, version {local_version_with(patch=1)}", local_version_with(patch=1)),
        ("no version here", "Couldn't determine the Ray version"),
        # `ray --version` fails, for example as `ray` isn't on its PATH.
        (None, "Couldn't determine the Ray version"),
    ],
)
def test_head_with_other_patch_or_unknown_version_starts(output, warning, capsys):
    if output is None:
        runner = ray_up([], runner=MockProcessRunner(fail_cmds=["ray --version"]))
    else:
        runner = ray_up([output])

    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")
    out = capsys.readouterr()
    assert warning in out.out + out.err


def test_mismatch_error_tells_docker_users_to_replace_the_container(capsys):
    runner = MockProcessRunner()
    # A container from an earlier `ray up` is running, and `ray up` keeps it
    # though its image differs from the YAML's.
    runner.respond_to_call("json .Mounts", ["[]"])
    runner.respond_to_call("json .Config.Env", ["[]"])
    runner.respond_to_call(".State.Running", ["true"] * 10)
    config = {
        **CLUSTER_CONFIG,
        "docker": {"image": "example", "container_name": "mock"},
    }

    ray_up(["ray, version 2.0.0"], config=config, runner=runner, fails=True)

    # The check reads the container's Ray, not the host's.
    runner.assert_has_call("1.2.3.4", pattern=r"docker exec\b.*\bmock\b.*ray --version")
    runner.assert_not_has_call("1.2.3.4", pattern="docker stop")
    out = capsys.readouterr()
    assert "run `ray down` and then `ray up`" in out.out + out.err


@pytest.mark.parametrize("flag", ["no_restart", "restart_only"])
def test_updating_existing_head_checks_ray_version(flag):
    provider = MockProvider()
    ray_up([f"ray, version {ray.__version__}"], provider=provider)

    runner = ray_up(
        ["ray, version 2.0.0"], provider=provider, fails=True, **{flag: True}
    )

    runner.assert_has_call("1.2.3.4", pattern="ray --version")
    runner.assert_not_has_call("1.2.3.4", pattern="start_ray_head")


def test_opt_out_starts_head_with_other_ray_version(monkeypatch):
    monkeypatch.setenv(SKIP_RAY_VERSION_CHECK_ENV, "1")

    runner = ray_up(["ray, version 2.0.0"])

    runner.assert_not_has_call("1.2.3.4", pattern="ray --version")
    runner.assert_has_call("1.2.3.4", pattern="start_ray_head")


def test_check_reads_the_real_ray_version_output():
    # The tests above fake the node's output. If `ray --version` changed its
    # format, the check would only warn and stop catching mismatches.
    output = subprocess.check_output(["ray", "--version"], text=True)

    assert parse_ray_version_output(output) == ray.__version__


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
