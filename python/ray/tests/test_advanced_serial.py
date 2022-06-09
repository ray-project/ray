import sys
import os

import pytest
import ray

from moto import mock_ec2, mock_iam
from testfixtures.popen import PopenBehaviour

from click.testing import CliRunner
import subprocess
import ray.scripts.scripts as scripts
from ray.tests.test_cli import (
    _setup_popen_mock,
    DEFAULT_TEST_CONFIG_PATH,
    _check_output_via_pattern,
)


def test_ray_start_and_stop():
    for i in range(10):
        subprocess.check_call(["ray", "start", "--head"])
        subprocess.check_call(["ray", "stop"])


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 0,
            "num_nodes": 1,
            "do_init": False,
        }
    ],
    indirect=True,
)
def test_ray_address_environment_variable(ray_start_cluster):
    address = ray_start_cluster.address
    # In this test we use zero CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.

    # Make sure we connect to an existing cluster if
    # RAY_ADDRESS is set to the cluster address.
    os.environ["RAY_ADDRESS"] = address
    ray.init()
    assert "CPU" not in ray.state.cluster_resources()
    ray.shutdown()
    del os.environ["RAY_ADDRESS"]

    # Make sure we connect to an existing cluster if
    # RAY_ADDRESS is set to "auto".
    os.environ["RAY_ADDRESS"] = "auto"
    ray.init()
    assert "CPU" not in ray.state.cluster_resources()
    ray.shutdown()
    del os.environ["RAY_ADDRESS"]

    # Prefer `address` parameter to the `RAY_ADDRESS` environment variable,
    # when `address` is not `auto`.
    os.environ["RAY_ADDRESS"] = "test"
    ray.init(address=address)
    assert "CPU" not in ray.state.cluster_resources()
    ray.shutdown()
    del os.environ["RAY_ADDRESS"]

    # Make sure we start a new cluster if RAY_ADDRESS is not set.
    ray.init()
    assert "CPU" in ray.state.cluster_resources()
    ray.shutdown()


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_up_record(
    configure_lang, _unlink_test_ssh_key, configure_aws, monkeypatch, tmp_path
):
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_path / "config.json"))

    def commands_mock(command, stdin):
        # if we want to have e.g. some commands fail,
        # we can have overrides happen here.
        # unfortunately, cutting out SSH prefixes and such
        # is, to put it lightly, non-trivial
        if "uptime" in command:
            return PopenBehaviour(stdout=b"MOCKED uptime")
        if "rsync" in command:
            return PopenBehaviour(stdout=b"MOCKED rsync")
        if "ray" in command:
            return PopenBehaviour(stdout=b"MOCKED ray")
        return PopenBehaviour(stdout=b"MOCKED GENERIC")

    with _setup_popen_mock(commands_mock):
        # config cache does not work with mocks
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [DEFAULT_TEST_CONFIG_PATH, "--no-config-cache", "-y", "--log-style=record"],
        )
        _check_output_via_pattern("test_ray_up_record.txt", result)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
