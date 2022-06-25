"""
Some instructions on writing CLI tests:
1. Look at test_ray_start for a simple output test example.
2. To get a valid regex, start with copy-pasting your output from a captured
   version (no formatting). Then escape ALL regex characters (parenthesis,
   brackets, dots, etc.). THEN add ".+" to all the places where info might
   change run to run.
3. Look at test_ray_up for an example of how to mock AWS, commands,
   and autoscaler config.
4. Print your outputs!!!! Tests are impossible to debug if they fail
   and you did not print anything. Since command output is captured by click,
   MAKE SURE YOU print(result.output) when tests fail!!!

WARNING: IF YOU MOCK AWS, DON'T FORGET THE AWS_CREDENTIALS FIXTURE.
         THIS IS REQUIRED SO BOTO3 DOES NOT ACCESS THE ACTUAL AWS SERVERS.

Note: config cache does not work with AWS mocks since the AWS resource ids are
      randomized each time.
"""
import glob
import multiprocessing as mp
import os
import re
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import moto
import pytest
from click.testing import CliRunner
from moto import mock_ec2, mock_iam
from testfixtures import Replacer
from testfixtures.popen import MockPopen, PopenBehaviour

import ray
import ray.autoscaler._private.aws.config as aws_config
import ray._private.ray_constants as ray_constants
import ray.scripts.scripts as scripts
from ray._private.test_utils import wait_for_condition
from ray.cluster_utils import cluster_not_supported

import psutil

boto3_list = [
    {
        "InstanceType": "t1.micro",
        "VCpuInfo": {"DefaultVCpus": 1},
        "MemoryInfo": {"SizeInMiB": 627},
    },
    {
        "InstanceType": "t3a.small",
        "VCpuInfo": {"DefaultVCpus": 2},
        "MemoryInfo": {"SizeInMiB": 2048},
    },
    {
        "InstanceType": "m4.4xlarge",
        "VCpuInfo": {"DefaultVCpus": 16},
        "MemoryInfo": {"SizeInMiB": 65536},
    },
    {
        "InstanceType": "p3.8xlarge",
        "VCpuInfo": {"DefaultVCpus": 32},
        "MemoryInfo": {"SizeInMiB": 249856},
        "GpuInfo": {"Gpus": [{"Name": "V100", "Count": 4}]},
    },
]


@pytest.fixture
def configure_lang():
    """Configure output for travis + click."""
    if sys.platform != "darwin":
        os.environ["LC_ALL"] = "C.UTF-8"
        os.environ["LANG"] = "C.UTF-8"


@pytest.fixture
def configure_aws():
    """Mocked AWS Credentials for moto."""
    os.environ["LC_ALL"] = "C.UTF-8"
    os.environ["LANG"] = "C.UTF-8"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

    # moto (boto3 mock) only allows a hardcoded set of AMIs
    dlami = (
        moto.ec2.ec2_backends["us-west-2"]
        .describe_images(filters={"name": "Deep Learning AMI Ubuntu*"})[0]
        .id
    )
    aws_config.DEFAULT_AMI["us-west-2"] = dlami
    list_instances_mock = MagicMock(return_value=boto3_list)
    with patch(
        "ray.autoscaler._private.aws.node_provider.list_ec2_instances",
        list_instances_mock,
    ):
        yield


@pytest.fixture(scope="function")
def _unlink_test_ssh_key():
    """Use this to remove the keys spawned by ray up."""
    yield
    try:
        for path in glob.glob(os.path.expanduser("~/.ssh/__test-cli_key*")):
            os.remove(path)
    except FileNotFoundError:
        pass


def _start_ray_and_block(runner, child_conn: mp.connection.Connection, as_head: bool):
    """Utility function to start a CLI command with `ray start --block`

    This function is expected to be run in another process, where `child_conn` is used
    for IPC with the parent.
    """
    args = ["--block"]
    if as_head:
        args.append("--head")
    else:
        # Worker node
        args.append(f"--address=localhost:{ray_constants.DEFAULT_PORT}")

    result = runner.invoke(
        scripts.start,
        args,
    )
    # Should be blocked until stopped by signals (SIGTERM)
    child_conn.send(result.output)


def _debug_die(output, assert_msg: str = ""):
    print("!!!!")
    print(output)
    print("!!!!")
    assert False, assert_msg


def _fail_if_false(
    predicate: bool, stdout: Optional[str] = "", assert_msg: Optional[str] = ""
):
    if not predicate:
        _debug_die(stdout, assert_msg)


def _die_on_error(result):
    _fail_if_false(result.exit_code == 0, result.output)


def _debug_check_line_by_line(result, expected_lines):
    output_lines = result.output.split("\n")
    i = 0

    for out in output_lines:
        if i >= len(expected_lines):
            i += 1
            print("!!!!!! Expected fewer lines")
            context = [f"CONTEXT: {line}" for line in output_lines[i - 3 : i]]
            print("\n".join(context))
            extra = [f"-- {line}" for line in output_lines[i:]]
            print("\n".join(extra))
            break

        exp = expected_lines[i]
        matched = re.fullmatch(exp + r" *", out) is not None
        if not matched:
            print(f"{i:>3}: {out}")
            print(f"!!! ^ ERROR: Expected (regex): {repr(exp)}")
        else:
            print(f"{i:>3}: {out}")
        i += 1
    if i < len(expected_lines):
        print("!!! ERROR: Expected extra lines (regex):")
        for line in expected_lines[i:]:

            print(repr(line))

    assert False


@contextmanager
def _setup_popen_mock(commands_mock, commands_verifier=None):
    """
    Mock subprocess.Popen's behavior and if applicable, intercept the commands
    received by Popen and check if they are as expected using
    commands_verifier provided by caller.
    TODO(xwjiang): Ideally we should write a lexical analyzer that can parse
    in a more intelligent way.
    """
    Popen = MockPopen()
    Popen.set_default(behaviour=commands_mock)

    with Replacer() as replacer:
        replacer.replace("subprocess.Popen", Popen)
        yield

    if commands_verifier:
        assert commands_verifier(Popen.all_calls)


def _load_output_pattern(name):
    pattern_dir = Path(__file__).parent / "test_cli_patterns"
    with open(str(pattern_dir / name)) as f:
        # Remove \n from each line.
        # Substitute the Ray version in each line containing the string
        # {ray_version}.
        out = []
        for x in f.readlines():
            if "{ray_version}" in x:
                out.append(x[:-1].format(ray_version=ray.__version__))
            else:
                out.append(x[:-1])
        return out


def _check_output_via_pattern(name, result):
    expected_lines = _load_output_pattern(name)

    if result.exception is not None:
        raise result.exception from None
    print(result.output)
    expected = r" *\n".join(expected_lines) + "\n?"
    if re.fullmatch(expected, result.output) is None:
        _debug_check_line_by_line(result, expected_lines)

    assert result.exit_code == 0


DEFAULT_TEST_CONFIG_PATH = str(
    Path(__file__).parent / "test_cli_patterns" / "test_ray_up_config.yaml"
)

MISSING_MAX_WORKER_CONFIG_PATH = str(
    Path(__file__).parent
    / "test_cli_patterns"
    / "test_ray_up_no_max_worker_config.yaml"
)

DOCKER_TEST_CONFIG_PATH = str(
    Path(__file__).parent / "test_cli_patterns" / "test_ray_up_docker_config.yaml"
)


def test_enable_usage_stats(monkeypatch, tmp_path):
    tmp_usage_stats_config_path = tmp_path / "config.json"
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
    runner = CliRunner()
    runner.invoke(scripts.enable_usage_stats, [])
    assert '{"usage_stats": true}' == tmp_usage_stats_config_path.read_text()


def test_disable_usage_stats(monkeypatch, tmp_path):
    tmp_usage_stats_config_path = tmp_path / "config.json"
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
    runner = CliRunner()
    runner.invoke(scripts.disable_usage_stats, [])
    assert '{"usage_stats": false}' == tmp_usage_stats_config_path.read_text()


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
def test_ray_start(configure_lang, monkeypatch, tmp_path):
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_path / "config.json"))
    runner = CliRunner()
    temp_dir = os.path.join("/tmp", uuid.uuid4().hex)
    result = runner.invoke(
        scripts.start,
        [
            "--head",
            "--log-style=pretty",
            "--log-color",
            "False",
            "--port",
            "0",
            "--temp-dir",
            temp_dir,
        ],
    )

    # Check that --temp-dir arg worked:
    assert os.path.isfile(os.path.join(temp_dir, "ray_current_cluster"))
    assert os.path.isdir(os.path.join(temp_dir, "session_latest"))

    _die_on_error(runner.invoke(scripts.stop))

    if ray.util.get_node_ip_address() == "127.0.0.1":
        _check_output_via_pattern("test_ray_start_localhost.txt", result)
    else:
        _check_output_via_pattern("test_ray_start.txt", result)


def _ray_start_hook(ray_params, head):
    os.makedirs(ray_params.temp_dir, exist_ok=True)
    with open(os.path.join(ray_params.temp_dir, "ray_hook_ok"), "w") as f:
        f.write("HOOK_OK")


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
def test_ray_start_hook(configure_lang, monkeypatch, tmp_path):
    monkeypatch.setenv("RAY_START_HOOK", "ray.tests.test_cli._ray_start_hook")
    runner = CliRunner()
    temp_dir = os.path.join("/tmp", uuid.uuid4().hex)
    runner.invoke(
        scripts.start,
        [
            "--head",
            "--log-style=pretty",
            "--log-color",
            "False",
            "--port",
            "0",
            "--temp-dir",
            temp_dir,
        ],
    )

    # Check that the hook executed.
    assert os.path.exists(temp_dir)
    assert os.path.exists(os.path.join(temp_dir, "ray_hook_ok"))

    _die_on_error(runner.invoke(scripts.stop))


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support. "),
)
@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows signal handling not compatible"
)
def test_ray_start_head_block_and_signals(configure_lang, monkeypatch, tmp_path):
    """Test `ray start` with `--block` as heads and workers and signal handles"""

    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_path / "config.json"))
    runner = CliRunner()

    head_parent_conn, head_child_conn = mp.Pipe()

    # Run `ray start --block --head` in another process and blocks
    head_proc = mp.Process(
        target=_start_ray_and_block,
        kwargs={"runner": runner, "child_conn": head_child_conn, "as_head": True},
    )

    # Run
    head_proc.start()

    # Give it some time to start various subprocesses and `ray stop`
    # A smaller interval seems to cause occasional failure as the head process
    # was stopped too early before spawning all the subprocesses.
    time.sleep(5)

    # Terminate some of the children process
    children = psutil.Process(head_proc.pid).children()

    # Terminate everyone other than GCS
    # NOTE(rickyyx): The choice of picking GCS is arbitrary.
    gcs_proc = None
    for child in children:
        if "gcs_server" in child.name():
            gcs_proc = child
            continue
        child.terminate()
        child.wait(5)

        if not head_proc.is_alive():
            # NOTE(rickyyx): call recv() here is safe since the process
            # is guaranteed to be terminated.
            _fail_if_false(
                False,
                head_parent_conn.recv(),
                (
                    "`ray start --head --block` should not exit"
                    f"({head_proc.exitcode}) when a subprocess is "
                    "terminated with SIGTERM."
                ),
            )

    # Kill the GCS last should unblock the CLI
    gcs_proc.kill()
    gcs_proc.wait(5)

    # NOTE(rickyyx): The wait here is needed for the `head_proc`
    # process to exit
    head_proc.join(5)

    # Process with "--block" should be dead with a subprocess killed
    if head_proc.is_alive() or head_proc.exitcode == 0:
        # NOTE(rickyyx): call recv() here is safe since the process
        # is guaranteed to be terminated thus invocation is non-blocking.
        _fail_if_false(
            False,
            head_parent_conn.recv() if not head_proc.is_alive() else "still alive",
            (
                "Head process should have exited with errors when one of"
                f" subprocesses killed. But exited={head_proc.exitcode}"
            ),
        )


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support. "),
)
@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows signal handling not compatible"
)
def test_ray_start_block_and_stop(configure_lang, monkeypatch, tmp_path):
    """Test `ray start` with `--block` as heads and workers and `ray stop`"""
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_path / "config.json"))
    runner = CliRunner()

    head_parent_conn, head_child_conn = mp.Pipe()
    worker_parent_conn, worker_child_conn = mp.Pipe()

    # Run `ray start --block --head` in another process and blocks
    head_proc = mp.Process(
        target=_start_ray_and_block,
        kwargs={"runner": runner, "child_conn": head_child_conn, "as_head": True},
    )

    # Run `ray start --block --address=localhost:DEFAULT_PORT`
    worker_proc = mp.Process(
        target=_start_ray_and_block,
        kwargs={"runner": runner, "child_conn": worker_child_conn, "as_head": False},
    )

    # Run
    head_proc.start()
    worker_proc.start()

    # Give it some time to start various subprocesses and `ray stop`
    # A smaller interval seems to cause occasional failure as the head process
    # was stopped too early before spawning all the subprocesses.
    time.sleep(5)
    stop_result = runner.invoke(scripts.stop)
    _die_on_error(stop_result)

    # Process with "--block" should be blocked forever w/o
    # termination by signals
    if not head_proc.is_alive():
        # NOTE(rickyyx): call recv() here is safe since the process
        # is guaranteed to be terminated.
        _fail_if_false(
            False,
            head_parent_conn.recv(),
            (
                "`ray start --head --block` should block forever even"
                " though Ray subprocesses are stopped normally. But "
                f"it exited with {head_proc.exitcode} early. \n"
                f"Stop command: {stop_result.output}"
            ),
        )

    if not worker_proc.is_alive():
        _fail_if_false(
            False,
            worker_parent_conn.recv(),
            (
                "`ray start --block` should block forever even"
                " though Ray subprocesses are stopped normally. But"
                f"it exited with {worker_proc.exitcode} already. \n"
                f"Stop command: {stop_result.output}"
            ),
        )

    # Stop both worker and head with SIGTERM
    head_proc.terminate()
    worker_proc.terminate()

    if head_parent_conn.poll(3):
        head_output = head_parent_conn.recv()
    if worker_parent_conn.poll(3):
        worker_output = worker_parent_conn.recv()

    head_proc.join(5)
    worker_proc.join(5)

    _fail_if_false(
        head_proc.exitcode == 0,
        head_output,
        f"Head process failed unexpectedly({head_proc.exitcode})",
    )

    _fail_if_false(
        worker_proc.exitcode == 0,
        worker_output,
        f"Worker process failed unexpectedly({worker_proc.exitcode})",
    )


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_up(
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
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _check_output_via_pattern("test_ray_up.txt", result)


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_up_docker(
    configure_lang, _unlink_test_ssh_key, configure_aws, monkeypatch, tmp_path
):
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_path / "config.json"))

    def commands_mock(command, stdin):
        # if we want to have e.g. some commands fail,
        # we can have overrides happen here.
        # unfortunately, cutting out SSH prefixes and such
        # is, to put it lightly, non-trivial
        if ".Config.Env" in command:
            return PopenBehaviour(stdout=b"{}")
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
            [
                DOCKER_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _check_output_via_pattern("test_ray_up_docker.txt", result)


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


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_attach(configure_lang, configure_aws, _unlink_test_ssh_key):
    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        print("ubuntu@ip-.+:~$ exit")
        return PopenBehaviour(stdout="ubuntu@ip-.+:~$ exit")

    with _setup_popen_mock(commands_mock):
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _die_on_error(result)

        result = runner.invoke(
            scripts.attach,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )

        _check_output_via_pattern("test_ray_attach.txt", result)


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_dashboard(configure_lang, configure_aws, _unlink_test_ssh_key):
    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        print("ubuntu@ip-.+:~$ exit")
        return PopenBehaviour(stdout="ubuntu@ip-.+:~$ exit")

    with _setup_popen_mock(commands_mock):
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _die_on_error(result)

        result = runner.invoke(
            scripts.dashboard, [DEFAULT_TEST_CONFIG_PATH, "--no-config-cache"]
        )
        _check_output_via_pattern("test_ray_dashboard.txt", result)


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_exec(configure_lang, configure_aws, _unlink_test_ssh_key):
    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        print("This is a test!")
        return PopenBehaviour(stdout=b"This is a test!")

    def commands_verifier(calls):
        for call in calls:
            if len(call[1]) > 0:
                if any(" ray stop; " in token for token in call[1][0]):
                    return True
        return False

    with _setup_popen_mock(commands_mock, commands_verifier):
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _die_on_error(result)

        result = runner.invoke(
            scripts.exec,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "--log-style=pretty",
                '"echo This is a test!"',
                "--stop",
            ],
        )

        _check_output_via_pattern("test_ray_exec.txt", result)


# Try to check if we are running in travis. Bazel overrides and controls
# env vars, so the typical travis env-vars don't help.
# Unfortunately it will not be nice if your username is travis
# and you're running on a Mac.
@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_submit(configure_lang, configure_aws, _unlink_test_ssh_key):
    def commands_mock(command, stdin):
        # TODO(maximsmol): this is a hack since stdout=sys.stdout
        #                  doesn't work with the mock for some reason
        if "rsync" not in command:
            print("This is a test!")
        return PopenBehaviour(stdout=b"This is a test!")

    with _setup_popen_mock(commands_mock):
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _die_on_error(result)

        with tempfile.NamedTemporaryFile(suffix="test.py", mode="w") as f:
            f.write("print('This is a test!')\n")
            result = runner.invoke(
                scripts.submit,
                [
                    DEFAULT_TEST_CONFIG_PATH,
                    "--no-config-cache",
                    "--log-style=pretty",
                    "--log-color",
                    "False",
                    # this is somewhat misleading, since the file
                    # actually never gets run
                    # TODO(maximsmol): make this work properly one day?
                    f.name,
                ],
            )

            _check_output_via_pattern("test_ray_submit.txt", result)


def test_ray_status(shutdown_only, monkeypatch):
    import ray

    address = ray.init(num_cpus=3).get("address")
    runner = CliRunner()

    def output_ready():
        result = runner.invoke(scripts.status)
        result.stdout
        if not result.exception and "memory" in result.output:
            return True
        raise RuntimeError(
            f"result.exception={result.exception} " f"result.output={result.output}"
        )

    wait_for_condition(output_ready)

    result = runner.invoke(scripts.status, [])
    _check_output_via_pattern("test_ray_status.txt", result)

    result_arg = runner.invoke(scripts.status, ["--address", address])
    _check_output_via_pattern("test_ray_status.txt", result_arg)

    # Try to check status with RAY_ADDRESS set
    monkeypatch.setenv("RAY_ADDRESS", address)
    result_env = runner.invoke(scripts.status)
    _check_output_via_pattern("test_ray_status.txt", result_env)

    result_env_arg = runner.invoke(scripts.status, ["--address", address])
    _check_output_via_pattern("test_ray_status.txt", result_env_arg)


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported on Windows")
def test_ray_status_multinode(ray_start_cluster):
    cluster = ray_start_cluster
    for _ in range(4):
        cluster.add_node(num_cpus=2)
    runner = CliRunner()

    def output_ready():
        result = runner.invoke(scripts.status)
        result.stdout
        if not result.exception and "memory" in result.output:
            return True
        raise RuntimeError(
            f"result.exception={result.exception} " f"result.output={result.output}"
        )

    wait_for_condition(output_ready)

    result = runner.invoke(scripts.status, [])
    _check_output_via_pattern("test_ray_status_multinode.txt", result)


@pytest.mark.skipif(
    sys.platform == "darwin" and "travis" in os.environ.get("USER", ""),
    reason=("Mac builds don't provide proper locale support"),
)
@mock_ec2
@mock_iam
def test_ray_cluster_dump(configure_lang, configure_aws, _unlink_test_ssh_key):
    def commands_mock(command, stdin):
        print("This is a test!")
        return PopenBehaviour(stdout=b"This is a test!")

    with _setup_popen_mock(commands_mock):
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [
                DEFAULT_TEST_CONFIG_PATH,
                "--no-config-cache",
                "-y",
                "--log-style=pretty",
                "--log-color",
                "False",
            ],
        )
        _die_on_error(result)

        result = runner.invoke(
            scripts.cluster_dump, [DEFAULT_TEST_CONFIG_PATH, "--no-processes"]
        )

        _check_output_via_pattern("test_ray_cluster_dump.txt", result)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
