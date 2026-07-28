import json
import os
import subprocess
import sys
import tempfile

import pytest

from ray_release.exception import ExitCode


@pytest.fixture
def setup(tmpdir):
    state_file = os.path.join(tmpdir, "state.txt")
    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )

    os.environ["NO_INSTALL"] = "1"
    os.environ["NO_CLONE"] = "1"
    os.environ["NO_ARTIFACTS"] = "1"
    os.environ[
        "RAY_TEST_SCRIPT"
    ] = "python ray_release/tests/_test_run_release_test_sh.py"
    os.environ["OVERRIDE_SLEEP_TIME"] = "0"
    os.environ["MAX_RETRIES"] = "3"
    os.environ["RELEASE_TEST_RETRY_MARKER"] = os.path.join(tmpdir, "retry_marker")
    # The fake test script imports ray_release; make sure it resolves under bazel.
    os.environ["PYTHONPATH"] = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    # should_retry() reads these; give the fake script a real retry budget.
    os.environ["BUILDKITE_TIME_LIMIT_FOR_RETRY"] = "10800"
    os.environ.pop("BUILDKITE_RETRY_COUNT", None)

    yield state_file, test_script


def _read_state(state_file):
    with open(state_file, "rt") as f:
        return int(f.read())


def _run_script(test_script, state_file, *exits):
    assert len(exits) == 3

    if os.path.exists(state_file):
        os.unlink(state_file)

    try:
        return subprocess.check_call(
            f"{test_script} "
            f"{state_file} "
            f"{' '.join(str(e.value) for e in exits)}",
            shell=True,
        )
    except subprocess.CalledProcessError as e:
        return e.returncode


def test_repeat(setup):
    state_file, test_script = setup

    assert (
        _run_script(
            test_script,
            state_file,
            ExitCode.SUCCESS,
            ExitCode.SUCCESS,
            ExitCode.SUCCESS,
        )
        == ExitCode.SUCCESS.value
    )
    assert _read_state(state_file) == 1

    assert (
        _run_script(
            test_script,
            state_file,
            ExitCode.RAY_WHEELS_TIMEOUT,
            ExitCode.SUCCESS,
            ExitCode.SUCCESS,
        )
        == ExitCode.SUCCESS.value
    )
    assert _read_state(state_file) == 2

    assert (
        _run_script(
            test_script,
            state_file,
            ExitCode.RAY_WHEELS_TIMEOUT,
            ExitCode.CLUSTER_ENV_BUILD_TIMEOUT,
            ExitCode.SUCCESS,
        )
        == ExitCode.SUCCESS.value
    )
    assert _read_state(state_file) == 3

    assert (
        _run_script(
            test_script,
            state_file,
            ExitCode.CLUSTER_STARTUP_TIMEOUT,
            ExitCode.CLUSTER_WAIT_TIMEOUT,
            ExitCode.RAY_WHEELS_TIMEOUT,
        )
        == 79  # BUILDKITE_RETRY_CODE
    )
    assert _read_state(state_file) == 3

    assert (
        _run_script(
            test_script,
            state_file,
            ExitCode.RAY_WHEELS_TIMEOUT,
            ExitCode.COMMAND_ALERT,
            ExitCode.SUCCESS,
        )
        == ExitCode.COMMAND_ALERT.value
    )
    assert _read_state(state_file) == 2


@pytest.mark.parametrize(
    "exit_code",
    [
        ExitCode.COMMAND_ERROR,
        ExitCode.COMMAND_ALERT,
        ExitCode.PREPARE_ERROR,
        ExitCode.CONFIG_ERROR,
        ExitCode.CLI_ERROR,
        ExitCode.FETCH_RESULT_ERROR,
    ],
)
def test_non_infra_failures_are_not_retried(setup, exit_code):
    """These must surface their own exit code, not the Buildkite retry code."""
    state_file, test_script = setup

    assert (
        _run_script(test_script, state_file, exit_code, exit_code, exit_code)
        == exit_code.value
    )
    assert _read_state(state_file) == 1


@pytest.mark.parametrize(
    "exit_code",
    [
        ExitCode.SETUP_ERROR,
        ExitCode.CLUSTER_RESOURCE_ERROR,
        ExitCode.ANYSCALE_ERROR,
        ExitCode.CLUSTER_STARTUP_TIMEOUT,
    ],
)
def test_infra_failures_ask_for_a_retry(setup, exit_code):
    state_file, test_script = setup

    assert (
        _run_script(test_script, state_file, exit_code, exit_code, exit_code)
        == 79  # BUILDKITE_RETRY_CODE
    )


def test_parameters(setup):
    state_file, test_script = setup

    os.environ["RAY_TEST_SCRIPT"] = "python ray_release/tests/_test_catch_args.py"

    with tempfile.TemporaryDirectory() as tmpdir:
        argv_file = os.path.join(tmpdir, "argv.json")

        subprocess.check_call(
            f"{test_script} " f"{argv_file} " f"--smoke-test",
            shell=True,
        )

        with open(argv_file, "rt") as fp:
            data = json.load(fp)

    assert "--smoke-test" in data


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
