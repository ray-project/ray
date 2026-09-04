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


def _run_script_capturing(test_script, extra_env, *args):
    """Run the real release test script and return its output.

    stderr is merged into stdout so that the returned text preserves the order
    the two streams were actually written in; the tests assert on that order.
    """
    env = {
        **os.environ,
        "NO_INSTALL": "1",
        "NO_CLONE": "1",
        "NO_ARTIFACTS": "1",
        "OVERRIDE_SLEEP_TIME": "0",
        "MAX_RETRIES": "1",
        **extra_env,
    }
    proc = subprocess.run(
        f"{test_script} {' '.join(args)}",
        shell=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.stdout


def test_obs_agent_analysis_is_printed_in_its_own_group(tmpdir):
    analysis_file = os.path.join(tmpdir, "analysis.txt")
    writer = os.path.join(tmpdir, "writer.sh")
    with open(writer, "wt") as fp:
        fp.write(f'echo "the analysis" > {analysis_file}\nexit 40\n')

    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )
    output = _run_script_capturing(
        test_script,
        {
            "RAY_TEST_SCRIPT": f"bash {writer}",
            "RELEASE_TEST_OBS_AGENT_FILE": analysis_file,
        },
        "test_name",
    )

    assert "+++ :robot_face: Observability agent analysis" in output
    assert "the analysis" in output
    # The group is the last thing the script prints, so nothing it emits can be
    # filed under the analysis heading.
    assert output.rstrip().endswith("the analysis")


def test_no_group_when_there_is_no_analysis(tmpdir):
    analysis_file = os.path.join(tmpdir, "analysis.txt")
    writer = os.path.join(tmpdir, "writer.sh")
    with open(writer, "wt") as fp:
        fp.write("exit 40\n")

    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )
    output = _run_script_capturing(
        test_script,
        {
            "RAY_TEST_SCRIPT": f"bash {writer}",
            "RELEASE_TEST_OBS_AGENT_FILE": analysis_file,
        },
        "test_name",
    )

    assert "Observability agent analysis" not in output


def test_stale_analysis_is_not_reported_against_a_later_attempt(tmpdir):
    analysis_file = os.path.join(tmpdir, "analysis.txt")
    with open(analysis_file, "wt") as fp:
        fp.write("stale analysis from a previous attempt\n")

    writer = os.path.join(tmpdir, "writer.sh")
    with open(writer, "wt") as fp:
        fp.write("exit 40\n")

    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )
    output = _run_script_capturing(
        test_script,
        {
            "RAY_TEST_SCRIPT": f"bash {writer}",
            "RELEASE_TEST_OBS_AGENT_FILE": analysis_file,
        },
        "test_name",
    )

    assert "stale analysis" not in output


def test_analysis_cannot_open_a_buildkite_group_of_its_own(tmpdir):
    """The summary is agent-written prose; it must not be read as markup."""
    analysis_file = os.path.join(tmpdir, "analysis.txt")
    writer = os.path.join(tmpdir, "writer.sh")
    with open(writer, "wt") as fp:
        fp.write(
            f'printf "summary line\\n--- not a group\\n+++ nor this\\n" '
            f"> {analysis_file}\nexit 40\n"
        )

    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )
    output = _run_script_capturing(
        test_script,
        {
            "RAY_TEST_SCRIPT": f"bash {writer}",
            "RELEASE_TEST_OBS_AGENT_FILE": analysis_file,
        },
        "test_name",
    )

    assert "+++ :robot_face: Observability agent analysis" in output
    # Every line of the analysis is indented, so none of them sits at the start
    # of a line where buildkite would read it as a group header.
    for line in ("summary line", "--- not a group", "+++ nor this"):
        assert f"  {line}" in output
        assert f"\n{line}" not in output


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
        == 79  # BUILDKITE_RETRY_CODE
    )
    assert _read_state(state_file) == 2


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


def test_buildkite_max_retries_is_inherited(tmpdir):
    """The step publishes the retry budget; the script must not clobber it."""
    recorder = os.path.join(tmpdir, "recorder.sh")
    recorded = os.path.join(tmpdir, "recorded.txt")
    with open(recorder, "wt") as fp:
        fp.write(f'echo "${{BUILDKITE_MAX_RETRIES:-unset}}" > {recorded}\n')

    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )
    env = {
        **os.environ,
        "NO_INSTALL": "1",
        "NO_CLONE": "1",
        "NO_ARTIFACTS": "1",
        "OVERRIDE_SLEEP_TIME": "0",
        "MAX_RETRIES": "1",
        "RAY_TEST_SCRIPT": f"bash {recorder}",
        "BUILDKITE_MAX_RETRIES": "3",
    }
    subprocess.run(f"{test_script} test_name", shell=True, env=env, check=False)

    with open(recorded, "rt") as fp:
        assert fp.read().strip() == "3"


def test_buildkite_max_retries_defaults_to_one(tmpdir):
    """Without a published budget the job keeps the historical default."""
    recorder = os.path.join(tmpdir, "recorder.sh")
    recorded = os.path.join(tmpdir, "recorded.txt")
    with open(recorder, "wt") as fp:
        fp.write(f'echo "${{BUILDKITE_MAX_RETRIES:-unset}}" > {recorded}\n')

    test_script = os.path.join(
        os.path.dirname(__file__), "..", "..", "run_release_test.sh"
    )
    env = {
        **os.environ,
        "NO_INSTALL": "1",
        "NO_CLONE": "1",
        "NO_ARTIFACTS": "1",
        "OVERRIDE_SLEEP_TIME": "0",
        "MAX_RETRIES": "1",
        "RAY_TEST_SCRIPT": f"bash {recorder}",
    }
    env.pop("BUILDKITE_MAX_RETRIES", None)
    subprocess.run(f"{test_script} test_name", shell=True, env=env, check=False)

    with open(recorded, "rt") as fp:
        assert fp.read().strip() == "1"


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
