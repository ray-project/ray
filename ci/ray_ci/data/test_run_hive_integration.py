import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("run_hive_integration.sh")


@pytest.fixture
def run_script(tmp_path):
    # Exercise the shell's actual control flow without requiring Docker in CI's
    # unit-test image. Fake executables record arguments and inject failures.
    executable = tmp_path / "fake_tool"
    executable.write_text(
        f"#!{sys.executable}\n"
        """
import json
import os
from pathlib import Path
import sys

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as log:
    log.write(json.dumps([tool, *args]) + '\\n')
scenario = os.environ['SCENARIO']
if tool == 'docker':
    if args[0] == 'run' and scenario == 'start_failure':
        sys.exit(125)
    if args[0] == 'exec':
        print('SQL probe output')
        if scenario in ('crashed', 'timeout'):
            sys.exit(1)
        if scenario == 'retry':
            marker = Path(os.environ['CALLS'] + '.probe')
            if not marker.exists():
                marker.touch()
                sys.exit(1)
    if args[0] == 'inspect':
        print('false' if scenario == 'crashed' else 'true')
    if args[0] == 'logs':
        print('HiveServer2 diagnostic logs')
elif tool == 'bazel' and scenario == 'test_failure':
    sys.exit(7)
"""
    )
    executable.chmod(0o755)
    for tool in ("docker", "bazel", "sleep"):
        (tmp_path / tool).symlink_to(executable)

    def run(scenario):
        calls_path = tmp_path / "calls.jsonl"
        env = {
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "BUILDKITE_JOB_ID": "hive-test",
            "RAY_HIVE_LOG_DIR": str(tmp_path / "logs"),
            "RAY_HIVE_STARTUP_TIMEOUT": "0" if scenario == "timeout" else "300",
            "CALLS": str(calls_path),
            "SCENARIO": scenario,
        }
        result = subprocess.run(
            ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=15
        )
        calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
        assert calls[-2:] == [
            ["docker", "rm", "-f", "-v", "ray-hive-hive-test"],
            ["docker", "network", "rm", "ray-hive-hive-test-network"],
        ]
        assert (
            "HiveServer2 diagnostic logs"
            in (tmp_path / "logs/hiveserver2.log").read_text()
        )
        return result, calls

    return run


@pytest.mark.parametrize("scenario", ["success", "retry"])
def test_ready_service_runs_suite(run_script, scenario):
    result, calls = run_script(scenario)
    assert result.returncode == 0, result.stderr
    service_command = next(call for call in calls if call[:2] == ["docker", "run"])
    assert "-Dhive.server2.authentication=NONE" in " ".join(service_command)
    assert service_command[service_command.index("--network-alias") + 1] == (
        "hiveserver2"
    )
    test_command = next(call for call in calls if call[0] == "bazel")
    assert "//python/ray/data:test_hive_integration" in test_command
    assert "--exact-targets" in test_command
    assert test_command[test_command.index("--network") + 1] == (
        service_command[service_command.index("--network") + 1]
    )
    test_envs = [
        test_command[i + 1] for i, arg in enumerate(test_command) if arg == "--test-env"
    ]
    assert "RAY_HIVE_TEST_HOST=hiveserver2" in test_envs
    assert "RAY_HIVE_TEST_AUTH=PLAIN" in test_envs
    assert "HIVE_TEST_PASSWORD=hive" in test_envs
    probes = [call for call in calls if call[:2] == ["docker", "exec"]]
    assert len(probes) == (2 if scenario == "retry" else 1)
    assert probes[-1][-2:] == ["-e", "SELECT 1"]
    assert calls.index(probes[-1]) < calls.index(test_command)


@pytest.mark.parametrize(
    "scenario,exit_code",
    [("start_failure", 125), ("crashed", 1), ("timeout", 1), ("test_failure", 7)],
)
def test_failure_propagates_and_cleans_up(run_script, scenario, exit_code):
    result, calls = run_script(scenario)
    assert result.returncode == exit_code
    assert "HiveServer2 diagnostic logs" in result.stdout
    assert any(call[0] == "bazel" for call in calls) == (scenario == "test_failure")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
