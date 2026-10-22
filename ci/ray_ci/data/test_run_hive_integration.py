import json
import os
import subprocess
import sys
import textwrap
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
        + textwrap.dedent(
            """\
            import json
            import os
            from pathlib import Path
            import sys

            tool = Path(sys.argv[0]).name
            args = sys.argv[1:]
            with open(os.environ['CALLS'], 'a') as log:
                log.write(json.dumps([tool, *args]) + '\\n')
            scenario = os.environ['SCENARIO']
            cleanup = os.environ['CLEANUP']
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
                if args[0] == 'rm' or args[:2] == ['network', 'rm']:
                    resource = 'container' if args[0] == 'rm' else 'network'
                    if cleanup in (resource + '_residue', 'absent', 'daemon_error'):
                        print('Removal failed: ' + args[-1], file=sys.stderr)
                        sys.exit(1)
                if args[:2] in (['container', 'ls'], ['network', 'ls']):
                    if cleanup == 'daemon_error':
                        print('Cannot connect to Docker daemon', file=sys.stderr)
                        sys.exit(1)
                    name = 'ray-hive-hive-test'
                    if args[0] == 'network':
                        name += '-network'
                    # Similar names must not be mistaken for this run's resources.
                    print(name + '-other')
                    if cleanup == args[0] + '_residue':
                        print(name)
            elif tool == 'bazel' and scenario == 'test_failure':
                sys.exit(7)
            """
        )
    )
    executable.chmod(0o755)
    for tool in ("docker", "bazel", "sleep"):
        (tmp_path / tool).symlink_to(executable)

    def run(scenario, cleanup="success"):
        calls_path = tmp_path / "calls.jsonl"
        env = {
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "BUILDKITE_JOB_ID": "hive-test",
            "RAY_HIVE_LOG_DIR": str(tmp_path / "logs"),
            "RAY_HIVE_STARTUP_TIMEOUT": "0" if scenario == "timeout" else "300",
            "CALLS": str(calls_path),
            "SCENARIO": scenario,
            "CLEANUP": cleanup,
        }
        result = subprocess.run(
            ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=15
        )
        calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
        assert calls[-4:] == [
            ["docker", "rm", "-f", "-v", "ray-hive-hive-test"],
            ["docker", "network", "rm", "ray-hive-hive-test-network"],
            [
                "docker",
                "container",
                "ls",
                "--all",
                "--format",
                "{{.Names}}",
                "--filter",
                "name=ray-hive-hive-test",
            ],
            [
                "docker",
                "network",
                "ls",
                "--format",
                "{{.Name}}",
                "--filter",
                "name=ray-hive-hive-test-network",
            ],
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
    assert service_command[-1].startswith("apache/hive:4.2.0@sha256:")
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
    assert "-t" in probes[-1]
    assert "JAVA_TOOL_OPTIONS=--enable-preview" in probes[-1]
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


@pytest.mark.parametrize(
    "cleanup", ["container_residue", "network_residue", "daemon_error"]
)
@pytest.mark.parametrize("scenario,exit_code", [("success", 1), ("test_failure", 7)])
def test_cleanup_failure_is_reported(run_script, cleanup, scenario, exit_code):
    result, _ = run_script(scenario, cleanup)
    assert result.returncode == exit_code
    assert "Removal failed:" in result.stderr
    assert "HiveServer2 diagnostic logs" in result.stdout
    if cleanup == "daemon_error":
        assert "Cannot connect to Docker daemon" in result.stderr
        assert "Could not verify cleanup of ray-hive-hive-test" in result.stderr
    else:
        name = "ray-hive-hive-test"
        if cleanup == "network_residue":
            name += "-network"
        assert f"Resource remains after cleanup: {name}\n" in result.stderr


@pytest.mark.parametrize("scenario,exit_code", [("success", 0), ("start_failure", 125)])
def test_already_absent_resources_are_clean(run_script, scenario, exit_code):
    result, _ = run_script(scenario, "absent")
    assert result.returncode == exit_code
    assert "Removal failed:" in result.stderr
    assert "Resource remains after cleanup:" not in result.stderr
    assert "Could not verify cleanup" not in result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
