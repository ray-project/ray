"""Run the selected real-service profiles once, preserving failures and cleanup."""

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from xml.etree import ElementTree

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parents[3]
CASES = json.loads((HERE / "cases.json").read_text())
PROXIES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
TARGET = "python/ray/data/tests/datasource/test_hive_auth_integration.py"
PYTEST_ENTRYPOINT = """
import sys
from pathlib import Path

import pytest

code = int(pytest.main(sys.argv[2:]))
Path(sys.argv[1]).write_text(str(code))
sys.exit(42 if code == 1 else code)
"""


class CommandFailed(RuntimeError):
    def __init__(self, stage, code):
        super().__init__(f"{stage} failed (exit {code})")
        self.code = code


def command(args, log, *, env, timeout, check=True):
    """Bound a process group and preserve its actual exit code."""
    with Path(log).open("ab") as stream:
        process = subprocess.Popen(
            args,
            cwd=SOURCE,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt, InterruptedError):
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    if check and code:
        raise CommandFailed(Path(log).stem, code)
    return code


def validate_results(path, profile):
    """Reject empty, skipped, partial or unexpectedly collected profiles."""
    root = ElementTree.parse(path).getroot()
    cases = list(root.iter("testcase"))
    expected = {f"test_hive_auth[{case}]" for case in CASES[profile]}
    names = [case.attrib["name"] for case in cases]
    if not names or len(names) != len(expected) or set(names) != expected:
        raise ValueError("Selected profile did not execute its exact case list")
    if any(
        case.find(tag) is not None
        for case in cases
        for tag in ("failure", "error", "skipped")
    ):
        raise ValueError("Selected profile has failed, errored, or skipped cases")
    return len(cases)


def image_info(image, path, env):
    template = (
        '{"id":{{json .Id}},"created":{{json .Created}},'
        '"tags":{{json .RepoTags}},"labels":{{json (index .Config "Labels")}}}'
    )
    command(
        ["docker", "image", "inspect", image, "--format", template],
        path,
        env=env,
        timeout=30,
    )
    return json.loads(path.read_text())


def remove_owned_image(image, expected, run_id, directory, env):
    current = image_info(image, directory / "image-cleanup-identity.json", env)
    if (
        not expected
        or current["id"] != expected["id"]
        or not current.get("created")
        or current["created"] != expected["created"]
        or (current.get("labels") or {}).get("com.ray.hive-auth.run") != run_id
        or image not in (current.get("tags") or [])
    ):
        raise ValueError("Runtime image ownership changed; preserve it")
    command(
        ["docker", "image", "rm", image],
        directory / "image-cleanup.log",
        env=env,
        timeout=30,
    )


def remove_owned_dangling(run_id, directory, env):
    path = directory / "owned-dangling.txt"
    command(
        [
            "docker",
            "image",
            "ls",
            "--no-trunc",
            "--filter",
            "dangling=true",
            "--filter",
            f"label=com.ray.hive-auth.run={run_id}",
            "--format",
            "{{.ID}}",
        ],
        path,
        env=env,
        timeout=15,
    )
    for index, image_id in enumerate(path.read_text().splitlines()):
        info = image_info(image_id, directory / f"dangling-{index}.json", env)
        if (
            info["id"] != image_id
            or not info.get("created")
            or info.get("tags")
            or (info.get("labels") or {}).get("com.ray.hive-auth.run") != run_id
        ):
            raise ValueError("Unconfirmed dangling image; preserve it")
        command(
            ["docker", "image", "rm", image_id],
            directory / f"dangling-{index}-cleanup.log",
            env=env,
            timeout=30,
        )


def wait_for_probe(compose, probe, directory, env, deadline):
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"{probe} readiness deadline exceeded")
        try:
            code = command(
                compose
                + ["exec", "-T", "head", "python", "/opt/hive_auth/probe.py", probe],
                directory / f"ready-{probe}.log",
                env=env,
                timeout=min(30, remaining),
                check=False,
            )
        except subprocess.TimeoutExpired:
            code = 1
        if code == 0:
            return
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def run_profile(args, profile, runtime, run_id, directory):
    directory.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in PROXIES}
    project = f"ray-hive-auth-{run_id}-{profile.lower()}"
    env.update(
        HIVE_AUTH_RUNTIME_IMAGE=runtime,
        HIVE_AUTH_PROFILE=profile,
        HIVE_AUTH_SOURCE=os.environ.get("RAYCI_CHECKOUT_DIR", str(SOURCE)),
        HIVE_AUTH_RUN_ID=run_id,
        HIVE_AUTH_HIVE_IMAGE=args.hive_image,
        HIVE_AUTH_PLACEHOLDER=secrets.token_urlsafe(16),
    )
    compose = ["docker", "compose", "-f", str(HERE / "compose.yaml"), "-p", project]
    started = False
    code = 0
    stage = "preflight"
    result = {"profile": profile, "project": project, "state": "not_run"}
    cleanup_deadline = None
    deadline = time.monotonic() + args.profile_timeout
    ready_deadline = min(deadline, time.monotonic() + args.readiness_timeout)
    try:
        command(
            compose + ["config", "--quiet"],
            directory / "config.log",
            env=env,
            timeout=30,
        )
        stage = "start"
        # up may fail after creating only some of this project's resources.
        started = True
        command(
            compose + ["up", "-d", "--no-build"],
            directory / "start.log",
            env=env,
            timeout=max(1, ready_deadline - time.monotonic()),
        )
        stage = "ready"
        wait_for_probe(compose, "sql", directory, env, ready_deadline)
        wait_for_probe(compose, "workers", directory, env, ready_deadline)
        if args.prepare_only:
            result["state"] = "prepared_only"
        else:
            stage = "tests"
            command(
                compose
                + [
                    "exec",
                    "-T",
                    "head",
                    "python",
                    "-c",
                    PYTEST_ENTRYPOINT,
                    "/events/pytest-exit-code",
                    "-v",
                    "-x",
                    "--timeout=180",
                    "-p",
                    "no:cacheprovider",
                    "--junitxml=/events/results.xml",
                    TARGET,
                ],
                directory / "pytest.log",
                env=env,
                timeout=max(1, deadline - time.monotonic()),
            )
            stage = "results"
            cleanup_deadline = time.monotonic() + args.cleanup_timeout
            command(
                compose + ["cp", "head:/events/.", str(directory / "events")],
                directory / "copy.log",
                env=env,
                timeout=30,
            )
            result.update(
                state="passed",
                cases=validate_results(directory / "events/results.xml", profile),
            )
    except (
        CommandFailed,
        subprocess.TimeoutExpired,
        TimeoutError,
        ValueError,
        ElementTree.ParseError,
        OSError,
        KeyboardInterrupt,
        InterruptedError,
    ) as exc:
        code = exc.code if isinstance(exc, CommandFailed) else 1
        result.update(state="failed", stage=stage, error=type(exc).__name__)
        print(
            f"{profile}: {stage}: {type(exc).__name__}; see {directory}",
            file=sys.stderr,
        )
    finally:
        if started:
            cleanup_deadline = (
                cleanup_deadline or time.monotonic() + args.cleanup_timeout
            )
            # Only the dedicated project is touched. No global Docker cleanup.
            diagnostics = [("service", ["logs", "--no-color", "--tail", "200"])]
            if code:
                diagnostics.append(
                    (
                        "events",
                        ["cp", "head:/events/.", str(directory / "failure-events")],
                    )
                )
            for suffix, tail in diagnostics:
                try:
                    command(
                        compose + tail,
                        directory / f"{suffix}.log",
                        env=env,
                        timeout=min(
                            10, max(1, cleanup_deadline - time.monotonic() - 5)
                        ),
                        check=False,
                    )
                except (
                    OSError,
                    subprocess.TimeoutExpired,
                    InterruptedError,
                    KeyboardInterrupt,
                ):
                    pass
            try:
                command(
                    compose
                    + ["down", "--volumes", "--remove-orphans", "--timeout", "20"],
                    directory / "cleanup.log",
                    env=env,
                    timeout=max(1, cleanup_deadline - time.monotonic() - 5),
                )
            except (
                CommandFailed,
                OSError,
                subprocess.TimeoutExpired,
                InterruptedError,
                KeyboardInterrupt,
            ) as exc:
                result["cleanup"] = "failed"
                code = code or (exc.code if isinstance(exc, CommandFailed) else 1)
            else:
                result["cleanup"] = "passed"
        result["exit_code"] = code
        (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return code, result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--profiles", nargs="+", choices=CASES, default=list(CASES))
    result.add_argument(
        "--runtime-image",
        help="Reuse an already built runtime without rebuilding or removing it",
    )
    result.add_argument(
        "--ray-image",
        default=os.environ.get("HIVE_AUTH_RAY_IMAGE", "rayproject/ray:2.59.0-py312"),
    )
    result.add_argument("--hive-image", default="apache/hive:4.2.0")
    result.add_argument(
        "--requirements",
        default="python/requirements/data/hive-auth.txt",
        help="Use the CI lock file when building the CI runtime",
    )
    result.add_argument("--log-dir", type=Path, required=True)
    result.add_argument("--prepare-only", action="store_true")
    result.add_argument("--readiness-timeout", type=int, default=300)
    result.add_argument("--profile-timeout", type=int, default=1200)
    result.add_argument("--cleanup-timeout", type=int, default=60)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    if len(args.profiles) != len(set(args.profiles)):
        raise ValueError("Select each profile only once")
    if min(args.readiness_timeout, args.profile_timeout, args.cleanup_timeout) <= 0:
        raise ValueError("Timeouts must be positive")
    if args.log_dir.exists() and any(args.log_dir.iterdir()):
        raise ValueError(
            "Use a new or empty log directory; previous evidence is preserved"
        )
    args.log_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    runtime = args.runtime_image or f"ray-hive-auth-it:{run_id}"
    env = {k: v for k, v in os.environ.items() if k not in PROXIES}
    results = []
    code = 0
    owned_image = False
    runtime_metadata = None
    docker_ready = False
    try:
        command(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            args.log_dir / "docker.log",
            env=env,
            timeout=30,
        )
        docker_ready = True
        command(
            ["docker", "compose", "version", "--short"],
            args.log_dir / "compose-version.log",
            env=env,
            timeout=15,
        )
        command(
            ["docker", "image", "ls", "--filter", "dangling=true", "--no-trunc"],
            args.log_dir / "images-before.log",
            env=env,
            timeout=15,
        )
        if not args.runtime_image:
            command(
                [
                    "docker",
                    "build",
                    "--progress=plain",
                    "--label",
                    f"com.ray.hive-auth.run={run_id}",
                    "--build-arg",
                    f"RAY_IMAGE={args.ray_image}",
                    "--build-arg",
                    f"HIVE_AUTH_REQUIREMENTS={args.requirements}",
                    "-t",
                    runtime,
                    "-f",
                    str(HERE / "Dockerfile"),
                    str(SOURCE),
                ],
                args.log_dir / "build.log",
                env=env,
                timeout=1800,
            )
            owned_image = True
        runtime_metadata = image_info(
            runtime, args.log_dir / "runtime-identity.json", env
        )
        for profile in args.profiles:
            code, result = run_profile(
                args, profile, runtime, run_id, args.log_dir / profile
            )
            results.append(result)
            if code:
                break
    except (
        CommandFailed,
        OSError,
        subprocess.TimeoutExpired,
        KeyboardInterrupt,
        InterruptedError,
        ValueError,
    ) as exc:
        code = exc.code if isinstance(exc, CommandFailed) else 1
        print(
            f"Preparation failed: {type(exc).__name__}; see {args.log_dir}",
            file=sys.stderr,
        )
    finally:
        completed = {r["profile"] for r in results}
        results.extend(
            {"profile": p, "state": "not_run"}
            for p in args.profiles
            if p not in completed
        )
        if owned_image:
            try:
                remove_owned_image(runtime, runtime_metadata, run_id, args.log_dir, env)
            except (
                CommandFailed,
                OSError,
                subprocess.TimeoutExpired,
                InterruptedError,
                KeyboardInterrupt,
                ValueError,
            ) as exc:
                code = code or (exc.code if isinstance(exc, CommandFailed) else 1)
        if docker_ready:
            try:
                remove_owned_dangling(run_id, args.log_dir, env)
                command(
                    [
                        "docker",
                        "image",
                        "ls",
                        "--filter",
                        "dangling=true",
                        "--no-trunc",
                    ],
                    args.log_dir / "images-after.log",
                    env=env,
                    timeout=15,
                )
            except (
                CommandFailed,
                OSError,
                subprocess.TimeoutExpired,
                ValueError,
                InterruptedError,
                KeyboardInterrupt,
            ) as exc:
                code = code or (exc.code if isinstance(exc, CommandFailed) else 1)
        (args.log_dir / "result.json").write_text(
            json.dumps(
                {"run_id": run_id, "profiles": results, "exit_code": code}, indent=2
            )
            + "\n"
        )
    return code


if __name__ == "__main__":

    def interrupted(signum, frame):
        raise InterruptedError(f"Interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
