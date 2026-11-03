"""Run each selected security profile once using an explicitly prepared image."""

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


class CommandFailed(RuntimeError):
    def __init__(self, stage, code):
        super().__init__(f"{stage} failed (exit {code})")
        self.code = code


FAILURES = (
    CommandFailed,
    OSError,
    subprocess.TimeoutExpired,
    ValueError,
    ElementTree.ParseError,
    KeyboardInterrupt,
)


def command(args, log, *, env, timeout, check=True):
    """Bound a process group and log its actual status, without guessing its source."""
    with log.open("ab") as stream:
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
        finally:
            stream.write(f"\nCommand exit code: {process.returncode}\n".encode())
    if check and code:
        raise CommandFailed(log.stem, code)
    return code


def validate_results(path, profile):
    """Require the exact case list, with no missing, extra, skipped or failed cases."""
    cases = list(ElementTree.parse(path).getroot().iter("testcase"))
    names = [case.attrib["name"] for case in cases]
    expected = {f"test_hive_auth[{case}]" for case in CASES[profile]}
    if (
        len(names) != len(expected)
        or set(names) != expected
        or any(
            case.find(tag) is not None
            for case in cases
            for tag in ("failure", "error", "skipped")
        )
    ):
        raise ValueError("Selected profile did not pass its exact case list")
    return len(cases)


def wait_for_probe(compose, probe, directory, env, deadline):
    while time.monotonic() < deadline:
        try:
            code = command(
                compose
                + ["exec", "-T", "head", "python", "/opt/hive_auth/probe.py", probe],
                directory / f"ready-{probe}.log",
                env=env,
                timeout=min(30, max(1, deadline - time.monotonic())),
                check=False,
            )
        except subprocess.TimeoutExpired:
            code = 1
        if code == 0:
            return
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"{probe} readiness deadline exceeded")


def run_profile(args, profile, run_id):
    directory = args.log_dir / profile
    directory.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in PROXIES}
    project = f"ray-hive-auth-{run_id}-{profile.lower()}"
    env.update(
        HIVE_AUTH_RUNTIME_IMAGE=args.runtime_image,
        HIVE_AUTH_PROFILE=profile,
        HIVE_AUTH_SOURCE=os.environ.get("RAYCI_CHECKOUT_DIR", str(SOURCE)),
        HIVE_AUTH_HIVE_IMAGE=args.hive_image,
        HIVE_AUTH_PLACEHOLDER=secrets.token_urlsafe(16),
    )
    compose = ["docker", "compose", "-f", str(HERE / "compose.yaml"), "-p", project]
    deadline = time.monotonic() + args.profile_timeout
    ready = min(deadline, time.monotonic() + args.readiness_timeout)
    code = 0
    print(f"{profile}: project={project}; logs={directory}", flush=True)
    try:
        command(
            compose + ["up", "-d", "--no-build"],
            directory / "start.log",
            env=env,
            timeout=max(1, ready - time.monotonic()),
        )
        for probe in ("sql", "workers"):
            wait_for_probe(compose, probe, directory, env, ready)
        command(
            compose
            + [
                "exec",
                "-T",
                "head",
                "python",
                "-m",
                "pytest",
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
    except FAILURES as exc:
        code = exc.code if isinstance(exc, CommandFailed) else 1
        print(f"{profile}: {exc}", file=sys.stderr)
    finally:
        # up can fail after partially creating this unique project.
        cleanup = time.monotonic() + args.cleanup_timeout
        for stage, tail in (
            ("service", ["logs", "--no-color", "--tail", "200"]),
            ("copy", ["cp", "head:/events/.", str(directory / "artifacts")]),
            (
                "cleanup",
                ["down", "--volumes", "--remove-orphans", "--timeout", "20"],
            ),
        ):
            try:
                command(
                    compose + tail,
                    directory / f"{stage}.log",
                    env=env,
                    timeout=max(
                        1,
                        min(
                            30 if stage == "cleanup" else 10,
                            cleanup - time.monotonic(),
                        ),
                    ),
                )
            except FAILURES as exc:
                code = code or (exc.code if isinstance(exc, CommandFailed) else 1)
                print(f"{profile}: {stage}: {exc}", file=sys.stderr)
    if not code:
        try:
            count = validate_results(directory / "artifacts/results.xml", profile)
            print(f"{profile}: {count} passed; cleanup passed", flush=True)
        except FAILURES as exc:
            print(f"{profile}: results: {exc}", file=sys.stderr)
            code = 1
    print(f"{profile}: exit code {code}", flush=True)
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--profiles", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--hive-image", default="apache/hive:4.2.0")
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--readiness-timeout", type=int, default=300)
    parser.add_argument("--profile-timeout", type=int, default=1200)
    parser.add_argument("--cleanup-timeout", type=int, default=60)
    args = parser.parse_args(argv)
    if len(args.profiles) != len(set(args.profiles)):
        raise ValueError("Select each profile only once")
    if min(args.readiness_timeout, args.profile_timeout, args.cleanup_timeout) <= 0:
        raise ValueError("Timeouts must be positive")
    if args.log_dir.exists() and any(args.log_dir.iterdir()):
        raise ValueError("Use a new or empty log directory; preserve previous evidence")
    args.log_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    for index, profile in enumerate(args.profiles):
        code = run_profile(args, profile, run_id)
        if code:
            print(f"Not run: {args.profiles[index + 1:]}", flush=True)
            return code
    return 0


if __name__ == "__main__":

    def interrupted(signum, frame):
        raise InterruptedError(f"Interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
