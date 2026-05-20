"""Minimal driver-side py-spy launcher for Ray Data release tests.

Enabled by setting the ``PYSPY_ENABLED=1`` environment variable. When enabled,
``start()`` records a speedscope-format profile of the current process (the
driver, which also runs the StreamingExecutor scheduler thread) and ``stop()``
writes it to ``pyspy_driver.speedscope.json`` in the chosen output directory.

When ``stop()`` is called with an ``s3_prefix``, the profile and its log are
uploaded under ``s3://<S3_BUCKET>/<s3_prefix>/``.
"""

import os
import shutil
import signal
import subprocess
from typing import Optional


PYSPY_RATE = int(os.environ.get("PYSPY_RATE", "100"))

# Hardcoded telemetry bucket — same one rayturbo's profiling stack uploads to.
S3_BUCKET = "anyscale-staging-data-cld-kvedzwag2qa8i5bjxuevf5i7"


_proc: Optional[subprocess.Popen] = None
_log_file = None
_output_path: Optional[str] = None
_log_path: Optional[str] = None


def is_enabled() -> bool:
    return os.environ.get("PYSPY_ENABLED") == "1"


def _ensure_permissions() -> None:
    """Best-effort relax of ``kernel.yama.ptrace_scope`` so py-spy can attach to
    the current process. Falls back to ``chmod u+s`` on the py-spy binary.
    Both attempts swallow failures because some hosts disallow sudo and the
    caller may already have sufficient permissions.
    """
    try:
        subprocess.run(
            ["sudo", "sysctl", "-w", "kernel.yama.ptrace_scope=0"],
            check=True,
            capture_output=True,
        )
        return
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    pyspy_path = shutil.which("py-spy")
    if pyspy_path:
        try:
            subprocess.run(
                ["sudo", "chmod", "u+s", pyspy_path],
                check=True,
                capture_output=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass


def start(outdir: str) -> None:
    """Launch py-spy on the driver process (no-op if PYSPY_ENABLED is unset).

    Best-effort: if py-spy isn't installed or fails to spawn, log and continue
    without crashing the benchmark.
    """
    global _proc, _log_file, _output_path, _log_path

    if not is_enabled():
        return
    if _proc is not None:
        raise RuntimeError("py-spy is already running")

    _ensure_permissions()
    os.makedirs(outdir, exist_ok=True)

    pid = os.getpid()
    output_path = os.path.join(outdir, "pyspy_driver.speedscope.json")
    log_path = os.path.join(outdir, "pyspy_driver.log")

    cmd = [
        "py-spy",
        "record",
        "-p",
        str(pid),
        "-o",
        output_path,
        "-f",
        "speedscope",
        "-r",
        str(PYSPY_RATE),
        "--nonblocking",
        "--subprocesses",
    ]

    log_file = open(log_path, "w")
    log_file.write(f"cmd: {' '.join(cmd)}\n")
    log_file.write(f"driver pid: {pid}\n")
    log_file.write(f"output_path: {output_path}\n")
    log_file.flush()

    try:
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
    except (FileNotFoundError, OSError) as e:
        log_file.write(f"failed to launch py-spy: {e}\n")
        log_file.close()
        print(f"py-spy failed to launch ({e}); continuing without profiling")
        return

    _output_path = output_path
    _log_path = log_path
    _log_file = log_file
    _proc = proc
    log_file.write(f"py-spy pid: {proc.pid}\n")
    log_file.flush()
    print(f"py-spy profiling started on driver (pid {pid}) -> {output_path}")


def _upload_to_s3(s3_prefix: str, local_paths) -> None:
    """Upload each path under ``s3://S3_BUCKET/<s3_prefix>/<basename>``.

    Failures are logged and swallowed so a transient S3 problem doesn't fail
    the benchmark run.
    """
    try:
        import boto3
    except ImportError:
        print("boto3 not installed; skipping py-spy artifact upload")
        return

    prefix = s3_prefix.strip("/")
    s3 = boto3.client("s3")
    for path in local_paths:
        if not path or not os.path.exists(path):
            continue
        key = f"{prefix}/{os.path.basename(path)}" if prefix else os.path.basename(path)
        try:
            print(f"Uploading {path} -> s3://{S3_BUCKET}/{key}")
            s3.upload_file(path, S3_BUCKET, key)
        except Exception as e:  # noqa: BLE001 — best-effort upload
            print(f"Failed to upload {path}: {e}")


def stop(s3_prefix: Optional[str] = None, timeout: float = 15.0) -> None:
    """SIGINT py-spy, wait for it to flush its output, and (if ``s3_prefix`` is
    given) upload artifacts to ``s3://S3_BUCKET/<s3_prefix>/``. Safe to call
    even if py-spy was never started or failed to launch.
    """
    global _proc, _log_file, _output_path, _log_path

    output_path = _output_path
    log_path = _log_path

    try:
        if _proc is not None:
            try:
                _proc.send_signal(signal.SIGINT)
                _proc.wait(timeout=timeout)
                msg = f"py-spy exited with code {_proc.returncode}"
                print(msg)
                if _log_file:
                    _log_file.write(msg + "\n")
            except subprocess.TimeoutExpired:
                msg = f"py-spy did not exit in {timeout}s, killing"
                print(msg)
                _proc.kill()
                if _log_file:
                    _log_file.write(msg + "\n")
            except ProcessLookupError:
                msg = "py-spy already exited"
                print(msg)
                if _log_file:
                    _log_file.write(msg + "\n")
    finally:
        if _log_file is not None:
            _log_file.flush()
            _log_file.close()
            _log_file = None
        _proc = None
        _output_path = None
        _log_path = None

    if s3_prefix and output_path:
        _upload_to_s3(s3_prefix, [output_path, log_path])
