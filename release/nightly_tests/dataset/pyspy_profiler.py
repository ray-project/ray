"""Minimal driver-side py-spy launcher for Ray Data release tests.

Enabled by setting the ``PYSPY_ENABLED=1`` environment variable. When enabled,
``start()`` records a speedscope-format profile of the current process (the
driver, which also runs the StreamingExecutor scheduler thread) and ``stop()``
writes it to ``pyspy_driver.speedscope.json`` in the chosen output directory.

If ``PYSPY_S3_DEST`` is also set (e.g. ``s3://my-bucket/run-abc/``), the
profile and its log are uploaded there after ``stop()`` finishes. When unset,
artifacts stay local.

Worker-node profiling is intentionally not included — the goal of this helper
is to make the scheduler-thread profile reproducible in the public release
tests; richer profiling lives in downstream tooling.
"""

import os
import shutil
import signal
import subprocess
from typing import Optional


PYSPY_RATE = int(os.environ.get("PYSPY_RATE", "100"))


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

    Args:
        outdir: Directory to write ``pyspy_driver.speedscope.json`` and
            ``pyspy_driver.log`` into. Created if missing.
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
    _output_path = output_path
    _log_path = log_path

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

    _log_file = open(log_path, "w")
    _log_file.write(f"cmd: {' '.join(cmd)}\n")
    _log_file.write(f"driver pid: {pid}\n")
    _log_file.write(f"output_path: {output_path}\n")
    _log_file.flush()

    _proc = subprocess.Popen(cmd, stdout=_log_file, stderr=_log_file)
    _log_file.write(f"py-spy pid: {_proc.pid}\n")
    _log_file.flush()

    print(f"py-spy profiling started on driver (pid {pid}) -> {output_path}")


def _upload_to_s3(local_paths) -> None:
    """Upload each path under PYSPY_S3_DEST. No-op if the env var is unset.

    PYSPY_S3_DEST should be an ``s3://bucket/prefix/`` URL. Each local file is
    uploaded to ``<prefix>/<basename>``. Failures are logged and swallowed so
    a transient upload error doesn't fail the benchmark run.
    """
    dest = os.environ.get("PYSPY_S3_DEST")
    if not dest:
        return
    if not dest.startswith("s3://"):
        print(f"PYSPY_S3_DEST is not an s3:// URL: {dest!r}; skipping upload")
        return

    try:
        import boto3
    except ImportError:
        print("boto3 not installed; skipping PYSPY_S3_DEST upload")
        return

    # Parse `s3://bucket/optional/prefix/` -> (bucket, prefix)
    without_scheme = dest[len("s3://") :]
    bucket, _, prefix = without_scheme.partition("/")
    prefix = prefix.rstrip("/")

    s3 = boto3.client("s3")
    for path in local_paths:
        if not path or not os.path.exists(path):
            continue
        key = f"{prefix}/{os.path.basename(path)}" if prefix else os.path.basename(path)
        try:
            print(f"Uploading {path} -> s3://{bucket}/{key}")
            s3.upload_file(path, bucket, key)
        except Exception as e:  # noqa: BLE001 — best-effort upload
            print(f"Failed to upload {path}: {e}")


def stop(timeout: float = 15.0) -> None:
    """SIGINT py-spy, wait for it to flush its output, and (if configured)
    upload artifacts to ``PYSPY_S3_DEST``. No-op if py-spy was never started.
    """
    global _proc, _log_file, _output_path, _log_path

    if _proc is None:
        return

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

    # Best-effort upload — must happen after the log file is closed so its
    # contents are fully flushed before we read it from disk.
    _upload_to_s3([_output_path, _log_path])
    _output_path = None
    _log_path = None
