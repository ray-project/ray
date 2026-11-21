"""What the NCCL hang detector's probes run: ``ncclras``, ``nvidia-smi``,
``py-spy`` and the PyTorch Flight Recorder dump. Copied in order from
``NCCLRASCallback``'s module, ``ray.train.v2._internal.callbacks.nccl_ras``,
which keeps its own copies until it's deleted."""
import os
import subprocess
import threading
import time
from typing import List, Optional, Tuple

from ray.train.health.nccl_ras.constants import (
    PASSWORDLESS_SUDO_CHECK_TIMEOUT_S,
    PY_SPY_NOT_INSTALLED,
)
from ray.train.health.probe import ProbeResult


def parse_ras_addr(addr: str) -> Tuple[str, int]:
    """Parse an ``NCCL_RAS_ADDR`` value (``host:port``) into ``(host, port)``.

    Handles bare hosts (default port), ``host:port``, and bracketed IPv6 such
    as ``[::1]:28028``.

    Args:
        addr: The ``NCCL_RAS_ADDR`` value to parse.

    Returns:
        Tuple of ``(host, port)``
    """
    addr = addr.strip()
    if addr.startswith("["):  # [ipv6](:port)?
        end = addr.index("]")
        host, rest = addr[1:end], addr[end + 1 :]
        port = int(rest[1:])
        return host, port
    else:
        host, _, port = addr.rpartition(":")
        return host, int(port)


def run_ncclras(fmt: str, timeout_s: float, ras_addr: Optional[str] = None) -> str:
    """Query NCCL RAS on this node with ``ncclras``.

    Args:
        fmt: The output format: ``"json"`` or ``"text"``.
        timeout_s: Timeout of the query, passed to ``ncclras`` too.
        ras_addr: The ``NCCL_RAS_ADDR`` to query, if not ``ncclras``'s default.

    Returns:
        The report.

    Raises:
        RASQueryError: If the query gave no report.
    """
    cmd = ["ncclras", "-f", fmt, "-t", str(int(timeout_s))]
    if ras_addr:
        host, port = parse_ras_addr(ras_addr)
        cmd += ["-h", host, "-p", str(port)]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except FileNotFoundError:
        raise RASQueryError(
            "binary_not_found", "binary 'ncclras' not found on the node.", fatal=True
        ) from None
    except subprocess.TimeoutExpired:
        raise RASQueryError(f"timed out ({timeout_s})") from None

    if proc.returncode != 0:
        stderr = proc.stderr or ""
        if "invalid option -- 'f'" in stderr:
            raise RASQueryError(
                "unsupported_f_option",
                "binary 'ncclras' rejected the `-f` format flag, which requires "
                "NCCL 2.28+",
                fatal=True,
            )
        raise RASQueryError(f"exit_{proc.returncode}", stderr=stderr[:500])
    if not proc.stdout.strip():
        raise RASQueryError("empty_output")
    return proc.stdout


class RASQueryError(Exception):
    """A ``ncclras`` query produced no usable output.

    Args:
        reason: Short machine-readable cause, e.g. ``binary_not_found``,
            ``query_timeout``, ``exit_1``, ``unparseable_json``.
        message: Human-readable explanation; defaults to ``reason``.
        stderr: Tail of the binary's stderr, when there was one.
        fatal: True for run-wide misconfigurations no retry can fix (binary
            missing, binary too old for ``-f``); the poller stops on these.
    """

    def __init__(
        self,
        reason: str,
        message: Optional[str] = None,
        stderr: Optional[str] = None,
        fatal: bool = False,
    ):
        text = message or reason
        if stderr:
            text = f"{text} (stderr: {stderr.strip()})"
        super().__init__(text)
        self.reason = reason
        self.stderr = stderr
        self.fatal = fatal


_passwordless_sudo: Optional[bool] = None


def _can_passwordless_sudo(timeout_s: float) -> bool:
    """Return whether ``sudo`` runs without a password, checked once per process.

    ``-n`` makes ``sudo`` fail instead of prompting.

    Args:
        timeout_s: Timeout for the check, capped at 5 s.

    Returns:
        Whether ``sudo -n true`` succeeded.
    """
    global _passwordless_sudo
    if _passwordless_sudo is None:
        try:
            proc = subprocess.run(
                ["sudo", "-n", "true"],
                capture_output=True,
                timeout=min(timeout_s, PASSWORDLESS_SUDO_CHECK_TIMEOUT_S),
            )
            _passwordless_sudo = proc.returncode == 0
        except Exception:  # noqa: BLE001
            _passwordless_sudo = False
    return _passwordless_sudo


def _run_py_spy(cmd: List[str], timeout_s: float) -> Tuple[Optional[str], str]:
    """Run one ``py-spy dump`` command.

    Args:
        cmd: The full command line, including any ``sudo`` prefix.
        timeout_s: Timeout for the subprocess.

    Returns:
        ``(dump, "")`` on success, otherwise ``(None, reason)``.
    """
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except FileNotFoundError:
        return None, PY_SPY_NOT_INSTALLED
    except subprocess.TimeoutExpired:
        return None, "py-spy timed out"
    except Exception as e:  # noqa: BLE001
        return None, f"py-spy error: {e}"
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout, ""
    return None, (proc.stderr or "").strip() or f"py-spy exited {proc.returncode}"


def dump_stack_trace(pyspy_timeout_s: float) -> str:
    """Dump native + Python stacks of the current (worker) process.

    Args:
        pyspy_timeout_s: Budget for the whole dump, shared by the unprivileged
            ``py-spy dump``, the sudo check and the sudo retry.

    Returns:
        The captured stack trace, or a Python-only traceback (prefixed with the
        reason py-spy was skipped) when py-spy is unavailable.
    """
    deadline = time.monotonic() + pyspy_timeout_s
    py_spy_cmd = ["py-spy", "dump", "--pid", str(os.getpid()), "--native"]
    stdout, stderr = _run_py_spy(py_spy_cmd, pyspy_timeout_s)
    if stdout is not None:
        return stdout

    # Restricted ptrace (yama ptrace_scope=1 without CAP_SYS_PTRACE) stops an
    # unprivileged py-spy from tracing its parent; passwordless sudo lifts that.
    # The retry only runs with budget left, so the caller's timeout still holds.
    remaining_s = deadline - time.monotonic()
    if (
        stderr != PY_SPY_NOT_INSTALLED
        and remaining_s > 0
        and _can_passwordless_sudo(remaining_s)
    ):
        remaining_s = deadline - time.monotonic()
        if remaining_s > 0:
            sudo_cmd = ["sudo", "-n", "env", f"PATH={os.environ.get('PATH', '')}"]
            stdout, sudo_stderr = _run_py_spy(sudo_cmd + py_spy_cmd, remaining_s)
            if stdout is not None:
                return stdout
            stderr = f"{stderr}; with sudo: {sudo_stderr}"

    # Python-only fallback: dump every thread's stack (cannot show C/C++ trace).
    import sys
    import traceback

    # Threads not started through ``threading`` (for example C-created ones)
    # have no name here.
    thread_names = {t.ident: t.name for t in threading.enumerate()}
    lines = [f"[py-spy unavailable: {stderr}; Python-only traceback follows]"]
    for thread_id, frame in sys._current_frames().items():
        name = thread_names.get(thread_id, "unknown")
        lines.append(f"\n# Thread {thread_id} ({name})")
        lines.append("".join(traceback.format_stack(frame)))
    return "\n".join(lines)


def run_nvidia_smi(timeout_s: float) -> ProbeResult:
    """Snapshot `nvidia-smi -q` on the current (worker) node.

    Args:
        timeout_s: Timeout for the ``nvidia-smi`` subprocess.

    Returns:
        The report as ``detail`` on success, otherwise ``error`` says why there
        is none.
    """
    try:
        proc = subprocess.run(
            ["nvidia-smi", "-q"], capture_output=True, text=True, timeout=timeout_s
        )
    except subprocess.TimeoutExpired:
        return ProbeResult(
            error=TimeoutError(
                f"`nvidia-smi -q` timed out after {timeout_s:.0f}s, which usually "
                "means the driver is itself stuck"
            )
        )
    except Exception as e:  # noqa: BLE001
        return ProbeResult(error=e)

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        return ProbeResult(
            error=RuntimeError(
                f"`nvidia-smi -q` exited {proc.returncode} (stderr: {stderr[:500]})"
            )
        )

    return ProbeResult(detail=proc.stdout)


def dump_flight_recorder() -> ProbeResult:
    """Dump the PyTorch Flight Recorder buffer of the current (worker) process.

    The buffer has to be armed *before* the process group is created (see
    ``TORCH_FR_BUFFER_SIZE``); it cannot be switched on retroactively in a hung
    process, in which case the dump holds no collectives.

    Returns:
        The dump as a JSON string, or the error saying why there is no dump,
        which is written into the rank's file so a gap is never silent.
    """
    try:
        from torch._C import _distributed_c10d as c10d
    except ImportError as e:
        return ProbeResult(error=e)

    try:
        # ProcessGroupNCCL records into its own buffer, which only
        # `_dump_nccl_trace_json` dumps. `_dump_fr_trace_json` dumps the generic
        # buffer, so it's only the fallback for torch builds without NCCL.
        # Neither takes a timeout.
        dump = getattr(c10d, "_dump_nccl_trace_json", None) or c10d._dump_fr_trace_json
        trace_bytes = dump()
        trace_json = trace_bytes.decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        return ProbeResult(error=e)

    return ProbeResult(detail=trace_json)
