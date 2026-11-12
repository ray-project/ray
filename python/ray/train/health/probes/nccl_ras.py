"""Probes of the NCCL hang detector on ``ray.train.health``.

NCCL ships a Reliability/Availability/Serviceability (RAS) subsystem (NCCL
>= 2.24) that runs a monitoring thread inside every NCCL process. Those threads
form a peer mesh that tracks each communicator's ranks and their collective
operation counts. ``NCCLRASProbe`` queries it with ``ncclras`` on a node, and
the other probes capture what a confirmed hang is diagnosed with: NCCL's text
report, ``nvidia-smi``, the PyTorch Flight Recorder and the stack traces.

They replace the queries of ``NCCLRASCallback``, which stays until the health
version of the hang detector is complete.
"""
import os
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

from ray.train.health.probe import NodeProbe, ProbeResult, WorkerProbe

# NCCL's variable for the address of its RAS service.
NCCL_RAS_ADDR_ENV_VAR = "NCCL_RAS_ADDR"

# Timeouts of the queries, under the on-demand probe timeout.
NCCL_RAS_QUERY_TIMEOUT_S: float = 8.0  # ncclras's own default -t is 5
NVIDIA_SMI_TIMEOUT_S: float = 30.0
STACK_DUMP_TIMEOUT_S: float = 30.0

_PY_SPY_NOT_INSTALLED = "py-spy not installed"
_PASSWORDLESS_SUDO_CHECK_TIMEOUT_S = 5.0

# Whether `sudo` runs without a password, checked once per process.
_passwordless_sudo: Optional[bool] = None


# ----------------------------------------------------------------------
# ncclras
# ----------------------------------------------------------------------
def parse_ras_addr(addr: str) -> Tuple[str, int]:
    """Parse an ``NCCL_RAS_ADDR`` value into ``(host, port)``.

    Handles ``host:port`` and bracketed IPv6, such as ``[::1]:28028``.

    Args:
        addr: The ``NCCL_RAS_ADDR`` value.

    Returns:
        ``(host, port)``.

    Raises:
        ValueError: If ``addr`` has no valid port.
    """
    addr = addr.strip()
    if addr.startswith("["):
        end = addr.index("]")
        host, rest = addr[1:end], addr[end + 1 :]
        return host, int(rest[1:])
    host, _, port = addr.rpartition(":")
    return host, int(port)


def run_ncclras(
    binary_path: str,
    timeout_s: float,
    fmt: str = "json",
    ras_addr: Optional[str] = None,
) -> Dict[str, Any]:
    """Run ``ncclras`` and return its output.

    Args:
        binary_path: Path to the ``ncclras`` client binary.
        timeout_s: Timeout of the query, passed to ``ncclras`` too.
        fmt: ``ncclras`` output format: ``"json"`` or ``"text"``.
        ras_addr: The ``NCCL_RAS_ADDR`` to query, if not ``ncclras``'s
            default.

    Returns:
        ``{"ok": True, "stdout": ...}``, or ``{"ok": False, "reason": ...}``
        with the ``stderr`` of a failed run. ``reason`` is
        ``"binary_not_found"`` for a missing binary, and
        ``"unsupported_f_option"`` for one older than NCCL 2.28.
    """
    cmd = [binary_path, "-f", fmt, "-t", str(int(timeout_s))]
    if ras_addr:
        host, port = parse_ras_addr(ras_addr)
        cmd += ["-h", host, "-p", str(port)]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except FileNotFoundError:
        return {"ok": False, "reason": "binary_not_found"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": f"timed out ({timeout_s})"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": f"error: {e}"}

    if proc.returncode != 0:
        stderr = proc.stderr or ""
        if "invalid option -- 'f'" in stderr:
            reason = "unsupported_f_option"
        else:
            reason = f"exit_{proc.returncode}"
        return {"ok": False, "reason": reason, "stderr": stderr[:500]}

    return {"ok": True, "stdout": proc.stdout}


class RASQueryError(Exception):
    """A ``ncclras`` query gave no output.

    Args:
        reason: Short machine-readable cause, such as ``binary_not_found`` or
            ``exit_1``.
        message: Human-readable explanation. Defaults to ``reason``.
        stderr: The end of the binary's stderr, if any.
        fatal: Whether no retry can fix it, such as a missing binary or one
            too old for ``-f``.
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


def ras_query_error(result: Dict[str, Any], binary_path: str) -> RASQueryError:
    """Turn a failed ``run_ncclras()`` result into a ``RASQueryError``.

    Args:
        result: What ``run_ncclras()`` returned, with ``ok`` false.
        binary_path: Path to the ``ncclras`` client binary.

    Returns:
        The error, ``fatal`` for a missing binary or one too old for ``-f``.
    """
    reason = result["reason"]
    if reason == "binary_not_found":
        return RASQueryError(
            reason, f"binary {binary_path!r} not found on the node.", fatal=True
        )
    if reason == "unsupported_f_option":
        return RASQueryError(
            reason,
            f"binary {binary_path!r} rejected the `-f` format flag, "
            "which requires NCCL 2.28+",
            fatal=True,
        )
    return RASQueryError(reason, stderr=result.get("stderr"))


# ----------------------------------------------------------------------
# Diagnostics
# ----------------------------------------------------------------------
def run_nvidia_smi(timeout_s: float) -> ProbeResult:
    """Snapshot ``nvidia-smi -q`` on this node.

    Args:
        timeout_s: Timeout of the ``nvidia-smi`` subprocess.

    Returns:
        The report as ``detail``, or the ``error`` saying why there is none.
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
    """Dump the PyTorch Flight Recorder buffer of this worker process.

    ProcessGroupNCCL records into its own buffer, which only
    ``_dump_nccl_trace_json`` dumps. ``_dump_fr_trace_json`` dumps the generic
    buffer that backends like Gloo record into, so it's only the fallback for
    torch builds without NCCL. The buffer has to be armed before the process
    group is created (``TORCH_FR_BUFFER_SIZE``); otherwise the dump holds no
    collectives.

    Returns:
        The dump as JSON in ``detail``, or the ``error`` saying why there is
        none.
    """
    try:
        from torch._C import _distributed_c10d as c10d
    except ImportError as e:
        return ProbeResult(error=e)

    try:
        # Torch only binds the NCCL dump in builds with NCCL.
        dump_trace_json = getattr(c10d, "_dump_nccl_trace_json", None)
        if dump_trace_json is None:
            dump_trace_json = c10d._dump_fr_trace_json
        trace_bytes = dump_trace_json()
    except Exception as e:  # noqa: BLE001
        return ProbeResult(error=e)
    return ProbeResult(detail=trace_bytes.decode("utf-8", errors="replace"))


def _can_passwordless_sudo(timeout_s: float) -> bool:
    """Whether ``sudo`` runs without a password, checked once per process.

    Args:
        timeout_s: Timeout of the check, capped at 5s.

    Returns:
        Whether ``sudo -n true`` succeeded.
    """
    global _passwordless_sudo
    if _passwordless_sudo is None:
        try:
            proc = subprocess.run(
                ["sudo", "-n", "true"],
                capture_output=True,
                timeout=min(timeout_s, _PASSWORDLESS_SUDO_CHECK_TIMEOUT_S),
            )
            _passwordless_sudo = proc.returncode == 0
        except Exception:  # noqa: BLE001
            _passwordless_sudo = False
    return _passwordless_sudo


def _run_py_spy(cmd: List[str], timeout_s: float) -> Tuple[Optional[str], str]:
    """Run one ``py-spy dump`` command.

    Args:
        cmd: The command line, with any ``sudo`` prefix.
        timeout_s: Timeout of the subprocess.

    Returns:
        ``(dump, "")``, or ``(None, reason)`` when it failed.
    """
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except FileNotFoundError:
        return None, _PY_SPY_NOT_INSTALLED
    except subprocess.TimeoutExpired:
        return None, "py-spy timed out"
    except Exception as e:  # noqa: BLE001
        return None, f"py-spy error: {e}"
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout, ""
    return None, (proc.stderr or "").strip() or f"py-spy exited {proc.returncode}"


def dump_stack_trace(timeout_s: float) -> str:
    """Dump the native and Python stacks of this worker process.

    Args:
        timeout_s: Budget of the whole dump, shared by the unprivileged
            ``py-spy dump``, the sudo check and the sudo retry.

    Returns:
        The ``py-spy`` dump, or, when ``py-spy`` can't run, every thread's
        Python stack after the reason why.
    """
    deadline = time.monotonic() + timeout_s
    py_spy_cmd = ["py-spy", "dump", "--pid", str(os.getpid()), "--native"]
    stdout, stderr = _run_py_spy(py_spy_cmd, timeout_s)
    if stdout is not None:
        return stdout

    # Restricted ptrace (yama ptrace_scope=1 without CAP_SYS_PTRACE) stops an
    # unprivileged py-spy from tracing its parent; passwordless sudo lifts that.
    # The retry only runs with budget left, so the caller's timeout still holds.
    remaining_s = deadline - time.monotonic()
    if (
        stderr != _PY_SPY_NOT_INSTALLED
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

    # Threads not started through `threading`, such as C-created ones, have no
    # name here.
    thread_names = {t.ident: t.name for t in threading.enumerate()}
    lines = [f"[py-spy unavailable: {stderr}; Python-only traceback follows]"]
    for thread_id, frame in sys._current_frames().items():
        name = thread_names.get(thread_id, "unknown")
        lines.append(f"\n# Thread {thread_id} ({name})")
        lines.append("".join(traceback.format_stack(frame)))
    return "\n".join(lines)


# ----------------------------------------------------------------------
# Probes
# ----------------------------------------------------------------------
class NCCLRASProbe(NodeProbe):
    """Queries NCCL RAS on its node with ``ncclras -f json``, for the report
    in ``detail``. Every node sees the whole RAS mesh. After a fatal error,
    such as a missing binary, it returns that error without querying again.

    Args:
        ras_addr: The workers' ``NCCL_RAS_ADDR``, if set. Defaults to this
            process's.
        binary_path: Path to the ``ncclras`` client binary.
    """

    fmt = "json"

    def __init__(self, ras_addr: Optional[str] = None, binary_path: str = "ncclras"):
        self._ras_addr = ras_addr or os.environ.get(NCCL_RAS_ADDR_ENV_VAR)
        self._binary_path = binary_path
        self._fatal_error: Optional[RASQueryError] = None

    def poll(self) -> ProbeResult:
        if self._fatal_error is not None:
            return ProbeResult(error=self._fatal_error)
        result = run_ncclras(
            self._binary_path, NCCL_RAS_QUERY_TIMEOUT_S, self.fmt, self._ras_addr
        )
        if result["ok"]:
            return ProbeResult(detail=result["stdout"])
        error = ras_query_error(result, self._binary_path)
        if error.fatal:
            self._fatal_error = error
        return ProbeResult(error=error)


class NCCLRASTextProbe(NCCLRASProbe):
    """Queries NCCL RAS on its node for its human-readable report, with
    ``ncclras -f text``."""

    fmt = "text"


class NvidiaSmiProbe(NodeProbe):
    """Snapshots the node's GPUs with ``nvidia-smi -q``, in ``detail``."""

    def poll(self) -> ProbeResult:
        return run_nvidia_smi(NVIDIA_SMI_TIMEOUT_S)


class FlightRecorderProbe(WorkerProbe):
    """Dumps the worker's PyTorch Flight Recorder buffer, in ``detail``."""

    def poll(self) -> ProbeResult:
        return dump_flight_recorder()


class StackTraceProbe(WorkerProbe):
    """Dumps the worker's native and Python stacks, in ``detail``."""

    def poll(self) -> ProbeResult:
        return ProbeResult(detail=dump_stack_trace(STACK_DUMP_TIMEOUT_S))
