"""fsync-honesty probe for REP-64 POC durability testing.

Single point: time how long `fsync()` actually takes on the host's
filesystem. The number tells you whether the substrate honours the
durability contract RocksDB (and Ray) expect.

Output is structured JSON. The script also self-labels the substrate
("honest" / "suspicious" / "lying" / "macos_fsync_only_no_fullfsync")
so a reviewer reading a result file from a collaborator's VM knows
immediately whether to trust durability claims drawn from it.

Why this matters: on this REP-64 POC's LinkedIn dev VM, fsync(4 KiB)
returns in ~0.7 us. Real non-volatile flush takes 30 us – 15 ms
depending on media. Anything sub-microsecond is the syscall acking
without committing — the host (or VM) is buffering. A POC built on
that substrate would test the wrong thing.

On macOS, `fsync()` is documented as flushing data to the storage
controller but NOT to permanent media. To force an actual flush you
need `fcntl(fd, F_FULLFSYNC)`. We probe both and report separately.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Optional


# F_FULLFSYNC value comes from <sys/fcntl.h> on macOS. It's stable.
_F_FULLFSYNC = 51


def _full_fsync_macos(fd: int) -> None:
    """Issue F_FULLFSYNC via fcntl on macOS. Raises OSError on failure."""
    libc = ctypes.CDLL("libc.dylib", use_errno=True)
    rc = libc.fcntl(ctypes.c_int(fd), ctypes.c_int(_F_FULLFSYNC),
                    ctypes.c_int(0))
    if rc != 0:
        err = ctypes.get_errno()
        raise OSError(err, f"fcntl(F_FULLFSYNC) failed: errno={err}")


def _percentiles(samples: list[int]) -> dict:
    s = sorted(samples)
    n = len(s)
    if n == 0:
        return {"n": 0}
    return {
        "n": n,
        "p50_us": round(s[int(0.50 * (n - 1))] / 1000.0, 3),
        "p99_us": round(s[int(0.99 * (n - 1))] / 1000.0, 3),
        "p999_us": round(s[int(0.999 * (n - 1))] / 1000.0, 3),
        "min_us": round(s[0] / 1000.0, 3),
        "max_us": round(s[-1] / 1000.0, 3),
        "mean_us": round(statistics.mean(s) / 1000.0, 3),
    }


def _time_op(fn) -> int:
    t0 = time.perf_counter_ns()
    fn()
    return time.perf_counter_ns() - t0


def probe(target_dir: str, n: int, write_size: int,
          sync_call: str) -> dict:
    """Run `n` probes of one specific sync call against a file in `target_dir`.

    sync_call in {"fsync", "fdatasync", "f_fullfsync"}.
    """
    fd = os.open(os.path.join(target_dir, f"rep64-probe-{sync_call}.tmp"),
                 os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    payload = b"x" * write_size

    if sync_call == "fsync":
        sync_fn = lambda: os.fsync(fd)
    elif sync_call == "fdatasync":
        if not hasattr(os, "fdatasync"):
            os.close(fd)
            return {"available": False, "reason": "os.fdatasync not present"}
        sync_fn = lambda: os.fdatasync(fd)
    elif sync_call == "f_fullfsync":
        if platform.system() != "Darwin":
            os.close(fd)
            return {"available": False,
                    "reason": "F_FULLFSYNC is macOS-only"}
        sync_fn = lambda: _full_fsync_macos(fd)
    else:
        os.close(fd)
        raise ValueError(f"unknown sync_call: {sync_call}")

    samples_ns: list[int] = []
    for _ in range(n):
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, payload)
        samples_ns.append(_time_op(sync_fn))

    os.close(fd)
    os.unlink(os.path.join(target_dir, f"rep64-probe-{sync_call}.tmp"))
    return {"available": True, **_percentiles(samples_ns)}


def label_substrate(host_os: str, fsync_p50_us: float,
                    fullfsync_p50_us: Optional[float]) -> dict:
    """Decide whether the probed substrate is honest about durability.

    Heuristics:
      * If fsync p50 < 5 us, the syscall is acking from RAM. "lying".
      * On macOS, fsync alone is documented as not flushing media; this
        is "macos_fsync_only_no_fullfsync" and is expected. The honest
        signal there is F_FULLFSYNC.
      * If fsync p50 >= 30 us, fsync is doing real I/O — treat as
        "honest" (with a small "suspicious" band 5-30 us).
    """
    notes: list[str] = []
    if host_os == "Darwin":
        if fullfsync_p50_us is None:
            return {"verdict": "macos_no_fullfsync_data", "notes": notes}
        if fullfsync_p50_us >= 1000.0:  # >= 1 ms, plausible for real media
            verdict = "honest_via_fullfsync"
            notes.append(
                "macOS: standard fsync is documented as not flushing to "
                "permanent media; F_FULLFSYNC is the honest signal here."
            )
        elif fullfsync_p50_us >= 30.0:
            verdict = "suspicious_via_fullfsync"
            notes.append(
                "F_FULLFSYNC is unusually fast for macOS; possible if "
                "the disk has a battery-backed cache or if running in "
                "a VM that intercepts fcntl calls."
            )
        else:
            verdict = "lying"
            notes.append(
                "F_FULLFSYNC under 30 us on macOS strongly suggests the "
                "underlying storage is virtualized and not honouring the "
                "flush command."
            )
        if fsync_p50_us < 5.0:
            notes.append(
                "Plain fsync also returned in <5 us, which is consistent "
                "with macOS's documented fsync semantics."
            )
        return {"verdict": verdict, "notes": notes}

    # Linux (and other Unixes).
    if fsync_p50_us < 5.0:
        verdict = "lying"
        notes.append(
            "fsync p50 < 5 us on Linux means the syscall is being "
            "acknowledged before non-volatile commit. Most common cause "
            "is a virtualization layer (Hyper-V, Type-2 hypervisor, "
            "container with write-back caching) doing host-side "
            "buffering. Durability tests on this substrate will report "
            "false negatives — they will pass for the wrong reason."
        )
    elif fsync_p50_us < 30.0:
        verdict = "suspicious"
        notes.append(
            "fsync p50 in the 5-30 us range is unusually fast. "
            "Possible if the device has a battery-backed write cache "
            "(some enterprise NVMe / RAID controllers); also possible "
            "the device is partially virtualized. Treat results from "
            "this substrate with care."
        )
    else:
        verdict = "honest"
        notes.append(
            "fsync p50 >= 30 us is consistent with a real flush command "
            "reaching non-volatile media. Durability tests on this "
            "substrate are trustworthy."
        )
    return {"verdict": verdict, "notes": notes}


def collect_environment() -> dict:
    info = {
        "host_os": platform.system(),
        "host_kernel": platform.release(),
        "host_machine": platform.machine(),
        "host_python": platform.python_version(),
    }
    # Linux: hypervisor detection.
    if platform.system() == "Linux":
        try:
            with open("/proc/cpuinfo") as f:
                cpuinfo = f.read()
            for vendor in ("Microsoft", "VMware", "KVM", "Xen", "Bochs",
                           "QEMU"):
                if vendor in cpuinfo:
                    info["hypervisor"] = vendor
                    break
            if "hypervisor" not in info and "hypervisor" in cpuinfo:
                info["hypervisor"] = "unknown_generic"
        except OSError:
            pass
    return info


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--target-dir", default=tempfile.gettempdir(),
                   help="Directory to probe. Use the path the durability "
                        "tests will actually write to.")
    p.add_argument("--n", type=int, default=200,
                   help="Number of fsync calls to measure.")
    p.add_argument("--write-size", type=int, default=4096,
                   help="Bytes written before each fsync call.")
    p.add_argument("--output", default="-",
                   help="Path to write JSON; '-' for stdout.")
    args = p.parse_args()

    if not os.path.isdir(args.target_dir):
        print(f"ERROR: target dir does not exist: {args.target_dir}",
              file=sys.stderr)
        return 2

    started = datetime.now(timezone.utc).isoformat()
    env = collect_environment()

    fsync_result = probe(args.target_dir, args.n, args.write_size, "fsync")
    fdatasync_result = probe(args.target_dir, args.n, args.write_size,
                             "fdatasync")
    fullfsync_result = probe(args.target_dir, args.n, args.write_size,
                             "f_fullfsync")

    fsync_p50 = fsync_result.get("p50_us") if fsync_result.get("available") \
                else None
    full_p50 = fullfsync_result.get("p50_us") if fullfsync_result.get(
        "available") else None
    label = label_substrate(env["host_os"], fsync_p50 or 0.0, full_p50)

    out = {
        "harness": {
            "name": "rep-64-poc-fsync-probe",
            "schema_version": 1,
            "started_at": started,
            "params": {
                "target_dir": args.target_dir,
                "n": args.n,
                "write_size": args.write_size,
            },
            "environment": env,
        },
        "results": {
            "fsync": fsync_result,
            "fdatasync": fdatasync_result,
            "f_fullfsync": fullfsync_result,
        },
        "label": label,
    }
    text = json.dumps(out, indent=2)
    if args.output == "-":
        print(text)
    else:
        with open(args.output, "w") as f:
            f.write(text)
        print(f"wrote {args.output}", file=sys.stderr)

    # Print a one-line human summary on stderr so a runner can see the
    # verdict without parsing JSON.
    summary = f"verdict={label['verdict']}  fsync_p50_us={fsync_p50}"
    if full_p50 is not None:
        summary += f"  fullfsync_p50_us={full_p50}"
    print(summary, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
