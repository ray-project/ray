#!/usr/bin/env python3
"""Phase 4 kill-9 durability harness — orchestrator.

Spawns the writer, reads acks from its stdout, SIGKILLs it after a
chosen ack offset, then runs the verifier on the same DB and reports
how many acked keys are missing.

Two configurations exercise the same harness:

  --sync 1   production config (WriteOptions::sync = true). On an honest
             ext4 substrate, expected verdict: PASS (zero acked-but-missing).
  --sync 0   negative control. Without per-write fsync, RocksDB only
             flushes the WAL periodically. SIGKILL between an ack and
             the next periodic flush should lose data — verdict: FAIL.
             This proves the harness has the sensitivity to detect
             durability bugs.

The PLAN's original "tmpfs as control" framing assumed SIGKILL would
lose state on tmpfs; in practice tmpfs lives in the kernel and survives
SIGKILL of any user-space process, so it would silently pass. The
sync=true / sync=false matrix on the same substrate is a cleaner check.

Usage example:

  bazel build //rep-64-poc/harness/durability/kill9:writer \\
              //rep-64-poc/harness/durability/kill9:verifier
  python3 rep-64-poc/harness/durability/kill9/run_kill9.py \\
    --writer bazel-bin/rep-64-poc/harness/durability/kill9/writer \\
    --verifier bazel-bin/rep-64-poc/harness/durability/kill9/verifier \\
    --num-writes 1000 --sync 1 \\
    --output rep-64-poc/harness/durability/results/kill9_ext4_sync1.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--writer", required=True, help="path to writer binary")
    ap.add_argument("--verifier", required=True, help="path to verifier binary")
    ap.add_argument(
        "--db-path",
        default=None,
        help="path for the RocksDB; default = a fresh temp dir under $TMPDIR",
    )
    ap.add_argument("--num-writes", type=int, default=1000)
    ap.add_argument(
        "--sync",
        type=int,
        choices=(0, 1),
        default=1,
        help="WriteOptions::sync value to pass to the writer",
    )
    ap.add_argument(
        "--ghost-writes",
        type=int,
        default=0,
        help="(negative-control flag) ack the last N writes without "
        "actually calling Put. The harness MUST detect exactly N "
        "acked-but-missing keys.",
    )
    ap.add_argument(
        "--kill-after-ack",
        type=int,
        default=None,
        help="kill writer immediately after seeing this ack number "
        "(default: random in middle 80%% of the run)",
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--output", default=None, help="write the JSON report to this path too"
    )
    ap.add_argument(
        "--keep-db",
        action="store_true",
        help="don't remove the DB dir at the end (useful for debugging)",
    )
    args = ap.parse_args()

    rng = random.Random(args.seed)

    cleanup = False
    if args.db_path is None:
        args.db_path = tempfile.mkdtemp(prefix="rep64-kill9-")
        cleanup = not args.keep_db
    else:
        # If the caller pointed us at an explicit path, never auto-remove.
        # They probably want to inspect it.
        cleanup = False

    if args.kill_after_ack is None:
        lo = max(1, args.num_writes // 10)
        hi = max(lo + 1, args.num_writes - args.num_writes // 10)
        args.kill_after_ack = rng.randint(lo, hi)

    print(
        f"# kill9 orchestrator: db_path={args.db_path} "
        f"num_writes={args.num_writes} sync={args.sync} "
        f"kill_after_ack={args.kill_after_ack}",
        file=sys.stderr,
    )

    # Phase 1: spawn writer, collect acks until we hit the kill offset.
    writer_argv = [
        args.writer,
        args.db_path,
        str(args.num_writes),
        "k",
        str(args.sync),
    ]
    if args.ghost_writes > 0:
        writer_argv.append(str(args.ghost_writes))
    proc = subprocess.Popen(
        writer_argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    acks: list[int] = []
    killed_at: int | None = None
    writer_finished_naturally = False
    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("ack:"):
            n = int(line[4:])
            acks.append(n)
            if n >= args.kill_after_ack:
                # SIGKILL — uncatchable, no destructors run, no atexit hooks.
                # This is the moment of truth: did the data acked up to and
                # including ack:N actually make it to durable storage?
                proc.send_signal(signal.SIGKILL)
                killed_at = n
                break
        else:
            # Writer should not be printing anything else on stdout; if it
            # does, surface it so we can debug.
            print(f"# writer-stdout: {line}", file=sys.stderr)
    else:
        # Loop ended without break — writer exited cleanly before we killed it.
        writer_finished_naturally = True

    proc.wait()
    writer_stderr = (proc.stderr.read() if proc.stderr else "") or ""
    writer_exit = proc.returncode

    # Wait a beat to let any kernel-side I/O ride out. Should not be
    # necessary if fsync was honest, but leaves room for racey substrates.
    time.sleep(0.5)

    # Phase 2: spawn verifier on the same DB.
    verifier_proc = subprocess.run(
        [args.verifier, args.db_path, str(args.num_writes), "k"],
        capture_output=True,
        text=True,
    )
    if verifier_proc.returncode != 0:
        print(
            f"# verifier exited {verifier_proc.returncode}; stderr:\n"
            f"{verifier_proc.stderr}",
            file=sys.stderr,
        )

    found: set[int] = set()
    missing: set[int] = set()
    for raw_line in verifier_proc.stdout.splitlines():
        line = raw_line.strip()
        if line.startswith("found:"):
            found.add(int(line[6:]))
        elif line.startswith("missing:"):
            missing.add(int(line[8:]))

    ack_set = set(acks)
    acked_but_missing = sorted(ack_set - found)
    not_acked_but_found = sorted(found - ack_set)

    verdict = "PASS" if len(acked_but_missing) == 0 else "FAIL"

    report = {
        "schema_version": 1,
        "host": {
            "node": platform.node(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "config": {
            "db_path": args.db_path,
            "num_writes": args.num_writes,
            "sync": args.sync,
            "ghost_writes": args.ghost_writes,
            "kill_after_ack_target": args.kill_after_ack,
            "seed": args.seed,
        },
        "writer": {
            "exit_code": writer_exit,
            "killed_at_ack": killed_at,
            "finished_naturally": writer_finished_naturally,
            "acks_seen": len(acks),
            "stderr_tail": writer_stderr[-2000:] if writer_stderr else "",
        },
        "verifier": {
            "exit_code": verifier_proc.returncode,
            "found_count": len(found),
            "missing_count": len(missing),
        },
        "result": {
            "acked_but_missing_count": len(acked_but_missing),
            "acked_but_missing_examples": acked_but_missing[:20],
            "not_acked_but_found_count": len(not_acked_but_found),
            "verdict": verdict,
        },
    }

    print(json.dumps(report, indent=2))
    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
            f.write("\n")

    if cleanup:
        shutil.rmtree(args.db_path, ignore_errors=True)

    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
