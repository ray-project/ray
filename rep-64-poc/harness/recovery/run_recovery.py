#!/usr/bin/env python3
"""Phase 8 storage-layer recovery-time orchestrator (REP-64 POC).

Runs the recovery_bench binary in two distinct processes:

  1. Process A: --mode populate. Writes N entries to a RocksDB at the
     given path, then exits cleanly. The exit closes the DB so later
     "open" timing is honest cold-open, not a re-attach to a still-open
     handle.

  2. Process B: --mode recover. Constructs a fresh RocksDbStoreClient
     against the same path, runs AsyncGetAll, and prints timing.

The two-process structure ensures we measure cold-open behaviour (the
case the REP cares about: head-pod restart re-mounts a PVC and the
recovering GCS opens RocksDB from disk).

Run for a small grid of state sizes; emit a JSON report.

Usage:
  python3 rep-64-poc/harness/recovery/run_recovery.py \
    --bin bazel-bin/rep-64-poc/harness/recovery/recovery_bench \
    --db-root $HOME/.cache/rep64-recovery \
    --output rep-64-poc/harness/recovery/results/recovery.json
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path


def run_one(bin_path: str, db_dir: Path, num_keys: int) -> dict:
    print(f"# size={num_keys}: populate", file=sys.stderr)
    t0 = time.monotonic()
    subprocess.run(
        [
            bin_path,
            "--mode",
            "populate",
            "--db-dir",
            str(db_dir),
            "--num-keys",
            str(num_keys),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    populate_seconds = time.monotonic() - t0

    print(f"# size={num_keys}: recover", file=sys.stderr)
    recover = subprocess.run(
        [bin_path, "--mode", "recover", "--db-dir", str(db_dir)],
        capture_output=True,
        text=True,
        check=True,
    )
    recover_json = json.loads(recover.stdout.strip())

    return {
        "num_keys": num_keys,
        "populate_wall_seconds": populate_seconds,
        "recover": recover_json,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--bin", required=True, help="path to bazel-built recovery_bench binary"
    )
    ap.add_argument(
        "--db-root",
        required=True,
        help="root dir for per-run RocksDB instances; should be on "
        "an honest-fsync substrate (e.g. ext4 on /home, NOT /tmp)",
    )
    ap.add_argument(
        "--sizes",
        default="100,1000,10000,100000",
        help="comma-separated list of state sizes to benchmark",
    )
    ap.add_argument("--output", default=None)
    ap.add_argument(
        "--keep-dbs",
        action="store_true",
        help="don't delete per-size DBs after the run",
    )
    args = ap.parse_args()

    sizes = [int(s) for s in args.sizes.split(",") if s.strip()]
    db_root = Path(args.db_root)
    db_root.mkdir(parents=True, exist_ok=True)

    runs = []
    for n in sizes:
        db_dir = db_root / f"recovery-{n}"
        if db_dir.exists():
            shutil.rmtree(db_dir)
        db_dir.mkdir(parents=True)
        try:
            r = run_one(args.bin, db_dir, n)
        finally:
            if not args.keep_dbs:
                shutil.rmtree(db_dir, ignore_errors=True)
        runs.append(r)

    report = {
        "schema_version": 1,
        "host": {
            "node": platform.node(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "runs": runs,
    }
    print(json.dumps(report, indent=2))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
            f.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
