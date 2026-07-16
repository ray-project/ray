"""Cross-process smoke test for the multi-region manifest transport.

Spins up an N-process pipeline, each process with its own FlightCore:

  stage 0            : produces a base table, store_shared -> manifest, serves it.
  stages 1..N-2      : fetch_shared the upstream manifest (zero-copy), append a
                       derived column, store_shared_append -> new manifest. The
                       upstream segments are re-exported from this process (their
                       fds are forwarded via SCM_RIGHTS), so the base is never
                       re-copied. Serves the new manifest downstream.
  stage N-1 (final)  : fetch_shared the manifest, reconstruct the full table
                       zero-copy across all N regions, and verify it equals the
                       ground-truth base + appended columns.

This proves the "hold N mmap'd regions, forward upstream fds, only materialize
the new column" design works across real process boundaries.

Run:
    python proto/shared_manifest_repro.py --stages 4 --rows 500000
"""

import argparse
import multiprocessing as mp

import numpy as np
import pyarrow as pa


def _make_base(rows: int, base_cols: int) -> "pa.Table":
    rng = np.random.default_rng(0)
    return pa.table({f"c{i}": rng.standard_normal(rows) for i in range(base_cols)})


def _derived(rows: int, col_idx: int) -> "pa.Array":
    rng = np.random.default_rng(1000 + col_idx)
    return pa.array(rng.standard_normal(rows))


def _expected(rows: int, base_cols: int, n_appends: int) -> "pa.Table":
    t = _make_base(rows, base_cols)
    for j in range(n_appends):
        t = t.append_column(f"d{j}", _derived(rows, j))
    return t


def _stage(idx, n_stages, base_cols, rows, recv_conn, send_conn, done_evt, result):
    from ray._private.flight_core import get_flight_core

    core = get_flight_core()

    if idx == 0:
        base = _make_base(rows, base_cols)
        manifest = core.store_shared("o0", base)
        send_conn.send(manifest)
        done_evt.wait()  # keep serving our base region until the final stage is done
        core.delete_shared("o0")
        return

    manifest_in = recv_conn.recv()

    if idx < n_stages - 1:
        # Intermediate stage: append a column and forward.
        table, handle = core.fetch_shared(manifest_in)
        col_idx = idx - 1
        col = _derived(rows, col_idx)
        table = None  # drop the reconstructed view before forwarding
        manifest_out = core.store_shared_append(f"o{idx}", handle, f"d{col_idx}", col)
        send_conn.send(manifest_out)
        done_evt.wait()
        core.delete_shared(f"o{idx}")
        return

    # Final stage: reconstruct and verify.
    table, handle = core.fetch_shared(manifest_in)
    n_appends = n_stages - 2  # stages 1..N-2 each appended one column
    expected = _expected(rows, base_cols, n_appends)
    result["ok"] = table.equals(expected)
    result["regions"] = len(manifest_in["segments"])
    result["cols"] = table.num_columns
    table = None
    handle.close()
    done_evt.set()


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--stages", type=int, default=4, help=">=3 (base + >=1 append + final)"
    )
    p.add_argument("--rows", type=int, default=500_000)
    p.add_argument("--base-cols", type=int, default=8)
    args = p.parse_args()
    assert args.stages >= 3, "need at least base + one append + final"

    ctx = mp.get_context("spawn")
    done_evt = ctx.Event()
    result = ctx.Manager().dict()

    # Chain of pipes: pipe[k] connects stage k -> stage k+1.
    pipes = [ctx.Pipe(duplex=False) for _ in range(args.stages - 1)]

    procs = []
    for idx in range(args.stages):
        recv_conn = pipes[idx - 1][0] if idx > 0 else None
        send_conn = pipes[idx][1] if idx < args.stages - 1 else None
        proc = ctx.Process(
            target=_stage,
            args=(
                idx,
                args.stages,
                args.base_cols,
                args.rows,
                recv_conn,
                send_conn,
                done_evt,
                result,
            ),
        )
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join(timeout=120)

    n_appends = args.stages - 2
    print(f"stages={args.stages}  base_cols={args.base_cols}  appends={n_appends}")
    print(
        f"final table columns: {result.get('cols')} (expected {args.base_cols + n_appends})"
    )
    print(
        f"segments in final manifest: {result.get('regions')} "
        f"(expected {1 + n_appends})"
    )
    print(f"correct: {result.get('ok')}")
    if not result.get("ok"):
        raise SystemExit("CROSS-PROCESS MANIFEST FAILURE")


if __name__ == "__main__":
    main()
