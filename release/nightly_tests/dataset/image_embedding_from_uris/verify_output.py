"""Check that an `image_embedding_from_uris` run landed every row exactly once.

Run after `main.py`, pointed at the same sink:

    python verify_output.py s3://ray-data-write-benchmark/<hex>

The pipeline's output is exactly predictable, which is what makes this checkable
without a baseline run. `create_metadata` emits ``NUM_CONTAINERS * NUM_UNITS`` rows
with a dense ``row_serial``, and ``patch_image`` fans each surviving image out over a
fixed coordinate grid, so ``(row_serial, patch_x, patch_y)`` is a primary key over the
sink and the full set of those keys is known up front.

Three independent things are checked:

  completeness   every expected key is present -- catches rows dropped on the floor,
                 the failure mode a lineage reconstruction path is most likely to have.
  uniqueness     no key appears twice -- catches over-reconstruction, where a task is
                 re-executed and its output delivered on top of output that already
                 made it downstream.
  integrity      ``row_serial`` still agrees with the metadata alongside it -- catches
                 a reconstructed block that carries the *wrong* rows, which row counting
                 alone cannot see.

Deliberately uses pyarrow rather than Ray Data to read the sink: checking Ray Data's
output with Ray Data would let a bug cancel itself out.
"""

import argparse
import sys
from typing import Dict, List, Set, Tuple

import numpy as np
import pyarrow.dataset as ds

# Must match main.py.
NUM_UNITS = 1380
NUM_CONTAINERS = 50
PATCH_SIZE = 256
# `process_image` rescales by 1/applied_scale, and applied_scale is 1, so images reach
# `patch_image` at their native size.
IMAGE_SIZE = 2048

# Only the identity columns. The embedding is ~4 KiB/row and reading it would turn a
# 60 MiB scan into a 10 GiB one; it is sampled separately by `--sample-embeddings`.
KEY_COLUMNS = [
    "row_serial",
    "container_id",
    "container_order_read_id",
    "patch_x",
    "patch_y",
]


def expected_patch_coords() -> Set[Tuple[int, int]]:
    """The (patch_x, patch_y) grid `patch_image` emits for one image.

    Mirrors its ``itertools.product`` over two ``range(PATCH_SIZE, dim - PATCH_SIZE,
    PATCH_SIZE)`` walks -- note both bounds are exclusive of the outermost patch, so
    the grid is inset by one patch on all four sides.
    """
    xs = range(PATCH_SIZE, IMAGE_SIZE - PATCH_SIZE, PATCH_SIZE)
    ys = range(PATCH_SIZE, IMAGE_SIZE - PATCH_SIZE, PATCH_SIZE)
    return {(x, y) for x in xs for y in ys}


def expected_metadata_for_serial(serial: int) -> Tuple[int, str]:
    """The (container_id, container_order_read_id) `create_metadata` gives `serial`.

    Its comprehension iterates ``for j in range(NUM_UNITS) for i in
    range(NUM_CONTAINERS)``, so ``i`` is the fast axis.
    """
    j, i = divmod(serial, NUM_CONTAINERS)
    return i, f"{i:04d}_{j:04d}"


def _fmt(values, limit: int = 10) -> str:
    values = list(values)
    head = ", ".join(str(v) for v in values[:limit])
    return f"{head}{', ...' if len(values) > limit else ''}"


class Report:
    """Accumulates pass/fail lines so every check runs before anything exits."""

    def __init__(self) -> None:
        self.failures: List[str] = []

    def check(self, ok: bool, name: str, detail: str = "") -> None:
        """Record one check. `detail` describes the failure, so it is shown only on
        failure -- printed under a PASS it would assert the opposite of what happened.
        """
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}", flush=True)
        if not ok:
            for line in detail.splitlines():
                print(f"         {line}", flush=True)
            self.failures.append(name)


def load_keys(path: str, columns: List[str]):
    """Read `columns` for every row in the sink."""
    dataset = ds.dataset(path, format="parquet")
    print(f"Scanning {path} ...", flush=True)
    table = dataset.to_table(columns=columns)
    print(f"Read {table.num_rows:,} rows x {len(columns)} identity columns", flush=True)
    return table


def verify(path: str, expect_rows_missing_ok: bool) -> int:
    report = Report()
    table = load_keys(path, KEY_COLUMNS)

    serial = table.column("row_serial").to_numpy(zero_copy_only=False).astype(np.int64)
    px = table.column("patch_x").to_numpy(zero_copy_only=False).astype(np.int64)
    py = table.column("patch_y").to_numpy(zero_copy_only=False).astype(np.int64)
    container_id = (
        table.column("container_id").to_numpy(zero_copy_only=False).astype(np.int64)
    )
    read_id = table.column("container_order_read_id").to_pylist()

    coords = expected_patch_coords()
    n_inputs = NUM_UNITS * NUM_CONTAINERS
    expected_rows = n_inputs * len(coords)

    print()
    print(
        f"Expecting {n_inputs:,} input rows x {len(coords)} patches "
        f"= {expected_rows:,} output rows"
    )
    print()

    # --- 1. Row count -----------------------------------------------------------
    report.check(
        table.num_rows == expected_rows,
        "total row count",
        f"got {table.num_rows:,}, expected {expected_rows:,} "
        f"(delta {table.num_rows - expected_rows:+,})",
    )

    # --- 2. Patch grid ----------------------------------------------------------
    # Compared as a set first: a wrong grid would otherwise show up as thousands of
    # confusing per-serial count failures below.
    observed_coords = set(zip(px.tolist(), py.tolist()))
    report.check(
        observed_coords == coords,
        "patch coordinate grid",
        f"unexpected={_fmt(sorted(observed_coords - coords))} "
        f"missing={_fmt(sorted(coords - observed_coords))}",
    )

    # --- 3. Completeness --------------------------------------------------------
    present = np.unique(serial)
    expected_serials = np.arange(n_inputs, dtype=np.int64)
    missing = np.setdiff1d(expected_serials, present, assume_unique=True)
    unexpected = np.setdiff1d(present, expected_serials, assume_unique=True)
    report.check(
        len(missing) == 0 or expect_rows_missing_ok,
        "every input row reached the sink",
        f"{len(missing):,} of {n_inputs:,} input rows absent: {_fmt(missing)}",
    )
    report.check(
        len(unexpected) == 0,
        "no unknown row_serial in sink",
        f"{len(unexpected):,} unknown: {_fmt(unexpected)}",
    )

    # --- 4. Uniqueness ----------------------------------------------------------
    # The whole point of the check: over-reconstruction shows up here and nowhere else.
    # Packed into one int64 so this is a single sort rather than a python-level set of
    # millions of tuples.
    packed = (serial * (IMAGE_SIZE + 1) + px) * (IMAGE_SIZE + 1) + py
    uniq, counts = np.unique(packed, return_counts=True)
    dup_mask = counts > 1
    n_dup_keys = int(dup_mask.sum())
    n_excess = int((counts[dup_mask] - 1).sum())
    dup_examples = []
    for packed_key in uniq[dup_mask][:10]:
        rest, y = divmod(int(packed_key), IMAGE_SIZE + 1)
        s, x = divmod(rest, IMAGE_SIZE + 1)
        dup_examples.append(f"(serial={s}, x={x}, y={y})")
    report.check(
        n_dup_keys == 0,
        "no duplicated output rows",
        f"{n_dup_keys:,} duplicated keys, {n_excess:,} excess rows: "
        f"{_fmt(dup_examples)}",
    )

    # --- 5. Uniform fan-out -----------------------------------------------------
    # A serial that is present but short means part of one flat_map output was lost --
    # invisible to the completeness check, which only asks whether a serial appeared.
    per_serial = np.bincount(serial, minlength=n_inputs)
    seen = per_serial[present]
    short = present[seen != len(coords)]
    report.check(
        len(short) == 0,
        "every image produced its full patch set",
        f"{len(short):,} serials with wrong patch count: "
        + _fmt([f"{s}:{per_serial[s]}" for s in short[:10]]),
    )

    # --- 6. Metadata integrity --------------------------------------------------
    # Catches a reconstructed block that carries the wrong rows: the row_serial would no
    # longer agree with the metadata sitting next to it in the same row.
    exp_container = (serial % NUM_CONTAINERS).astype(np.int64)
    container_mismatch = int((container_id != exp_container).sum())
    report.check(
        container_mismatch == 0,
        "container_id agrees with row_serial",
        f"{container_mismatch:,} rows disagree",
    )

    # String compare over millions of rows is slow, so check one row per serial --
    # enough to localise any block whose identity got crossed.
    first_idx = np.unique(serial, return_index=True)[1]
    read_id_mismatch: Dict[int, Tuple[str, str]] = {}
    for idx in first_idx:
        s = int(serial[idx])
        _, expected_read_id = expected_metadata_for_serial(s)
        if read_id[idx] != expected_read_id:
            read_id_mismatch[s] = (read_id[idx], expected_read_id)
    report.check(
        not read_id_mismatch,
        "container_order_read_id agrees with row_serial",
        f"{len(read_id_mismatch):,} serials disagree: "
        + _fmt([f"{s}: got {g} want {w}" for s, (g, w) in read_id_mismatch.items()]),
    )

    print()
    if report.failures:
        print(f"FAILED {len(report.failures)} check(s): {', '.join(report.failures)}")
        return 1
    print("All checks passed: every row arrived exactly once, with intact identity.")
    return 0


def sample_embeddings(path: str, n_files: int) -> int:
    """Spot-check embedding values in a few output files.

    Row accounting cannot tell a real embedding from a block of zeros or NaNs, so this
    reads the payload for a handful of files and checks it is finite, correctly shaped
    and not degenerate.
    """
    report = Report()
    dataset = ds.dataset(path, format="parquet")
    files = sorted(dataset.files)[:n_files]
    print()
    print(f"Sampling embeddings from {len(files)} of {len(dataset.files)} files")
    # `dataset.files` are paths *within* the filesystem, with the `s3://` scheme
    # already stripped, so the filesystem has to be carried over explicitly -- without
    # it they get resolved against the local disk and every one is "not found".
    sample = ds.dataset(
        files, format="parquet", filesystem=dataset.filesystem
    ).to_table(columns=["embedding"])
    column = sample.column("embedding").combine_chunks()
    # `flatten` drops null entries, which would misalign the reshape below.
    report.check(
        column.null_count == 0,
        "embeddings are non-null",
        f"{column.null_count:,} null embeddings",
    )
    if report.failures:
        return 1
    # Flatten into one NumPy buffer instead of materializing a Python float per value.
    embeddings = column.flatten().to_numpy(zero_copy_only=False)
    embeddings = embeddings.reshape(len(column), -1)
    report.check(
        embeddings.ndim == 2 and embeddings.shape[1] == 1000,
        "embedding shape",
        f"got {embeddings.shape}, expected (n, 1000)",
    )
    report.check(
        bool(np.isfinite(embeddings).all()),
        "embeddings are finite",
        f"{int((~np.isfinite(embeddings)).sum()):,} non-finite values",
    )
    # All-identical rows would mean the payload never varied with its input.
    report.check(
        len(np.unique(embeddings, axis=0)) > 1,
        "embeddings are not degenerate",
        "every sampled embedding is identical",
    )
    return 1 if report.failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path", help="The sink `main.py` wrote to, e.g. s3://bucket/hex"
    )
    parser.add_argument(
        "--sample-embeddings",
        type=int,
        default=0,
        metavar="N",
        help="Also read the embedding payload from N output files and sanity-check it.",
    )
    parser.add_argument(
        "--allow-missing-rows",
        action="store_true",
        help=(
            "Report absent input rows without failing. Use when the run's filter is "
            "expected to drop rows; it does not for the stock workload."
        ),
    )
    args = parser.parse_args()

    status = verify(args.path, args.allow_missing_rows)
    if args.sample_embeddings:
        status |= sample_embeddings(args.path, args.sample_embeddings)
    return status


if __name__ == "__main__":
    sys.exit(main())
