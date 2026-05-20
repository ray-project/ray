#!/usr/bin/env python3
"""Generate synthetic parquet data for the CPU pipeline benchmark.

Produces deterministic parquet files with mixed column types at a target
total size, using small row groups to create many blocks when read by Ray Data.

Usage:
  python generate_parquet.py --output-dir data/public/parquet --total-gb 5 --seed 42
  python generate_parquet.py --output-dir data/private/parquet --total-gb 5 --seed 99
"""

import argparse
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Target ~500 KB per row group for ~10K blocks from 5 GB
ROWS_PER_FILE = 500_000
ROW_GROUP_SIZE = 5_000  # rows per row group -> ~500 KB with our schema


def generate_dataframe(num_rows: int, seed: int) -> pd.DataFrame:
    """Generate a DataFrame with mixed column types."""
    rng = np.random.default_rng(seed)

    data = {
        # Integer columns (8 bytes each)
        "col_int_0": rng.integers(0, 1_000_000, size=num_rows, dtype=np.int64),
        "col_int_1": rng.integers(0, 1_000_000, size=num_rows, dtype=np.int64),
        "col_int_2": rng.integers(0, 100, size=num_rows, dtype=np.int64),
        # Float columns (8 bytes each)
        "col_float_0": rng.standard_normal(num_rows),
        "col_float_1": rng.uniform(0, 1000, size=num_rows),
        # String columns (variable, ~20 bytes avg)
        "col_str_0": [f"category_{i % 1000}" for i in rng.integers(0, 10000, size=num_rows)],
        "col_str_1": [f"item_{i}" for i in rng.integers(0, 100000, size=num_rows)],
    }

    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic parquet data")
    parser.add_argument("--output-dir", required=True, help="Output directory for parquet files")
    parser.add_argument("--total-gb", type=float, default=5.0, help="Target total size in GB")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--row-group-size", type=int, default=ROW_GROUP_SIZE, help="Rows per row group")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Estimate bytes per row (~100 bytes with our schema after Parquet compression)
    bytes_per_row_estimate = 100
    total_rows = int(args.total_gb * 1e9 / bytes_per_row_estimate)
    num_files = max(1, total_rows // ROWS_PER_FILE)

    print(f"Generating {total_rows:,} rows across {num_files} files (seed={args.seed})")
    print(f"Row group size: {args.row_group_size} rows")
    print(f"Target directory: {output_dir}")

    rows_written = 0
    for file_idx in range(num_files):
        file_seed = args.seed + file_idx
        rows_this_file = min(ROWS_PER_FILE, total_rows - rows_written)
        if rows_this_file <= 0:
            break

        df = generate_dataframe(rows_this_file, file_seed)
        table = pa.Table.from_pandas(df, preserve_index=False)

        file_path = os.path.join(output_dir, f"part_{file_idx:05d}.parquet")
        pq.write_table(
            table,
            file_path,
            row_group_size=args.row_group_size,
            compression="snappy",
        )

        rows_written += rows_this_file
        if (file_idx + 1) % 10 == 0 or file_idx == num_files - 1:
            print(f"  Written {file_idx + 1}/{num_files} files ({rows_written:,} rows)")

    # Report actual size
    total_bytes = sum(
        os.path.getsize(os.path.join(output_dir, f))
        for f in os.listdir(output_dir)
        if f.endswith(".parquet")
    )
    print(f"Done: {rows_written:,} rows, {total_bytes / 1e9:.2f} GB, {num_files} files")

    # Write manifest
    manifest = {
        "total_rows": rows_written,
        "num_files": num_files,
        "total_bytes": total_bytes,
        "seed": args.seed,
        "row_group_size": args.row_group_size,
    }
    import json
    with open(os.path.join(output_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)


if __name__ == "__main__":
    main()
