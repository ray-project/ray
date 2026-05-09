import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
from pyarrow.fs import FileSelector, FileType, LocalFileSystem
from pyiceberg import schema as pyi_schema, types as pyi_types
from pyiceberg.catalog.sql import SqlCatalog

import ray
from ray.data import read_iceberg
from ray.data.checkpoint import CheckpointConfig
from ray.data.context import DataContext


def _make_catalog_kwargs(name: str, tmp_path) -> dict:
    """Build kwargs for a local SQLite-backed SQL catalog."""
    return {
        "name": name,
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/{name}.db",
        "warehouse": f"file://{tmp_path}",
    }


def _create_iceberg_table(catalog: SqlCatalog, table_id: str) -> None:
    """Create an iceberg table with schema: id INT, name STRING, age INT."""
    schema = pyi_schema.Schema(
        pyi_types.NestedField(1, "id", pyi_types.IntegerType(), required=False),
        pyi_types.NestedField(2, "name", pyi_types.StringType(), required=False),
        pyi_types.NestedField(3, "age", pyi_types.IntegerType(), required=False),
    )
    catalog.create_table(table_id, schema=schema)


def _write_source_rows(n: int, table_id: str, catalog_kwargs: dict) -> None:
    """Write n rows (id 0..n-1) to an iceberg table without checkpoint."""
    ctx = DataContext.get_current()
    prev_ckpt = ctx.checkpoint_config
    ctx.checkpoint_config = None
    try:
        df = pd.DataFrame(
            {
                "id": list(range(n)),
                "name": [f"name_{i}" for i in range(n)],
                "age": [i % 100 for i in range(n)],
            }
        )
        ray.data.from_pandas(df).write_iceberg(
            table_id,
            mode="overwrite",
            catalog_kwargs=catalog_kwargs,
        )
    finally:
        ctx.checkpoint_config = prev_ckpt


@dataclass
class CheckpointDirStats:
    """File counts and row totals found in a checkpoint directory."""

    parquet_count: int
    meta_pkl_count: int
    pending_parquet_count: int
    total_checkpointed_rows: int


def _inspect_checkpoint_dir(
    checkpoint_path: str,
    id_column: str,
    filesystem: Optional[LocalFileSystem] = None,
) -> CheckpointDirStats:
    """Scan the checkpoint directory and return file counts and checkpointed row total.

    Counts:
      - committed .parquet files (exclude .pending.parquet)
      - .meta.pkl files
      - .pending.parquet files (should be 0 after a successful run)

    Also reads all committed .parquet files and sums their row counts.

    Args:
        checkpoint_path: Local filesystem path to the checkpoint directory.
        id_column: Name of the ID column in the checkpoint parquet files.
        filesystem: PyArrow filesystem. Defaults to LocalFileSystem.

    Returns:
        CheckpointDirStats with counts and total checkpointed rows.
    """
    fs = filesystem or LocalFileSystem()
    file_infos = fs.get_file_info(
        FileSelector(checkpoint_path, recursive=False)
    )

    parquet_count = 0
    meta_pkl_count = 0
    pending_parquet_count = 0
    total_rows = 0

    for f in file_infos:
        if f.type != FileType.File:
            continue
        name = os.path.basename(f.path)
        if name.endswith(".pending.parquet"):
            pending_parquet_count += 1
        elif name.endswith(".parquet"):
            parquet_count += 1
            table = pq.read_table(f.path, filesystem=fs, columns=[id_column])
            total_rows += table.num_rows
        elif name.endswith(".meta.pkl"):
            meta_pkl_count += 1

    return CheckpointDirStats(
        parquet_count=parquet_count,
        meta_pkl_count=meta_pkl_count,
        pending_parquet_count=pending_parquet_count,
        total_checkpointed_rows=total_rows,
    )


def test_iceberg_source_to_iceberg_dest_with_checkpoint(tmp_path):
    """Checkpoint recovery: iceberg_source -> write_iceberg to iceberg_dest.

    Run 1: filter ids 0-49 from iceberg_source and write to iceberg_dest.
           Checkpoint records ids 0-49.
           Verify: checkpoint dir has paired .parquet + .meta.pkl files,
                   no .pending.parquet files, total checkpointed rows == 50.
    Run 2: read all 100 rows from iceberg_source. Checkpoint skips ids 0-49
           and writes ids 50-99 to iceberg_dest.
    Final: iceberg_dest contains exactly 100 unique rows (ids 0-99).
    """
    cat_kwargs = _make_catalog_kwargs("cat_iceberg", tmp_path)
    catalog = SqlCatalog(**cat_kwargs)
    catalog.create_namespace("db")
    _create_iceberg_table(catalog, "db.iceberg_source")
    _create_iceberg_table(catalog, "db.iceberg_dest")
    _write_source_rows(100, "db.iceberg_source", cat_kwargs)

    ckpt_path = str(tmp_path / "ckpt_iceberg")
    checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=ckpt_path,
        delete_checkpoint_on_success=False,
    )
    ctx = DataContext.get_current()

    # Run 1: process only ids 0-49; checkpoint records these ids.
    ctx.checkpoint_config = checkpoint_config
    (
        read_iceberg(table_identifier="db.iceberg_source", catalog_kwargs=cat_kwargs)
        .filter(lambda row: row["id"] < 50)
        .write_iceberg("db.iceberg_dest", catalog_kwargs=cat_kwargs)
    )

    # Verify checkpoint directory after run 1.
    # write_iceberg is a non-file datasink: each block produces a paired
    # {uuid}.parquet (row IDs) and {uuid}.meta.pkl (IcebergWriteResult).
    # No .pending.parquet files should exist after a successful run.
    stats = _inspect_checkpoint_dir(ckpt_path, id_column="id")
    assert stats.parquet_count >= 1, (
        f"Expected at least 1 committed .parquet file, got {stats.parquet_count}"
    )
    assert stats.meta_pkl_count >= 1, (
        f"Expected at least 1 .meta.pkl file, got {stats.meta_pkl_count}"
    )
    assert stats.parquet_count == stats.meta_pkl_count, (
        f".parquet count ({stats.parquet_count}) != "
        f".meta.pkl count ({stats.meta_pkl_count}): files must be paired"
    )
    assert stats.pending_parquet_count == 0, (
        f"Expected 0 .pending.parquet files after success, "
        f"got {stats.pending_parquet_count}"
    )
    assert stats.total_checkpointed_rows == 50, (
        f"Expected 50 checkpointed rows after run 1, "
        f"got {stats.total_checkpointed_rows}"
    )

    # After a SUCCESSFUL run 1, delete .meta.pkl files before starting run 2.
    # Background: .meta.pkl stores IcebergWriteResult objects to support crash
    # recovery (re-committing data files written but not yet committed to the
    # iceberg table).  When run 1 SUCCEEDS it already commits those data files,
    # so keeping .meta.pkl would cause run 2's on_write_complete to re-commit
    # the same data files, producing duplicate rows.  The .parquet checkpoint
    # files (row-ID lists) are kept so that run 2 can still filter out ids 0-49.
    for fname in os.listdir(ckpt_path):
        if fname.endswith(".meta.pkl"):
            os.remove(os.path.join(ckpt_path, fname))

    # Run 2: read all 100 rows; checkpoint filters out ids 0-49, writes 50-99.
    ctx.checkpoint_config = checkpoint_config
    (
        read_iceberg(table_identifier="db.iceberg_source", catalog_kwargs=cat_kwargs).write_iceberg(
            "db.iceberg_dest", catalog_kwargs=cat_kwargs
        )
    )

    # Verify iceberg_dest has exactly 100 unique rows (0-99).
    ctx.checkpoint_config = None
    result_rows = read_iceberg(table_identifier="db.iceberg_dest", catalog_kwargs=cat_kwargs).take_all()
    assert len(result_rows) == 100, f"Expected 100 rows, got {len(result_rows)}"
    ids_out = sorted(r["id"] for r in result_rows)
    assert ids_out == list(range(100)), f"Unexpected ids: {ids_out}"


def test_iceberg_source_to_csv_with_checkpoint(tmp_path):
    """Checkpoint recovery: iceberg_source -> write_csv.

    Run 1: filter ids 0-49 from iceberg_source and write to csv_path.
           Checkpoint records ids 0-49.
           Verify: checkpoint dir has committed .parquet files (2-phase commit
                   finalised), no .meta.pkl files, no .pending.parquet files,
                   total checkpointed rows == 50.
    Run 2: read all 100 rows from iceberg_source. Checkpoint skips ids 0-49
           and writes ids 50-99 to csv_path.
    Final: csv_path contains exactly 100 unique rows (ids 0-99).
    """
    cat_kwargs = _make_catalog_kwargs("cat_csv", tmp_path)
    catalog = SqlCatalog(**cat_kwargs)
    catalog.create_namespace("db")
    _create_iceberg_table(catalog, "db.iceberg_source")
    _write_source_rows(100, "db.iceberg_source", cat_kwargs)

    csv_output = str(tmp_path / "csv_output")
    os.makedirs(csv_output, exist_ok=True)

    ckpt_path = str(tmp_path / "ckpt_csv")
    checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=ckpt_path,
        delete_checkpoint_on_success=False,
    )
    ctx = DataContext.get_current()

    # Run 1: process only ids 0-49; checkpoint records these ids.
    ctx.checkpoint_config = checkpoint_config
    (
        read_iceberg(table_identifier="db.iceberg_source", catalog_kwargs=cat_kwargs)
        .filter(lambda row: row["id"] < 50)
        .write_csv(csv_output)
    )

    # Verify checkpoint directory after run 1.
    # write_csv uses 2-phase commit: .pending.parquet is renamed to .parquet
    # after each data file is written successfully. No .meta.pkl files are
    # produced for file-based datasinks.
    stats = _inspect_checkpoint_dir(ckpt_path, id_column="id")
    assert stats.parquet_count >= 1, (
        f"Expected at least 1 committed .parquet file, got {stats.parquet_count}"
    )
    assert stats.meta_pkl_count == 0, (
        f"Expected 0 .meta.pkl files for CSV sink, got {stats.meta_pkl_count}"
    )
    assert stats.pending_parquet_count == 0, (
        f"Expected 0 .pending.parquet files after success, "
        f"got {stats.pending_parquet_count}"
    )
    assert stats.total_checkpointed_rows == 50, (
        f"Expected 50 checkpointed rows after run 1, "
        f"got {stats.total_checkpointed_rows}"
    )

    # Run 2: read all 100 rows; checkpoint filters out ids 0-49, writes 50-99.
    ctx.checkpoint_config = checkpoint_config
    (
        read_iceberg(table_identifier="db.iceberg_source", catalog_kwargs=cat_kwargs).write_csv(
            csv_output
        )
    )

    # Verify csv_output has exactly 100 unique rows (0-99).
    ctx.checkpoint_config = None
    result_rows = ray.data.read_csv(csv_output).take_all()
    assert len(result_rows) == 100, f"Expected 100 rows, got {len(result_rows)}"
    ids_out = sorted(r["id"] for r in result_rows)
    assert ids_out == list(range(100)), f"Unexpected ids: {ids_out}"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
