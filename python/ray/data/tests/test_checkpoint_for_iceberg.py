import os
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pkg_resources import parse_version
from pyiceberg import (
    catalog as pyi_catalog,
    schema as pyi_schema,
    types as pyi_types,
)

import ray
from ray.data._internal.datasource.iceberg_datasink import (
    IcebergDatasink,
    IcebergWriteResult,
)
from ray.data._internal.savemode import SaveMode
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_filter import IcebergCheckpointLoader
from ray.data.checkpoint.checkpoint_writer import BatchBasedCheckpointWriter
from ray.data.context import DataContext
from ray.data.datasource.datasink import WriteResult

_PYICEBERG_MIN_PYARROW_VERSION = parse_version("14.0.0")
_SKIP_IF_PYARROW_TOO_OLD_FOR_PYICEBERG = pytest.mark.skipif(
    get_pyarrow_version() < _PYICEBERG_MIN_PYARROW_VERSION,
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)


@pytest.fixture
def clean_data_context():
    ctx = DataContext.get_current()
    old_config = ctx.checkpoint_config
    yield
    ctx.checkpoint_config = old_config


def _create_sql_catalog_table(
    tmp_path, *, catalog_name: str, db_name: str, table_name: str
):
    warehouse_path = tmp_path / "warehouse"
    os.makedirs(warehouse_path, exist_ok=True)
    catalog_kwargs = {
        "name": catalog_name,
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/{catalog_name}.db",
        "warehouse": f"file://{warehouse_path}",
    }

    full_table_identifier = f"{db_name}.{table_name}"
    sql_catalog = pyi_catalog.load_catalog(**catalog_kwargs)

    if (db_name,) not in sql_catalog.list_namespaces():
        sql_catalog.create_namespace(db_name)
    if (db_name, table_name) in sql_catalog.list_tables(db_name):
        sql_catalog.drop_table(full_table_identifier)

    sql_catalog.create_table(
        full_table_identifier,
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1,
                name="id",
                field_type=pyi_types.LongType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=2,
                name="data",
                field_type=pyi_types.StringType(),
                required=False,
            ),
        ),
    )

    return full_table_identifier, catalog_kwargs


def _write_parquet_files(base_dir, file_to_rows):
    os.makedirs(base_dir, exist_ok=True)
    for file_name, rows in file_to_rows.items():
        pq.write_table(pa.Table.from_pylist(rows), str(base_dir / file_name))


def _sorted_df(rows):
    return pd.DataFrame(rows).sort_values("id").reset_index(drop=True)


def _read_iceberg_sorted_df(*, full_table_identifier: str, catalog_kwargs):
    return (
        ray.data.read_iceberg(
            table_identifier=full_table_identifier,
            catalog_kwargs=catalog_kwargs,
        )
        .to_pandas()
        .sort_values("id")
        .reset_index(drop=True)
    )


def _set_checkpoint_config(tmp_path):
    checkpoint_dir = tmp_path / "checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    ckpt_config = CheckpointConfig(
        checkpoint_path=str(checkpoint_dir),
        id_column="id",
        delete_checkpoint_on_success=False,
    )
    DataContext.get_current().checkpoint_config = ckpt_config
    return ckpt_config


def _make_fail_once_datasink(fail_message: str, *args, **kwargs):
    class _FailOnceIcebergDatasink(IcebergDatasink):
        def __init__(self, *iargs, **ikwargs):
            super().__init__(*iargs, **ikwargs)
            self._fail_once = True

        def on_write_complete(self, write_result: WriteResult) -> None:
            if self._fail_once:
                self._fail_once = False
                raise RuntimeError(fail_message)
            return super().on_write_complete(write_result)

    return _FailOnceIcebergDatasink(*args, **kwargs)


def test_iceberg_checkpoint_write_and_load(
    ray_start_10_cpus_shared, tmp_path, clean_data_context
):
    checkpoint_dir = tmp_path / "checkpoints"
    os.makedirs(checkpoint_dir)

    ckpt_config = CheckpointConfig(
        checkpoint_path=str(checkpoint_dir),
        id_column="id",
        delete_checkpoint_on_success=False,
    )

    writer = BatchBasedCheckpointWriter(ckpt_config)

    block1 = BlockAccessor.for_block(
        pa.table({"id": [1, 2, 3], "data": ["a", "b", "c"]})
    )
    result1 = IcebergWriteResult(
        data_files=[SimpleNamespace(file_path="file1.parquet")],
        schemas=[pa.schema([("id", pa.int64()), ("data", pa.string())])],
    )

    block2 = BlockAccessor.for_block(pa.table({"id": [4, 5], "data": ["d", "e"]}))
    result2 = IcebergWriteResult(
        data_files=[SimpleNamespace(file_path="file2.parquet")],
        schemas=[],
    )

    writer.write_block_checkpoint(block1, write_result=result1)
    writer.write_block_checkpoint(block2, write_result=result2)

    files = os.listdir(checkpoint_dir)
    parquet_files = [f for f in files if f.endswith(".parquet")]
    meta_files = [f for f in files if f.endswith(".meta.pkl")]

    assert len(parquet_files) == 2
    assert len(meta_files) == 2

    loader = IcebergCheckpointLoader(ckpt_config)

    loaded_results = loader.load_write_results()
    assert len(loaded_results) == 2

    data_file_paths = {
        df.file_path for res in loaded_results for df in (res.data_files or [])
    }
    assert "file1.parquet" in data_file_paths
    assert "file2.parquet" in data_file_paths

    loaded_ids = loader.get_checkpoint_ids(id_col=ckpt_config.id_column)
    assert len(loaded_ids) == 5
    assert loaded_ids == {1, 2, 3, 4, 5}


@_SKIP_IF_PYARROW_TOO_OLD_FOR_PYICEBERG
def test_iceberg_checkpoint_recovers_write_results_and_commits(
    ray_start_10_cpus_shared, tmp_path, clean_data_context
):
    full_table_identifier, catalog_kwargs = _create_sql_catalog_table(
        tmp_path,
        catalog_name="ray_catalog_ckpt",
        db_name="ray_db_ckpt",
        table_name="ray_table_ckpt",
    )
    input_dir = tmp_path / "input_parquet"
    rows = [{"id": i, "data": f"v{i}"} for i in range(10)]
    _write_parquet_files(
        input_dir,
        {"part0.parquet": rows[:5], "part1.parquet": rows[5:]},
    )
    expected_df = _sorted_df(rows)

    _set_checkpoint_config(tmp_path)

    datasink = _make_fail_once_datasink(
        "fail once before iceberg commit",
        full_table_identifier,
        catalog_kwargs=catalog_kwargs,
    )

    ds = ray.data.read_parquet(str(input_dir)).repartition(2)
    with pytest.raises(RuntimeError, match="fail once before iceberg commit"):
        ds.write_datasink(datasink, concurrency=2)

    ds = ray.data.read_parquet(str(input_dir)).repartition(2)
    ds.write_datasink(datasink, concurrency=2)

    actual_df = _read_iceberg_sorted_df(
        full_table_identifier=full_table_identifier, catalog_kwargs=catalog_kwargs
    )
    assert actual_df["id"].is_unique
    assert actual_df.equals(expected_df)


@_SKIP_IF_PYARROW_TOO_OLD_FOR_PYICEBERG
def test_iceberg_checkpoint_recovers_upsert_keys_and_upserts(
    ray_start_10_cpus_shared, tmp_path, clean_data_context
):
    full_table_identifier, catalog_kwargs = _create_sql_catalog_table(
        tmp_path,
        catalog_name="ray_catalog_upsert_ckpt",
        db_name="ray_db_upsert_ckpt",
        table_name="ray_table_upsert_ckpt",
    )
    seed_dir = tmp_path / "seed_parquet"
    seed_rows = [{"id": i, "data": f"v{i}"} for i in range(5)]
    _write_parquet_files(seed_dir, {"seed.parquet": seed_rows})

    seed_sink = IcebergDatasink(
        full_table_identifier,
        catalog_kwargs=catalog_kwargs,
        mode=SaveMode.APPEND,
    )
    ray.data.read_parquet(str(seed_dir)).repartition(1).write_datasink(
        seed_sink, concurrency=1
    )

    upsert_dir = tmp_path / "upsert_parquet"
    upsert_rows = [{"id": i, "data": f"v{i}_new"} for i in range(3)] + [
        {"id": 10, "data": "v10"},
        {"id": 11, "data": "v11"},
    ]
    _write_parquet_files(
        upsert_dir,
        {"part0.parquet": upsert_rows[:3], "part1.parquet": upsert_rows[3:]},
    )

    ckpt_config = _set_checkpoint_config(tmp_path)

    datasink = _make_fail_once_datasink(
        "fail once before iceberg upsert commit",
        full_table_identifier,
        catalog_kwargs=catalog_kwargs,
        mode=SaveMode.UPSERT,
        upsert_kwargs={"join_cols": ["id"]},
    )

    ds = ray.data.read_parquet(str(upsert_dir)).repartition(2)
    with pytest.raises(RuntimeError, match="fail once before iceberg upsert commit"):
        ds.write_datasink(datasink, concurrency=2)

    loader = IcebergCheckpointLoader(ckpt_config)
    loaded_results = loader.load_write_results()
    assert loaded_results
    total_key_rows = sum(
        len(r.upsert_keys) for r in loaded_results if r.upsert_keys is not None
    )
    assert total_key_rows == len(upsert_rows)

    ds = ray.data.read_parquet(str(upsert_dir)).repartition(2)
    ds.write_datasink(datasink, concurrency=2)

    expected_rows = [
        {"id": 0, "data": "v0_new"},
        {"id": 1, "data": "v1_new"},
        {"id": 2, "data": "v2_new"},
        {"id": 3, "data": "v3"},
        {"id": 4, "data": "v4"},
        {"id": 10, "data": "v10"},
        {"id": 11, "data": "v11"},
    ]
    expected_df = _sorted_df(expected_rows)
    actual_df = _read_iceberg_sorted_df(
        full_table_identifier=full_table_identifier, catalog_kwargs=catalog_kwargs
    )
    assert actual_df["id"].is_unique
    assert actual_df.equals(expected_df)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
