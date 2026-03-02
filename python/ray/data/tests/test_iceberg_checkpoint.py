import pandas as pd
import pyarrow as pa
from pyiceberg import schema as pyi_schema
from pyiceberg import types as pyi_types
from pyiceberg.catalog.sql import SqlCatalog

import ray
from ray.data import read_iceberg
from ray.data.checkpoint import CheckpointConfig, CheckpointBackend
from ray.data.context import DataContext

def test_iceberg_checkpoint_with_iceberg_backend(tmp_path):
    """End-to-end validation for Iceberg-backed checkpoints.

    Verifies:
    1) Checkpoint backend is ICEBERG and the checkpoint table is materialized.
    2) Checkpoint contents are correct (schema and values).
    3) Read-side checkpoint filtering works for READ operators: when combining
       previously-processed data with new data, only new rows (ids) pass through.
    """
    _DB_NAME = "ray_db_backend"
    _TABLE_NAME = "ray_test_backend"
    _CKPT_TABLE_NAME = "ray_ckpt_backend"
    _WAREHOUSE_PATH = f"file://{tmp_path}"
    _CATALOG_KWARGS = {
        "name": "ray_catalog_backend",
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog_backend.db",
        "warehouse": _WAREHOUSE_PATH,
    }

    catalog = SqlCatalog(**_CATALOG_KWARGS)
    catalog.create_namespace(_DB_NAME)

    table_identifier = f"{_DB_NAME}.{_TABLE_NAME}"
    ckpt_table_identifier = f"{_DB_NAME}.{_CKPT_TABLE_NAME}"

    schema = pyi_schema.Schema(
        pyi_types.NestedField(1, "id", pyi_types.LongType(), required=False),
        pyi_types.NestedField(2, "val", pyi_types.StringType(), required=False),
    )
    catalog.create_table(table_identifier, schema=schema)

    # 1) Write initial data with Iceberg checkpoint backend.
    checkpoint_config = CheckpointConfig(
        id_column="id",
        checkpoint_path=ckpt_table_identifier,
        override_backend=CheckpointBackend.ICEBERG,
        catalog_kwargs=_CATALOG_KWARGS,
        delete_checkpoint_on_success=False,
    )

    ctx = DataContext.get_current()
    ctx.checkpoint_config = checkpoint_config

    df1 = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    ds1 = ray.data.from_pandas(df1)

    ds1.write_iceberg(table_identifier, catalog_kwargs=_CATALOG_KWARGS)

    catalog = SqlCatalog(**_CATALOG_KWARGS)
    ckpt_table = catalog.load_table(ckpt_table_identifier)
    arrow_tbl = ckpt_table.scan().to_arrow()
    # Check row count.
    assert arrow_tbl.num_rows == 2
    # Check schema (id:int64 only).
    assert set(arrow_tbl.schema.names) == {"id"}
    assert arrow_tbl.schema.field("id").type == pa.int64()
    # Check values and uniqueness.
    ids_list = arrow_tbl.column(0).to_pylist()
    assert set(ids_list) == {1, 2}
    assert len(ids_list) == len(set(ids_list))
    # Check the checkpoint table is readable via Ray's Iceberg reader.
    ds_ckpt = read_iceberg(table_identifier=ckpt_table_identifier, catalog_kwargs=_CATALOG_KWARGS)
    assert ds_ckpt.count() == 2
    ckpt_rows = sorted(ds_ckpt.take_all(), key=lambda r: r["id"])
    assert ckpt_rows[0]["id"] == 1 and ckpt_rows[1]["id"] == 2
    # Ensure the backend is ICEBERG.
    assert checkpoint_config.backend is CheckpointBackend.ICEBERG

    # 2) Build READ sources that include both processed and new data,
    #    and verify READ-side checkpoint filtering removes processed ids.
    df2 = pd.DataFrame({"id": [3, 4], "val": ["c", "d"]})
    table_identifier_extra = f"{_DB_NAME}.{_TABLE_NAME}_extra"
    catalog.create_table(table_identifier_extra, schema=schema)
    # Temporarily disable checkpointing to ensure (3,4) are not appended to the checkpoint table.
    prev_ckpt = ctx.checkpoint_config
    ctx.checkpoint_config = None
    ray.data.from_pandas(df2).write_iceberg(
        table_identifier_extra, catalog_kwargs=_CATALOG_KWARGS
    )
    ctx.checkpoint_config = prev_ckpt
    ds_old = read_iceberg(table_identifier=table_identifier, catalog_kwargs=_CATALOG_KWARGS)
    ds_extra = read_iceberg(table_identifier=table_identifier_extra, catalog_kwargs=_CATALOG_KWARGS)
    ds_combined = ds_old.union(ds_extra).repartition(1)
    # Use a write as the execution root so READ-side filtering is applied.
    # Only new ids (3,4) should be written.
    out_dir = str(tmp_path / "filtered_out")
    ds_combined.write_parquet(out_dir)
    out_ds = ray.data.read_parquet(out_dir)
    rows = sorted(out_ds.take_all(), key=lambda r: r["id"])
    assert [r["id"] for r in rows] == [3, 4]

    ctx.checkpoint_config = None
