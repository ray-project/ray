import ray
import pytest
import getpass as gt


@pytest.mark.skipif(
    gt.getuser() != "jonasjiang",
    reason="A place holder for testing ray.data.read_iceberg api",
)
def test_read_iceberg_from_glue():
    TEST_TABLE_NAMES = [
        "iceberg_ref.iceberg_all_types_parquet",
        "iceberg_ref.iceberg_nested_parquet",
        "iceberg_ref.iceberg_all_primitive_types_id_bool_partitioned_parquet",
        "iceberg_ref.iceberg_all_primitive_types_id_bool_partitioned_parquet",
        "iceberg_ref.iceberg_all_primitive_types_id_partitioned_parquet",
        "iceberg_ref.iceberg_all_types_id_partitioned_parquet",
        "iceberg_ref.iceberg_all_types_partitioned_parquet_v1",
        "iceberg_ref.iceberg_nested_partitioned_parquet",
        "iceberg_ref.iceberg_nested_partitioned_parquet_v1",
        "snapshot_to_iceberg_demo.migrated_iceberg_all_types_partitioned_same_location",
        "iceberg_ref.type_test_ref_unpartitioned4",
        "iceberg_ref.nested_frame_unpartitioned4",
    ]
    for table_name in TEST_TABLE_NAMES:
        ray_dataset = ray.data.read_iceberg(table_name, catalog_name="default").limit(
            100
        )
        print(ray_dataset.to_pandas(limit=100))
