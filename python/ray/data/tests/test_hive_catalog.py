import logging

import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pytest
from thrift.transport.TTransport import TTransportException

from ray.data._internal.datasource.hive_catalog import HiveCatalog, HiveTypeSystem


def check_hms_connection(func):
    """check hive catalog connection"""

    def wrapper(hive_catalog, *args, **kwargs):
        try:
            hive_catalog.get_database("default")
            return func(hive_catalog, *args, **kwargs)
        except TTransportException as e:
            pytest.skip(f"Skipped due to HMS connection failure: {str(e)}")
        except Exception as e:
            logging.error(f"HMS operational error: {str(e)}")
            pytest.fail(f"Test failed due to HMS error: {str(e)}")

    return wrapper


@pytest.fixture(scope="module")
def hive_catalog():
    """auto retry hive catalog connection"""
    max_retries = 3
    catalog = None

    for attempt in range(max_retries):
        try:
            catalog = HiveCatalog(metastore_host="10.37.47.7", metastore_port=9083)
            # validate connection
            catalog.get_database("default")
            logging.info(f"HMS connection established (attempt {attempt+1})")
            break
        except TTransportException as e:
            if attempt == max_retries - 1:
                pytest.skip(
                    f"Failed to connect HMS after {max_retries} attempts, metastore_host=10.37.47.7, metastore_port=9083, error: {str(e)}"
                )
            logging.warning(
                f"HMS connection retrying... (attempt {attempt+1}), metastore_host=10.37.47.7, metastore_port=9083, error: {str(e)}"
            )
            continue
        except Exception as e:
            pytest.fail(f"Unexpected HMS error: {str(e)}")

    yield catalog

    # Clean up test database after tests
    if catalog:
        try:
            if catalog.get_database("test_db"):
                catalog.drop_database("test_db", cascade=True, delete_data=True)
        except Exception as e:
            logging.error(f"Cleanup failed: {str(e)}")
            raise


@pytest.fixture
def s3_filesystem():
    """Configure S3-compatible filesystem"""
    return fs.S3FileSystem(
        access_key="minio_access_key",
        secret_key="minio_secret_key",
        endpoint_override="http://10.37.47.7:9000",
        region="us-west-2",
    )


@pytest.mark.parametrize(
    "file_format,extension", [("PARQUET", "parquet"), ("CSV", "csv"), ("JSON", "json")]
)
def test_file_formats(hive_catalog, s3_filesystem, file_format, extension):
    """Test full lifecycle for different file formats"""
    # 1. Create test database
    hive_catalog.create_database(
        name="test_db", location="s3a://bucket1/test_db", comment="Test database"
    )

    # 2. Create test data
    test_data = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    # 3. Create table and write data
    table_identifier = f"test_db.employees_{extension}"
    hive_catalog.create_table(
        identifier=table_identifier,
        schema=test_data,
        location=f"s3a://bucket1/test_db/employees.{extension}",
        file_format=file_format,
    )

    # 4. Validate table metadata
    table = hive_catalog.get_table(table_identifier)
    assert table.parameters["table_type"] == extension
    assert table.sd.location.endswith(f".{extension}")

    # 5. Write dataset using Ray
    hive_catalog.ray_write_dataset(
        identifier=table_identifier,
        data=test_data,
        file_format=file_format,
        filesystem=s3_filesystem,
    )

    # 6. Read and validate data
    dataset = hive_catalog.ray_read_dataset(
        table_identifier,
        filesystem=s3_filesystem,
        # Format-specific parameters
        **(
            {
                # CSV parameters
                "read_csv_args": {"parse_dates": False} if file_format == "CSV" else {},
                # JSON parameters
                "read_json_args": {"orient": "records"}
                if file_format == "JSON"
                else {},
            }
        ),
    )

    assert dataset.count() == 3

    # Verify JSON string conversion
    assert dataset.schema().types[1] == pa.string()


@check_hms_connection
def test_invalid_format(hive_catalog):
    """Test unsupported format exception handling"""
    with pytest.raises(ValueError) as excinfo:
        hive_catalog.create_table(
            identifier="test_db.invalid_table",
            schema=pd.DataFrame({"col": [1]}),
            location="s3a://bucket1/invalid",
            file_format="UNKNOWN_FORMAT",
        )

    assert "Unsupported file format" in str(excinfo.value)


def test_schema_conversion():
    """Validate schema conversion logic"""
    # Create test data
    simple_df = pd.DataFrame(
        {"id": [1, 2], "scores": [85, 90], "active": [True, False]}
    )

    # Convert to Hive schema
    schema = HiveTypeSystem.pandas_to_schema(simple_df)

    # Validate conversion results
    assert len(schema) == 3
    assert schema[0]["type"] == "bigint"
    assert schema[1]["type"] == "bigint"
    assert schema[2]["type"] == "boolean"


if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])
