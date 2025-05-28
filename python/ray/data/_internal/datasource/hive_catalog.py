import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders import (
    ColumnBuilder,
    SerDeInfoBuilder,
    StorageDescriptorBuilder,
    TableBuilder,
)
from thrift.transport.TTransport import TTransportException
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (
    Database as HiveDatabase,
    Table as HiveTable,
)

import ray
from ray.data.datasource import Datasource

# ------------------------- Utility Methods -------------------------


@staticmethod
def _validate_file_format(file_format: str) -> None:
    supported_formats = {"PARQUET", "CSV", "JSON"}
    if file_format.upper() not in supported_formats:
        raise ValueError(
            f"Unsupported file format: {file_format}. "
            f"Supported formats: {', '.join(supported_formats)}"
        )


def _handle_connection_errors(func):
    """Decorator for handling connection-related exceptions"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TTransportException as e:
            logging.error("Connection error occurred: %s", e)
            raise RuntimeError("Metastore connection failure") from e
        except Exception as e:
            logging.error("Operation failed: %s", e)
            raise

    return wrapper


# ------------------------- Type System Component -------------------------
class HiveTypeSystem:
    """Handles type mapping between Arrow and Hive schemas"""

    TYPE_MAPPING = {
        pa.int8(): "tinyint",
        pa.int16(): "smallint",
        pa.int32(): "int",
        pa.int64(): "bigint",
        pa.float32(): "float",
        pa.float64(): "double",
        pa.bool_(): "boolean",
        pa.string(): "string",
        pa.binary(): "binary",
        pa.date32(): "date",
        pa.timestamp("ms"): "timestamp",
        pa.timestamp("ns"): "timestamp",
        pa.timestamp("us"): "timestamp",
    }

    @classmethod
    def arrow_to_hive_type(cls, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to Hive type with complex type handling"""
        if pa.types.is_timestamp(arrow_type):
            return "timestamp"
        if pa.types.is_list(arrow_type):
            element_type = cls.arrow_to_hive_type(arrow_type.value_type)
            return f"array<{element_type}>"
        if pa.types.is_struct(arrow_type):
            fields = [
                f"{field.name}:{cls.arrow_to_hive_type(field.type)}"
                for field in arrow_type
            ]
            return f'struct<{",".join(fields)}>'
        if isinstance(arrow_type, (pa.Decimal128Type, pa.Decimal256Type)):
            return f"decimal({arrow_type.precision},{arrow_type.scale})"
        return cls.TYPE_MAPPING.get(arrow_type, "string")

    @classmethod
    def pandas_to_schema(cls, df: pd.DataFrame) -> List[Dict]:
        """Convert Pandas DataFrame to Hive schema with type validation"""

        def safe_arrow_conversion(dtype) -> str:
            if isinstance(dtype, np.dtype):
                if dtype.kind == "M":
                    return "timestamp"
                if dtype.kind in ["O", "U"]:
                    return "string"
            try:
                arrow_type = pa.from_numpy_dtype(dtype)
                return cls.arrow_to_hive_type(arrow_type)
            except (pa.ArrowNotImplementedError, TypeError):
                return "string"
            except Exception as e:
                logging.warning(f"Type conversion error: {dtype} - {str(e)}")
                return "string"

        return [
            {
                "name": col,
                "type": safe_arrow_conversion(dtype),
                "comment": f"Original dtype: {dtype}"
                + (
                    " (auto-converted)"
                    if safe_arrow_conversion(dtype) in ["timestamp", "string"]
                    else ""
                ),
            }
            for col, dtype in df.dtypes.items()
        ]

    @classmethod
    def arrow_to_schema(cls, table: pa.Table) -> List[Dict]:
        """Convert Arrow Table to Hive column definitions"""
        return [
            {
                "name": field.name,
                "type": cls.arrow_to_hive_type(field.type),
                "comment": field.metadata.get(b"comment", b"").decode()
                if field.metadata
                else "",
            }
            for field in table.schema
        ]


# ------------------------- Ray Integration Components -------------------------
class HiveDataSourceAdapter:
    """Adapts Hive tables to Ray DataSources based on file formats"""

    FORMAT_MAPPING = {
        "PARQUET": ray.data._internal.datasource.parquet_datasource.ParquetDatasource,
        "CSV": ray.data._internal.datasource.csv_datasource.CSVDatasource,
        "JSON": ray.data._internal.datasource.json_datasource.JSONDatasource,
    }

    def __init__(self, catalog: "HiveCatalog"):
        self.catalog = catalog

    def get_datasource(self, table_identifier: str, filesystem) -> Datasource:
        """Get appropriate DataSource for table format"""
        table = self.catalog.get_table(table_identifier)
        file_format = table.parameters.get("table_type", "PARQUET").upper()
        ds_cls = self.FORMAT_MAPPING.get(file_format)
        if not ds_cls:
            raise ValueError(f"Unsupported format: {file_format}")

        return ds_cls(
            paths=table.sd.location,
            filesystem=filesystem,
        )


class HiveDataSinkAdapter:
    """Adapts Hive tables to Ray DataSinks based on file formats"""

    FORMAT_MAPPING = {
        "PARQUET": ray.data._internal.datasource.parquet_datasink.ParquetDatasink,
        "CSV": ray.data._internal.datasource.csv_datasink.CSVDatasink,
        "JSON": ray.data._internal.datasource.json_datasink.JSONDatasink,
    }

    def __init__(self, catalog: "HiveCatalog"):
        self.catalog = catalog

    def get_datasink(
        self, identifier: str, filesystem: Optional["pa.fs.FileSystem"] = None
    ) -> ray.data.Datasink:
        """Get appropriate DataSink for table format"""
        table = self.catalog.get_table(identifier)
        file_format = table.parameters.get("table_type", "PARQUET").upper()
        ds_cls = self.FORMAT_MAPPING.get(file_format)
        if not ds_cls:
            raise ValueError(f"Unsupported format: {file_format}")

        return ds_cls(
            path=table.sd.location,
            filesystem=filesystem,
        )


# ------------------------- Enhanced Hive Catalog -------------------------
class HiveCatalog:
    """Manages Hive metadata operations with connection pooling and type conversion"""

    _URI_PATTERN = re.compile(r"^thrift://([\w.-]+):(\d+)$")
    DEFAULT_NAMESERVICE = "nameservice1"

    def __init__(self, metastore_host: str, metastore_port: int = 9083):
        self.client = HiveMetastoreClient(metastore_host, metastore_port)
        self._connection_lock = threading.Lock()
        self._is_connected = False
        self.default_nameservice = self.DEFAULT_NAMESERVICE

    # ------------------------- Connection Management -------------------------
    def _safe_open(self):
        """Establish thread-safe connection"""
        with self._connection_lock:
            try:
                if not self._is_connected:
                    self.client.open()
                    self._is_connected = True
                    logging.debug("Hive metastore connection established")
            except TTransportException as e:
                if e.type == TTransportException.ALREADY_OPEN:
                    self._is_connected = True
                else:
                    raise
            except Exception as e:
                logging.error("Connection error: %s", e)
                raise

    def _safe_close(self):
        """Close connection safely"""
        with self._connection_lock:
            if self._is_connected:
                try:
                    self.client.close()
                except Exception as e:
                    logging.warning("Connection closure error: %s", str(e))
                finally:
                    self._is_connected = False

    def check_connection(self) -> bool:
        """Validate metastore connection"""
        try:
            self._safe_open()
            self.client.get_all_databases()
            return True
        except Exception as e:
            logging.error("Connection verification failed: %s", e)
            return False
        finally:
            self._safe_close()

    # ------------------------- Database Operations -------------------------
    @_handle_connection_errors
    def create_database(
        self, name: str, location: str, comment: str = ""
    ) -> HiveDatabase:
        """Create new Hive database"""
        clean_loc = self._normalize_location(location)
        db = HiveDatabase(
            name=name, locationUri=clean_loc, description=comment, parameters={}
        )
        try:
            self._safe_open()
            self.client.create_database(db)
            return db
        except Exception as e:
            if "already exists" in str(e):
                return self.get_database(name)
            raise RuntimeError(f"Database creation failed: {e}") from e
        finally:
            self._safe_close()

    def get_database(self, name: str) -> HiveDatabase:
        """Retrieve database metadata"""
        try:
            self._safe_open()
            return self.client.get_database(name)
        except Exception as e:
            raise RuntimeError(f"Database not found: {name}") from e
        finally:
            self._safe_close()

    # ------------------------- Table Operations -------------------------
    def create_table(
        self,
        identifier: str,
        schema: Union[pd.DataFrame, pa.Table, List[Dict]],
        location: str,
        file_format: str = "parquet",
    ) -> HiveTable:
        _validate_file_format(file_format)
        """Create new Hive table with schema validation"""
        db_name, tbl_name = self._parse_identifier(identifier)
        columns = self._convert_schema(schema)

        storage_desc = StorageDescriptorBuilder(
            columns=[ColumnBuilder(**col).build() for col in columns],
            location=self._ensure_uri_protocol(location),
            input_format=self._get_input_format(file_format),
            output_format=self._get_output_format(file_format),
            serde_info=SerDeInfoBuilder(
                serialization_lib=self._get_serde_lib(file_format)
            ).build(),
        ).build()

        table = TableBuilder(
            table_name=tbl_name,
            db_name=db_name,
            storage_descriptor=storage_desc,
            parameters={"table_type": file_format.lower()},
        ).build()

        try:
            self._safe_open()
            self.client.create_table(table)
            return table
        except Exception as e:
            if "already exists" in str(e):
                return self.get_table(identifier)
            raise RuntimeError(f"Table creation failed: {identifier} - {e}") from e
        finally:
            self._safe_close()

    @_handle_connection_errors
    def get_table(self, identifier: str) -> HiveTable:
        """Retrieve table metadata"""
        db_name, tbl_name = self._parse_identifier(identifier)
        try:
            self._safe_open()
            thrift_table = self.client.get_table(db_name, tbl_name)
            return self._convert_thrift_table(thrift_table)
        except Exception as e:
            status = "connected" if self._is_connected else "disconnected"
            raise RuntimeError(f"Table retrieval failed ({status}): {str(e)}") from e
        finally:
            self._safe_close()

    # ------------------------- Deletion Operations -------------------------
    @_handle_connection_errors
    def drop_database(
        self, name: str, cascade: bool = False, delete_data: bool = False
    ) -> None:
        """Drop Hive database with optional cascading deletion"""
        try:
            self._safe_open()
            self.client.drop_database(name, cascade, delete_data)
            logging.info(f"Database dropped: {name}")
        except Exception as e:
            error_msg = f"Database deletion failed: {name}"
            if "DatabaseNotEmptyException" in str(e):
                error_msg += " (Use cascade=True for non-empty databases)"
            raise RuntimeError(error_msg) from e
        finally:
            self._safe_close()

    @_handle_connection_errors
    def drop_table(self, identifier: str, purge: bool = False) -> None:
        """Drop Hive table with optional data purge"""
        db_name, tbl_name = self._parse_identifier(identifier)

        try:
            self._safe_open()
            table = self.client.get_table(db_name, tbl_name)
            data_location = table.sd.location

            self.client.drop_table(db_name, tbl_name, purge)
            logging.info(f"Table dropped: {identifier}")

            if purge and data_location:
                self._purge_data(data_location)
        except Exception as e:
            error_msg = f"Table deletion failed: {identifier}"
            if "NoSuchObjectException" in str(e):
                error_msg += " (Table not found)"
            raise RuntimeError(error_msg) from e
        finally:
            self._safe_close()

    # ------------------------- Helper Methods -------------------------
    def _purge_data(self, location: str) -> None:
        """Purge underlying storage data"""
        try:
            fs = pa.fs.FileSystem.from_uri(location)
            fs.delete_dir(location)
            logging.debug(f"Storage purged: {location}")
        except Exception as e:
            logging.warning(f"Data purge failed: {location} - {str(e)}")

    def _convert_thrift_table(self, thrift_table) -> HiveTable:
        """Convert Thrift table object to domain model"""
        return TableBuilder(
            table_name=thrift_table.tableName,
            db_name=thrift_table.dbName,
            storage_descriptor=self._convert_storage_descriptor(thrift_table.sd),
            parameters=thrift_table.parameters,
            table_type=thrift_table.tableType,
        ).build()

    def _convert_storage_descriptor(self, thrift_sd) -> StorageDescriptorBuilder:
        """Convert Thrift storage descriptor"""
        return StorageDescriptorBuilder(
            location=self._ensure_uri_protocol(thrift_sd.location),
            input_format=thrift_sd.inputFormat,
            output_format=thrift_sd.outputFormat,
            columns=[self._convert_column(col) for col in thrift_sd.cols],
            serde_info=thrift_sd.serdeInfo,
        )

    def _convert_column(self, thrift_col) -> ColumnBuilder:
        """Convert Thrift column definition"""
        return ColumnBuilder(
            name=thrift_col.name,
            type=thrift_col.type,
            comment=getattr(thrift_col, "comment", ""),
        )

    def _convert_schema(
        self, schema: Union[pd.DataFrame, pa.Table, List[Dict]]
    ) -> List[Dict]:
        """Unified schema conversion handler"""
        if isinstance(schema, pd.DataFrame):
            return HiveTypeSystem.pandas_to_schema(schema)
        if isinstance(schema, pa.Table):
            return HiveTypeSystem.arrow_to_schema(schema)
        if isinstance(schema, list) and all(isinstance(item, dict) for item in schema):
            return schema
        raise ValueError(f"Unsupported schema type: {type(schema)}")

    # ------------------------- URI Handling -------------------------
    def _ensure_uri_protocol(self, location: str) -> str:
        """Ensure valid storage protocol in URI"""
        if not location:
            return ""

        if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", location):
            return location

        if location.startswith("/"):
            return f"hdfs://{self.default_nameservice}{location}"
        if re.match(r"^\./|^\.\./|^[a-zA-Z]:\\", location):
            return f"file://{os.path.abspath(location)}"
        if "@" in location and ":" in location:
            bucket, rest = location.split("@", 1)
            return f"s3://{bucket}/{rest}"
        return f"{self.default_protocol}://{location}"

    def _parse_identifier(self, identifier: str) -> tuple:
        """Parse database.table identifier"""
        parts = identifier.split(".")
        if len(parts) < 2:
            return "default", identifier
        return ".".join(parts[:-1]), parts[-1]

    # ------------------------- ray interface -------------------------
    def ray_write_dataset(
        self,
        identifier: str,
        data: Union[ray.data.Dataset, pd.DataFrame, pa.Table],
        file_format: str = "PARQUET",
        ray_params: Dict = None,
        location: str = None,
        filesystem: Optional["pa.fs.FileSystem"] = None,
        **write_args,
    ) -> ray.data.Dataset:
        _validate_file_format(file_format)
        """Write dataset to Hive table using Ray"""
        try:
            self._safe_open()
            table = self.client.get_table(*self._parse_identifier(identifier))
            location = table.sd.location
        except Exception:
            location = self._ensure_uri_protocol(location)
            if not location:
                raise ValueError("Location required for new tables")

            schema = data.schema() if hasattr(data, "schema") else data
            self.create_table(identifier, schema, location, file_format)

        # Convert input data to Ray Dataset
        if isinstance(data, pd.DataFrame):
            ray_ds = ray.data.from_pandas(data)
        elif isinstance(data, pa.Table):
            ray_ds = ray.data.from_arrow(data)
        elif isinstance(data, ray.data.Dataset):
            ray_ds = data
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

        # Configure DataSink
        sink_adapter = HiveDataSinkAdapter(self)
        datasink = sink_adapter.get_datasink(identifier, filesystem)

        # Execute distributed write
        ray_params = ray_params or {"num_cpus": 1}
        ray_ds.write_datasink(datasink, ray_remote_args=ray_params, **write_args)
        return ray_ds

    def ray_read_dataset(
        self,
        identifier: str,
        *,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        concurrency: Optional[int] = None,
        override_num_blocks: Optional[int] = None,
        filesystem: Optional["pa.fs.FileSystem"] = None,
        **read_args,
    ) -> ray.data.Dataset:
        """Read Hive table as Ray Dataset"""
        ds_adapter = HiveDataSourceAdapter(self)
        datasource = ds_adapter.get_datasource(identifier, filesystem)

        return ray.data.read_datasource(
            datasource=datasource,
            parallelism=parallelism,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **read_args,
        )

    # ------------------------- Format Handlers -------------------------

    @staticmethod
    def _normalize_location(location: str) -> str:
        """Normalize storage path protocol while preserving HDFS paths"""
        # Handle non-HDFS protocols
        normalized = re.sub(
            r"^(?!hdfs://)([a-z]+)://",  # Exclude HDFS protocol
            "s3://",
            location,
            flags=re.IGNORECASE,
        )

        # Post-processing for consistency
        if normalized.startswith("s3://"):
            normalized = re.sub(r"s3:///+", "s3://", normalized)
        elif normalized.startswith("hdfs://"):
            normalized = re.sub(r"hdfs:///+", "hdfs://", normalized)

        return normalized

    @staticmethod
    def _get_serde_lib(format: str) -> str:
        """Get serialization library for specified format"""
        return {
            "PARQUET": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "ORC": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "CSV": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "JSON": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "AVRO": "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        }.get(format.upper(), "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")

    @staticmethod
    def _get_input_format(format: str) -> str:
        """Get input format class for specified format"""
        return {
            "PARQUET": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "ORC": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "CSV": "org.apache.hadoop.mapred.TextInputFormat",
            "JSON": "org.apache.hadoop.hive.ql.io.JSONSerDe",
            "AVRO": "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        }.get(format.upper(), "org.apache.hadoop.mapred.TextInputFormat")

    @staticmethod
    def _get_output_format(format: str) -> str:
        """Get output format class for specified format"""
        return {
            "PARQUET": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "ORC": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            "CSV": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "JSON": "org.apache.hadoop.hive.ql.io.JSONSerDe",
            "AVRO": "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            "LANCE": "org.apache.lance.hive.LanceOutputFormat",
        }.get(
            format.upper(), "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        )
