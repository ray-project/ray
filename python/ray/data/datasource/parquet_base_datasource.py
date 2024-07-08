import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow

from ray.util.metrics import Histogram
from ray.serve._private.constants import DEFAULT_LATENCY_BUCKET_MS
import time

logger = logging.getLogger(__name__)


@PublicAPI
class ParquetBaseDatasource(FileBasedDatasource):
    """Minimal Parquet datasource, for reading and writing Parquet files."""

    _FILE_EXTENSIONS = ["parquet"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        read_table_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        if read_table_args is None:
            read_table_args = {}

        self.read_table_args = read_table_args
        self.null_count_metric = Histogram(
            "data_read_null_percentage",
            boundaries=[0.00001, 1],
            description=("Coverage of input data (percentage of null to filled values in columns)"),
        )
        self.pyarrow_table_size_metric = Histogram(
            "data_pyarrow_decompressed_size",
            boundaries=DEFAULT_LATENCY_BUCKET_MS, # Even though this is ms, will be reused to give buckets for MB
            description=("Size of the output PyArrow table after being downloaded from S3 and deserialized"),
        )
        self.read_file_latency = Histogram(
            "data_pyarrow_read_table_latency",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            description=("Latency to read input file and convert to PyArrow format"),
        )

    def get_name(self):
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `FileBasedDatasource` method.
        """
        return "ParquetBulk"

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        import pyarrow.parquet as pq

        use_threads = self.read_table_args.pop("use_threads", False)
        start = time.time()
        table = pq.read_table(f, use_threads=use_threads, **self.read_table_args)
        stop = time.time()
        self.read_file_latency.observe((stop - start) * 1000)
        if table and table.nbytes:
            self.pyarrow_table_size_metric.observe(table.nbytes / 1024 / 1024) # Measured in MB
        self.calculate_nulls(table)
        yield table

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # Parquet requires `open_input_file` due to random access reads
        return filesystem.open_input_file(path, **open_args)
    
    def calculate_nulls(self, table):
        total = 0
        null = 0
        for column in table.columns:
            null += column.null_count
            total += len(column)
            
        percent = (total - null) / total
        self.null_count_metric.observe(percent)
