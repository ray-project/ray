import logging
import os
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pyarrow

    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

_MIN_HUDI_VERSION = "0.5.0.dev0"


def _check_hudi_version() -> None:
    import hudi
    from packaging.version import parse as parse_version

    version = getattr(hudi, "__version__", "0")
    if parse_version(version) < parse_version(_MIN_HUDI_VERSION):
        raise ImportError(
            f"Ray Data's Hudi datasource requires hudi >= {_MIN_HUDI_VERSION}, "
            f"but found {version}. Run "
            f"`pip install -U 'hudi>={_MIN_HUDI_VERSION}'`."
        )


def _bin_pack_by_size(slices, n_bins):
    sorted_slices = sorted(slices, key=lambda fs: fs.total_size_bytes(), reverse=True)
    bins: List[list] = [[] for _ in range(n_bins)]
    bin_sizes = [0] * n_bins
    for fs in sorted_slices:
        lightest = min(range(n_bins), key=lambda i: bin_sizes[i])
        bins[lightest].append(fs)
        bin_sizes[lightest] += fs.total_size_bytes()
    return [b for b in bins if b]


class HudiDatasource(Datasource):
    """Hudi datasource, for reading Apache Hudi tables.

    Backed by `hudi-rs <https://github.com/apache/hudi-rs>`_ via the ``hudi``
    Python package (>= 0.5).
    """

    def __init__(
        self,
        table_uri: str,
        query_type: str = "snapshot",
        *,
        filters: Optional[List[Tuple[str, str, str]]] = None,
        columns: Optional[List[str]] = None,
        as_of_timestamp: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        hudi_options: Optional[Dict[str, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        _check_import(self, module="hudi", package="hudi")
        _check_hudi_version()

        from hudi import HudiQueryType

        try:
            self._query_type = getattr(HudiQueryType, query_type.capitalize())
        except AttributeError:
            raise ValueError(f"Unsupported query type: {query_type!r}.")
        self._table_uri = table_uri
        self._filters = list(filters) if filters else []
        self._columns = list(columns) if columns else None
        self._as_of_timestamp = as_of_timestamp
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._hudi_options = dict(hudi_options) if hudi_options else {}
        self._storage_options = storage_options or {}
        self._hudi_table = None

    def _build_table(self):
        if self._hudi_table is None:
            from hudi import HudiTableBuilder

            self._hudi_table = (
                HudiTableBuilder.from_base_uri(self._table_uri)
                .with_hudi_options(self._hudi_options)
                .with_storage_options(self._storage_options)
                .build()
            )
        return self._hudi_table

    def _build_read_options(self):
        """Build a ``HudiReadOptions`` via the chainable builders."""
        from hudi import HudiReadOptions

        options = HudiReadOptions(
            filters=self._filters or None,
            projection=self._columns,
            hudi_options=self._hudi_options or None,
        ).with_query_type(self._query_type)

        if self._as_of_timestamp is not None:
            options = options.with_as_of_timestamp(self._as_of_timestamp)
        if self._start_timestamp is not None:
            options = options.with_start_timestamp(self._start_timestamp)
        if self._end_timestamp is not None:
            options = options.with_end_timestamp(self._end_timestamp)
        return options

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List["ReadTask"]:
        import pyarrow

        hudi_table = self._build_table()
        read_options = self._build_read_options()

        logger.info(
            "Planning Hudi %s read on %s with parallelism=%d",
            self._query_type.value,
            self._table_uri,
            parallelism,
        )

        flat_slices = list(hudi_table.get_file_slices(read_options))
        if not flat_slices:
            return []
        n_bins = max(1, min(parallelism, len(flat_slices)))
        file_slices_splits = _bin_pack_by_size(flat_slices, n_bins)

        schema = hudi_table.get_schema_with_meta_fields()
        if self._columns:
            available = set(schema.names)
            unknown = [c for c in self._columns if c not in available]
            if unknown:
                raise ValueError(
                    f"Columns {unknown} not found in schema. "
                    f"Available columns: {sorted(available)}"
                )
            schema = pyarrow.schema([schema.field(c) for c in self._columns])

        table_uri = self._table_uri
        hudi_opts = dict(self._hudi_options)
        storage_opts = dict(self._storage_options)
        worker_opts_dict = dict(read_options.hudi_options)
        worker_filters = list(read_options.filters)
        worker_projection = (
            list(read_options.projection) if read_options.projection else None
        )

        def _make_read_fn(slice_paths: List[Tuple[str, List[str]]]):
            def _read() -> Iterator["pyarrow.Table"]:
                from hudi import HudiReadOptions, HudiTableBuilder

                table = (
                    HudiTableBuilder.from_base_uri(table_uri)
                    .with_hudi_options(hudi_opts)
                    .with_storage_options(storage_opts)
                    .build()
                )
                read_opts = HudiReadOptions(
                    filters=worker_filters or None,
                    projection=worker_projection,
                    hudi_options=worker_opts_dict,
                )
                reader = table.create_file_group_reader_with_options(
                    read_options=read_opts,
                )
                for base_path, log_paths in slice_paths:
                    stream = reader.read_file_slice_from_paths_stream(
                        base_path, log_paths, read_opts
                    )
                    for batch in stream:
                        yield pyarrow.Table.from_batches([batch])

            return _read

        read_tasks: List[ReadTask] = []
        for split in file_slices_splits:
            slice_paths: List[Tuple[str, List[str]]] = []
            num_rows: Optional[int] = 0
            size_bytes = 0
            input_files: List[str] = []
            for fs in split:
                base_rel = fs.base_file_relative_path()
                log_rels = list(fs.log_files_relative_paths())
                slice_paths.append((base_rel, log_rels))
                inmem = fs.base_file_byte_size
                size_bytes += inmem if inmem > 0 else fs.total_size_bytes()
                if num_rows is not None:
                    if fs.num_records > 0:
                        num_rows += fs.num_records
                    else:
                        num_rows = None
                input_files.append(os.path.join(table_uri, base_rel))
                input_files.extend(os.path.join(table_uri, p) for p in log_rels)

            if not slice_paths:
                continue

            metadata = BlockMetadata(
                num_rows=num_rows or None,
                input_files=input_files,
                size_bytes=size_bytes or None,
                exec_stats=None,
            )

            read_tasks.append(
                ReadTask(
                    read_fn=_make_read_fn(slice_paths),
                    metadata=metadata,
                    schema=schema,
                    per_task_row_limit=per_task_row_limit,
                )
            )

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        try:
            stats = self._build_table().compute_table_stats(self._build_read_options())
            if stats is None:
                return None
            _, byte_size = stats
            return byte_size
        except Exception as e:
            logger.debug("compute_table_stats() failed: %s", e)
            return None
