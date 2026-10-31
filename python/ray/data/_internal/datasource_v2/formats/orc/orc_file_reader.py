"""ORC file reader with stripe-level read-unit splitting.

Extends the shared :class:`FileReader` to handle ORC-specific stripe-level
reads via :meth:`pyarrow.orc.ORCFile.read_stripe` when the input manifest
carries per-stripe :class:`FileChunk` metadata. Whole-file manifest rows
(``chunk_metadata is None``) fall through to pyarrow's dataset path
unchanged, preserving compatibility with single-stripe files and any other
listing path that still emits one row per file.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow import orc
from pyarrow.fs import FileSystem, LocalFileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileReader
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    _split_predicate_by_columns,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files, one stripe at a time when the manifest names them.

    When the manifest carries whole-file rows (``chunk_metadata is None``)
    the reader falls through to pyarrow's dataset path unchanged -- column
    projection, partition pruning, predicate pushdown, and pickled-object
    safety checks all flow through :meth:`FileReader._iter_fragment_tables`.

    When the manifest carries per-stripe :class:`FileChunk` rows the reader
    bypasses pyarrow.dataset for those rows and reads each stripe directly
    via :func:`pyarrow.orc.ORCFile.read_stripe`, replaying the same
    projection / filter / partition-synthesis logic the pyarrow path
    applies. This lets :class:`OrcFileIndexer` split a multi-stripe ORC
    file across several Ray read tasks, which would otherwise be indivisible.
    """

    # ------------------------------------------------------------------
    # Public read override -- fan whole-file vs chunked rows.
    # ------------------------------------------------------------------

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Route whole-file manifest rows to the pyarrow path and chunked rows
        to the stripe-direct reader, preserving OrcScanner's declared column order
        after per-file partition synthesis.

        A single split can carry both whole-file and stripe-chunked rows
        (OnlineBinPacker may coalesce them). We therefore apply the scanner
        push-down ``limit`` exactly once at this boundary, so it is honored
        across both paths as a global cap on rows emitted from the split.
        """
        if len(input_split) == 0:
            return

        paths = input_split.paths.tolist()
        chunk_mds = input_split.file_chunk_metadatas.tolist()

        # Classify manifest rows.
        whole_indices: list[int] = []
        chunked_rows: list[tuple[int, str, dict | None]] = []
        for i, (path, md) in enumerate(zip(paths, chunk_mds)):
            if md is None or "unit_ids" not in md:
                whole_indices.append(i)
            else:
                chunked_rows.append((i, str(path), md))

        filesystem = self._filesystem or LocalFileSystem()
        limit = self._limit
        rows_read = 0

        def _apply_limit(table: pa.Table) -> pa.Table | None:
            """Slice ``table`` to respect the push-down ``limit``, if any.

            Returns None when the limit has already been exhausted.
            """
            nonlocal rows_read
            if limit is None:
                return table
            if rows_read >= limit:
                return None
            if len(table) > limit - rows_read:
                table = table.slice(0, limit - rows_read)
            rows_read += len(table)
            return table

        # (1) Whole-file rows go through the pyarrow dataset path unchanged.
        if whole_indices:
            whole_paths = [paths[i] for i in whole_indices]
            whole_sizes = list(input_split.file_sizes[whole_indices])
            whole_mds = [None] * len(whole_indices)
            from ray.data._internal.datasource_v2.interfaces.file_manifest import (
                FileManifest as FM,
            )

            sub_manifest = FM.construct_manifest(
                paths=[str(p) for p in whole_paths],
                sizes=[int(s) for s in whole_sizes],
                chunk_metadatas=whole_mds,
            )
            schema = self._schema
            for table in self._read_whole_file_manifest(sub_manifest):
                if self._columns is None and schema is not None:
                    produced = set(table.column_names)
                    schema_names = schema.names
                    schema_name_set = set(schema_names)
                    column_names = [name for name in schema_names if name in produced]
                    column_names.extend(
                        name
                        for name in table.column_names
                        if name not in schema_name_set
                    )
                    table = table.select(column_names)
                limited = _apply_limit(table)
                if limited is None:
                    return
                yield limited

        # (2) Chunked rows -> stripe-direct reader.
        if chunked_rows and not (limit is not None and rows_read >= limit):
            for table in self._read_chunked_stripe_rows(chunked_rows, filesystem):
                limited = _apply_limit(table)
                if limited is None:
                    return
                yield limited

    # ------------------------------------------------------------------
    # Whole-file manifest (delegates to FileReader.read). Split out so the
    # original override-order re-shaping logic can reuse it without double
    # wrapping the ``super()`` call.
    # ------------------------------------------------------------------

    def _read_whole_file_manifest(self, manifest: FileManifest) -> Iterator[pa.Table]:
        yield from super().read(manifest)

    # ------------------------------------------------------------------
    # Stripe-direct path for chunked manifest rows.
    # ------------------------------------------------------------------

    def _read_chunked_stripe_rows(
        self,
        rows: list[tuple[int, str, dict]],
        filesystem: FileSystem,
    ) -> Iterator[pa.Table]:
        """Read the stripe units named by ``rows``.

        Each ``row`` is ``(manifest_row_idx, path, chunk_metadata_dict)``.
        ``chunk_metadata_dict['unit_ids']`` lists stripe indices for the row;
        a row may carry multiple stripes (OnlineBinPacker coalesced them into
        one manifest row when the file has many small stripes).

        The scanner push-down limit is applied by :meth:`read` before this
        method is called; this method yields every stripe table it produces.
        """
        columns = self._columns
        predicate = self._predicate
        partition_parser = self._partition_parser
        synthesized = {c.name for c in self._synthesized_columns}

        for _, path, md in rows:
            chunk = FileChunk.from_metadata(md)
            stripe_ids = chunk.unit_ids

            # Open once per file so we read its schema and footer once.
            source: str | object | None = None
            of = None
            try:
                if filesystem is None or isinstance(filesystem, LocalFileSystem):
                    source = path
                else:
                    source = filesystem.open_input_file(path)
                of = orc.ORCFile(source)
                file_schema = of.schema
            except Exception:
                # Any failure while opening the ORC file must still close
                # the remote source handle we just opened.
                if (
                    source is not None
                    and filesystem is not None
                    and not isinstance(filesystem, LocalFileSystem)
                    and hasattr(source, "close")
                ):
                    source.close()
                raise

            close_source = (
                filesystem is not None
                and not isinstance(filesystem, LocalFileSystem)
                and hasattr(source, "close")
            )
            try:
                for stripe_idx in stripe_ids:
                    yield from self._read_one_stripe(
                        of=of,
                        path=path,
                        stripe_idx=int(stripe_idx),
                        file_schema=file_schema,
                        columns=columns,
                        predicate=predicate,
                        partition_parser=partition_parser,
                        synthesized=synthesized,
                    )
            finally:
                if close_source:
                    source.close()

    def _read_one_stripe(
        self,
        *,
        of: orc.ORCFile,
        path: str,
        stripe_idx: int,
        file_schema: pa.Schema,
        columns: list[str] | None,
        predicate,
        partition_parser,
        synthesized: set[str],
    ) -> Iterator[pa.Table]:
        """Read one stripe, apply predicates / projection, yield tables."""
        schema = self._schema
        # Partition values from the path (same logic as _iter_fragment_tables).
        partitions = partition_parser(path) if partition_parser is not None else {}

        # Build the logical schema used for projection/filter synthesis.
        out_schema = schema if schema is not None else file_schema
        out_schema = pa.schema([f for f in out_schema if f.name not in synthesized])
        for name, value in partitions.items():
            if out_schema.get_field_index(name) == -1:
                out_schema = out_schema.append(
                    pa.field(
                        name,
                        self._broadcast_partition_value(name, value, 0).type,
                    )
                )

        out_names = set(out_schema.names)
        output_columns = (
            out_schema.names
            if columns is None
            else [name for name in columns if name in out_names]
        )
        filter_columns = (
            set(get_column_references(predicate)) if predicate is not None else set()
        )
        # Only physical (file) columns are read from the stripe; partition
        # and synthesized columns are appended post-read.
        physical_required = list(
            dict.fromkeys(
                name
                for name in (output_columns + list(filter_columns))
                if name not in partitions and name not in synthesized
            )
        )
        # Intersect with the file's own physical columns so that schema-
        # evolved files (missing a column present in another file of the
        # same dataset) don't make ``read_stripe`` raise. An empty list is
        # intentionally preserved -- it signals "don't decode anything"
        # rather than the pyarrow default of "decode every column" (None).
        file_physical_names = set(file_schema.names)
        physical_required = [c for c in physical_required if c in file_physical_names]

        # Apply pushdown predicate splitting (partition vs data vs residual).
        data_predicate = predicate
        residual_predicate = None
        partition_matches = True
        if predicate is not None:
            split = _split_predicate_by_columns(predicate, set(partitions))
            data_predicate = split.data_predicate
            residual_predicate = split.residual_predicate
            if split.partition_predicate is not None:
                partition_table = pa.table(
                    {
                        name: self._broadcast_partition_value(name, value, 1)
                        for name, value in partitions.items()
                    }
                )
                partition_matches = (
                    partition_table.filter(
                        split.partition_predicate.to_pyarrow()
                    ).num_rows
                    > 0
                )

        if not partition_matches:
            return

        # Pickled-object safety check on the columns we will decode.
        decoded_schema = pa.schema(
            [f for f in file_schema if f.name in physical_required]
        )
        raise_on_pickle_object_columns(pa.Table.from_batches([], schema=decoded_schema))

        try:
            if not physical_required:
                # ``columns=[]`` makes ``read_stripe`` return a 0-row batch,
                # which would silently drop rows for count-style reads or
                # projections that need no physical file columns (e.g. only
                # path or partition columns). Pass ``None`` so pyarrow
                # decodes everything; the projection step below will drop
                # the extras we don't need.
                batch = of.read_stripe(stripe_idx, columns=None)
            else:
                batch = of.read_stripe(stripe_idx, columns=physical_required)
        except Exception as exc:
            # Stripe-level read failure should surface so the task can be
            # retried; swallowing would silently drop rows.
            logger.warning(
                "ORC stripe read failed for %s stripe %d: %s",
                path,
                stripe_idx,
                exc,
            )
            raise

        if batch.num_rows == 0:
            return

        table = pa.Table.from_batches([batch])

        # Cast physical columns to the unified schema's types so files
        # with different per-column types (schema evolution) all match
        # ``schema``. The whole-file path gets this cast from pyarrow's
        # dataset scanner; the stripe-direct path must do it explicitly.
        if schema is not None:
            for col_name in table.column_names:
                idx = schema.get_field_index(col_name)
                if idx != -1:
                    target_type = schema.field(idx).type
                    if table.schema.field(col_name).type != target_type:
                        try:
                            table = table.set_column(
                                table.schema.get_field_index(col_name),
                                col_name,
                                table.column(col_name).cast(target_type),
                            )
                        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                            # If pyarrow can't cast (e.g. lossy downcast),
                            # leave the file's native type -- downstream concat
                            # will surface the conflict if any.
                            logger.debug(
                                "Could not cast ORC stripe column %r from %s to %s",
                                col_name,
                                table.schema.field(col_name).type,
                                target_type,
                            )

        # Apply data predicate on the physical columns.
        if data_predicate is not None:
            # If schema evolution removed columns this file doesn't have, fill
            # them with nulls so pyarrow can still evaluate the filter (matching
            # what the whole-file pyarrow.dataset path does).
            data_filter_cols = set(get_column_references(data_predicate))
            for col_name in data_filter_cols:
                if col_name not in table.column_names and col_name not in partitions:
                    field = None
                    if schema is not None:
                        idx = schema.get_field_index(col_name)
                        if idx != -1:
                            field = schema.field(idx)
                    if field is None:
                        idx = file_schema.get_field_index(col_name)
                        if idx != -1:
                            field = file_schema.field(idx)
                    col_type = field.type if field is not None else pa.null()
                    table = table.append_column(
                        col_name, pa.nulls(table.num_rows, type=col_type)
                    )
            pyarrow_filter = data_predicate.to_pyarrow()
            table = table.filter(pyarrow_filter)
            if table.num_rows == 0:
                return

        # Append partition values and residual filter.
        for name, value in partitions.items():
            if name in output_columns or residual_predicate is not None:
                if name in table.column_names:
                    table = table.drop([name])
                table = table.append_column(
                    name,
                    self._broadcast_partition_value(name, value, table.num_rows),
                )

        if residual_predicate is not None:
            table = table.filter(residual_predicate.to_pyarrow())
            if table.num_rows == 0:
                return

        # Append synthesized columns (e.g. ``path`` from include_paths=True).
        # The whole-file path handles these in FileReader's outer iterator,
        # but the stripe-direct reader bypasses that iterator.
        for column in self._synthesized_columns:
            if self._columns is not None and column.name not in self._columns:
                # Projection pushdown excluded this column; skip.
                continue
            if column.name in table.column_names:
                table = table.drop([column.name])
            table = table.append_column(
                column.name,
                pa.repeat(pa.scalar(path, type=column.type), table.num_rows),
            )

        # Fill any remaining missing output columns with nulls. Multi-file
        # ORC datasets can evolve schemas -- a column present in one file
        # may be absent from another -- so the unified output schema must
        # null-fill rather than raise.
        if self._columns is not None:
            output_cols_to_emit = self._columns
        else:
            output_cols_to_emit = list(dict.fromkeys(out_schema.names + output_columns))
        for col_name in output_cols_to_emit:
            if col_name in table.column_names:
                continue
            # Find a type we can use for the null fill. Prefer the unified
            # schema; fall back to the file schema if the column is there;
            # default to null() otherwise.
            field = None
            if schema is not None:
                idx = schema.get_field_index(col_name)
                if idx != -1:
                    field = schema.field(idx)
            if field is None:
                idx = file_schema.get_field_index(col_name)
                if idx != -1:
                    field = file_schema.field(idx)
            col_type = field.type if field is not None else pa.null()
            table = table.append_column(
                col_name, pa.nulls(table.num_rows, type=col_type)
            )

        # Reorder to the requested column order and drop extras. Mirror
        # the whole-file path's logic: when no projection is pushed but we
        # have a schema, reorder to schema names and keep extras; when a
        # projection is pushed, select only the requested columns.
        if columns is None and schema is not None:
            produced = set(table.column_names)
            schema_names = schema.names
            schema_name_set = set(schema_names)
            column_names = [name for name in schema_names if name in produced]
            column_names.extend(
                name for name in table.column_names if name not in schema_name_set
            )
            table = table.select(column_names)
        elif columns is not None:
            produced = set(table.column_names)
            projected = [c for c in columns if c in produced]
            table = table.select(projected)

        if table.num_columns == 0 and table.num_rows > 0:
            from ray.data._internal.arrow_block import (
                _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
            )

            table = table.append_column(
                _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                pa.nulls(table.num_rows),
            )

        self._on_batch_read(table)
        yield table

    # ------------------------------------------------------------------
    # Default fragment reader (unchanged).
    # ------------------------------------------------------------------

    @override
    def _iter_fragment_tables(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> Iterator[pa.Table]:
        """Read one ORC fragment and apply its path-derived partition values.

        PyArrow reads physical columns and evaluates data predicates. Partition
        values are synthesized from the path, then any residual predicate is
        evaluated on the resulting table.
        """
        if self._partition_parser is None:
            yield from super()._iter_fragment_tables(fragment, scanner_kwargs)
            return

        partitions = self._partition_parser(fragment.path)
        physical_schema = fragment.physical_schema
        schema = self._schema
        if schema is None:
            schema = physical_schema
        synthesized = {column.name for column in self._synthesized_columns}
        schema = pa.schema([field for field in schema if field.name not in synthesized])

        for name, value in partitions.items():
            if schema.get_field_index(name) == -1:
                schema = schema.append(
                    pa.field(name, self._broadcast_partition_value(name, value, 0).type)
                )

        schema_names = set(schema.names)
        output_columns = (
            schema.names
            if self._columns is None
            else [name for name in self._columns if name in schema_names]
        )
        filter_columns = (
            get_column_references(self._predicate)
            if self._predicate is not None
            else []
        )
        required_columns = list(dict.fromkeys(output_columns + filter_columns))

        data_predicate = self._predicate
        residual_predicate = None
        partition_matches = True
        if self._predicate is not None:
            split = _split_predicate_by_columns(self._predicate, set(partitions))
            data_predicate = split.data_predicate
            residual_predicate = split.residual_predicate
            if split.partition_predicate is not None:
                partition_table = pa.table(
                    {
                        name: self._broadcast_partition_value(name, value, 1)
                        for name, value in partitions.items()
                    }
                )
                partition_matches = (
                    partition_table.filter(
                        split.partition_predicate.to_pyarrow()
                    ).num_rows
                    > 0
                )

        # The shared kwargs are also used by concurrent fragment reads.
        scan_kwargs = dict(scanner_kwargs)

        if not partition_matches:
            return

        required_names = set(required_columns)
        # Partition fields come from the path, so only read physical columns.
        scan_kwargs["columns"] = [
            name for name in required_columns if name not in partitions
        ]
        # Start from the full predicate in scanner_kwargs. After splitting,
        # only the data part belongs in PyArrow; other parts are handled separately.
        if data_predicate is not self._predicate:
            scan_kwargs["filter"] = (
                data_predicate.to_pyarrow() if data_predicate is not None else None
            )
        file_schema = pa.schema(
            [field for field in schema if field.name not in partitions]
        )
        # Filtering must not hide an unsafe column that this scan will decode.
        decoded_names = set(scan_kwargs["columns"])
        decoded_schema = pa.schema(
            [field for field in physical_schema if field.name in decoded_names]
        )
        raise_on_pickle_object_columns(pa.Table.from_batches([], schema=decoded_schema))
        scanner = fragment.scanner(**scan_kwargs, schema=file_schema)
        residual_filter = (
            residual_predicate.to_pyarrow() if residual_predicate is not None else None
        )
        for tagged in scanner.scan_batches():
            table = pa.Table.from_batches([tagged.record_batch])
            for name, value in partitions.items():
                if name in required_names:
                    table = table.append_column(
                        name,
                        self._broadcast_partition_value(name, value, table.num_rows),
                    )
            if residual_filter is not None:
                table = table.filter(residual_filter)
            yield table

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
