import copy
from typing import Iterator

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow.fs import LocalFileSystem
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.common.file_reader import FileReader
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data.datasource.file_based_datasource import _add_partitions_to_table
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches; this reader does not provide ORC-native stripe pruning.
    Partitioned reads validate and synthesize partition values before projection.
    """

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Project partitioned reads after validating and synthesizing columns."""
        reader = self
        if self._columns is not None and self._partition_parser is not None:
            reader = copy.copy(self)
            reader._columns = None

        for table in FileReader.read(reader, input_split):
            if reader is not self:
                assert self._columns is not None
                produced = set(table.column_names)
                table = table.select(
                    [name for name in self._columns if name in produced]
                )
                if table.num_columns == 0 and table.num_rows > 0:
                    table = table.append_column(
                        _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                        pa.nulls(table.num_rows),
                    )
            elif self._columns is None and self._schema is not None:
                produced = set(table.column_names)
                schema_names = self._schema.names
                schema_name_set = set(schema_names)
                column_names = [name for name in schema_names if name in produced]
                column_names.extend(
                    name for name in table.column_names if name not in schema_name_set
                )
                table = table.select(column_names)
            yield table

    @override
    def _iter_fragment_tables(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> Iterator[pa.Table]:
        if self._partition_parser is None:
            yield from super()._iter_fragment_tables(fragment, scanner_kwargs)
            return

        partitions = self._partition_parser(fragment.path)
        physical_schema = fragment.physical_schema
        schema = self._schema if self._schema is not None else physical_schema
        synthesized = {column.name for column in self._synthesized_columns}
        schema = pa.schema([field for field in schema if field.name not in synthesized])
        # Keep real partition columns long enough to enforce V1's consistency
        # check. A missing column is only an Arrow null-fill placeholder.
        for name in partitions:
            index = physical_schema.get_field_index(name)
            if index != -1:
                field = physical_schema.field(index)
                index = schema.get_field_index(name)
                schema = (
                    schema.append(field) if index == -1 else schema.set(index, field)
                )

        if any(physical_schema.get_field_index(name) != -1 for name in partitions):
            import pyarrow.orc as orc

            # V1 validates a whole stripe. Validating individual scan batches
            # would reject an all-null batch within an otherwise valid stripe.
            filesystem = self._filesystem or LocalFileSystem()
            columns = [
                name
                for name in schema.names
                if physical_schema.get_field_index(name) != -1
            ]
            with filesystem.open_input_file(fragment.path) as source:
                orc_file = orc.ORCFile(source)
                for stripe_index in range(orc_file.nstripes):
                    table = pa.Table.from_batches(
                        [orc_file.read_stripe(stripe_index, columns=columns)]
                    )
                    if table.num_rows == 0:
                        continue
                    raise_on_pickle_object_columns(table)
                    table = _add_partitions_to_table(table, partitions)
                    # Reuse Arrow's schema alignment and batch sizing after
                    # validation, including null-fill for missing data fields.
                    for stripe in pds.dataset(table).get_fragments():
                        scanner = stripe.scanner(**scanner_kwargs, schema=schema)
                        for tagged in scanner.scan_batches():
                            yield pa.Table.from_batches([tagged.record_batch])
            return

        scanner = fragment.scanner(**scanner_kwargs, schema=schema)
        for tagged in scanner.scan_batches():
            yield pa.Table.from_batches([tagged.record_batch])

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
