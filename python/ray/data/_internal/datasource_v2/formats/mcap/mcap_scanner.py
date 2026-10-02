from dataclasses import dataclass, replace
from typing import List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MESSAGE_GRANULARITY,
    MCAPSelection,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import (
    DEFAULT_MAX_ROW_BYTES,
    MCAPReader,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class MCAPScanner(FileScanner, SupportsColumnPruning, SupportsLimitPushdown):
    """Scanner for MCAP files on Datasource V2.

    Carries the message selection and row granularity fixed by ``read_mcap``
    and the pushdowns the optimizer applies: column pruning (a pruned read
    skips building, and for JSON channels decoding, the columns it will not
    return) and a per-task row limit. Partition pruning comes from
    :class:`FileScanner`. Filter pushdown is not offered yet; a ``Filter``
    above the read applies ``ds.filter``.

    The planned ``schema`` also fixes what ``data`` holds at ``message``
    granularity: decoded JSON values when the datasource found every selected
    channel of its sample to be JSON-encoded, the payload bytes otherwise.
    Every reader follows that one decision, so no block mixes the two.
    """

    schema: pa.Schema
    selection: MCAPSelection = MCAPSelection()
    granularity: str = MESSAGE_GRANULARITY
    window: Optional[WindowSpec] = None
    video: Optional[VideoOptions] = None
    include_metadata: bool = True
    include_row_id: bool = False
    log_time_order: bool = True
    filesystem: Optional[FileSystem] = None
    columns: Optional[Tuple[str, ...]] = None
    limit: Optional[int] = None
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    target_block_size: Optional[int] = None
    max_row_bytes: int = DEFAULT_MAX_ROW_BYTES

    def read_schema(self) -> pa.Schema:
        """The dataset schema after column pruning.

        Partition and synthesized columns are part of ``schema`` already (the
        datasource appends them at inference), so a projection selects among
        them like any other column.
        """
        if self.columns is None:
            return self.schema
        fields = []
        for name in self.columns:
            idx = self.schema.get_field_index(name)
            assert idx >= 0, f"Column {name} not found in schema"
            fields.append(self.schema.field(idx))
        return pa.schema(fields)

    @override
    def prune_columns(self, columns: List[str]) -> "MCAPScanner":
        if self.columns:
            existing = set(self.columns)
            columns = [c for c in columns if c in existing]
        return replace(self, columns=tuple(columns))

    @override
    def pruned_column_names(self) -> Optional[Tuple[str, ...]]:
        return self.columns

    @override
    def push_limit(self, limit: int) -> "MCAPScanner":
        current = self.limit
        return replace(
            self, limit=min(current, limit) if current is not None else limit
        )

    @override
    def pushed_limit(self) -> Optional[int]:
        return self.limit

    def decodes_json(self) -> bool:
        """Whether ``data`` holds decoded JSON values rather than bytes."""
        idx = self.schema.get_field_index("data")
        return idx != -1 and self.schema.field(idx).type != pa.binary()

    def create_reader(self) -> MCAPReader:
        return MCAPReader(
            selection=self.selection,
            granularity=self.granularity,
            window=self.window,
            video=self.video,
            include_metadata=self.include_metadata,
            include_row_id=self.include_row_id,
            log_time_order=self.log_time_order,
            columns=list(self.columns) if self.columns is not None else None,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            synthesized_columns=self.synthesized_columns,
            target_block_size=self.target_block_size,
            schema=self.schema,
            decode_json=self.decodes_json(),
            max_row_bytes=self.max_row_bytes,
        )
