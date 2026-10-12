"""The MCAP ``Scanner``: the read's options plus the optimizer's pushdowns."""

from dataclasses import dataclass, replace
from typing import FrozenSet, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    DEFAULT_MAX_LEAD_IN_NS,
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

    Holds the message selection and row granularity from ``read_mcap``, the
    topics it lists as video (``video_topics``), and the pushdowns the
    optimizer applies: column pruning and a per-task row limit. A pruned read
    neither builds nor JSON-decodes the columns it drops. Partition pruning
    comes from :class:`FileScanner`. Other filters are not pushed down.

    At ``message`` granularity, the planned ``schema`` fixes what ``data``
    holds in every block: decoded JSON values when every selected channel of
    the sampled files is JSON-encoded, and the payload bytes otherwise. With
    ``video`` at ``window`` granularity, ``decoded_topics`` names the topics
    the planned schema gives frame columns.
    """

    schema: pa.Schema
    selection: MCAPSelection = MCAPSelection()
    granularity: str = MESSAGE_GRANULARITY
    window: Optional[WindowSpec] = None
    video: Optional[VideoOptions] = None
    video_topics: FrozenSet[str] = frozenset()
    decoded_topics: Tuple[str, ...] = ()
    include_metadata: bool = True
    include_row_id: bool = False
    log_time_order: bool = True
    filesystem: Optional[FileSystem] = None
    columns: Optional[Tuple[str, ...]] = None
    limit: Optional[int] = None
    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    target_block_size: Optional[int] = None
    max_row_bytes: int = DEFAULT_MAX_ROW_BYTES
    max_lead_in_ns: int = DEFAULT_MAX_LEAD_IN_NS

    def read_schema(self) -> pa.Schema:
        """Return the dataset schema after column pruning.

        ``schema`` already includes the partition and synthesized columns, so a
        projection selects them like any other column.
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
        # An empty projection, as for ``count()``, stays empty.
        if self.columns is not None:
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

    @property
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
            video_topics=self.video_topics,
            decoded_topics=self.decoded_topics,
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
            decode_json=self.decodes_json,
            max_row_bytes=self.max_row_bytes,
            max_lead_in_ns=self.max_lead_in_ns,
        )
