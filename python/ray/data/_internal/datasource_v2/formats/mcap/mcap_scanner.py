from dataclasses import dataclass, replace
from typing import List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.common.pushdown_utils import combine_predicates
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ATTACHMENT_GRANULARITY,
    MESSAGE_GRANULARITY,
    METADATA_GRANULARITY,
    MCAPSelection,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_pushdown import (
    narrow_selection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import (
    DEFAULT_MAX_ROW_BYTES,
    MCAPReader,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class MCAPScanner(
    FileScanner, SupportsFilterPushdown, SupportsColumnPruning, SupportsLimitPushdown
):
    """Scanner for MCAP files on Datasource V2.

    Carries the message selection and row granularity fixed by ``read_mcap``
    and the pushdowns the optimizer applies: column pruning (a pruned read
    skips building, and for JSON channels decoding, the columns it will not
    return), a per-task row limit, and, at message granularity, filters on
    ``topic`` and ``log_time``, which fold into the selection so listing
    prunes files and chunks with them. Partition pruning comes from
    :class:`FileScanner`.

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
    # The conjuncts folded into ``selection`` by :meth:`push_filters`; what the
    # planner hands the indexer so listing prunes on the same selection.
    predicate: Optional[Expr] = None

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
    def metadata_row_count_is_exact(self) -> bool:
        """Whether ``count()`` can be answered from the summaries.

        A summary's ``Statistics`` counts messages per channel, so a selection
        by topic or message type is exact from metadata, and it counts
        attachments and metadata records outright. A time range is not: the
        statistics say nothing about how many records fall inside it. Coarse
        rows are windows, topics or files, which no statistic counts.
        """
        if self.limit is not None or self.partition_predicate is not None:
            return False
        if self.granularity == METADATA_GRANULARITY:
            return True
        if self.granularity in (MESSAGE_GRANULARITY, ATTACHMENT_GRANULARITY):
            if self.video is not None and self.video.decode:
                # Rows are decoded frames: ``fps`` thins them and a decoder may
                # drop what it cannot decode.
                return False
            return self.selection.time_range is None
        return False

    @override
    def push_filters(self, predicate: Expr) -> Tuple["MCAPScanner", Optional[Expr]]:
        """Fold ``topic`` and ``log_time`` conjuncts into the selection.

        Only message rows have those as plain columns; a coarse row carries
        them as lists, so at any other granularity the whole predicate stays
        with the ``Filter`` above the read.
        """
        if self.granularity != MESSAGE_GRANULARITY:
            return self, predicate
        narrowed = narrow_selection(self.selection, predicate)
        if narrowed.pushed is None:
            return self, predicate
        return (
            replace(
                self,
                selection=narrowed.selection,
                predicate=combine_predicates(self.predicate, narrowed.pushed),
            ),
            narrowed.residual,
        )

    @override
    def pushed_predicate(self) -> Optional[Expr]:
        return self.predicate

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
