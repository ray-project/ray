"""The MCAP ``Scanner``: the read's options plus the optimizer's pushdowns."""

from dataclasses import dataclass, replace
from typing import FrozenSet, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_scanner import FileScanner
from ray.data._internal.datasource_v2.common.pushdown_utils import combine_predicates
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ATTACHMENT_GRANULARITY,
    DEFAULT_MAX_LEAD_IN_NS,
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

    Holds the message selection and row granularity from ``read_mcap``, the
    topics it lists as video (``video_topics``), and the pushdowns the
    optimizer applies: column pruning, a per-task row limit and, at message
    granularity, filters on ``topic`` and ``log_time``. Those filters fold into
    the selection, so listing prunes files and chunks with them. A pruned read
    neither builds nor JSON-decodes the columns it drops. Partition pruning
    comes from :class:`FileScanner`.

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
    # The conjuncts :meth:`push_filters` folded into ``selection``. The planner
    # hands them to the indexer, so listing prunes on the same selection.
    predicate: Optional[Expr] = None

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
    def metadata_row_count_is_exact(self) -> bool:
        """Whether ``count()`` can be answered from the summaries.

        A summary's ``Statistics`` counts messages per channel, so a selection
        by topic or message type is exact from metadata. It also counts
        attachments and metadata records. It cannot say how many records fall
        inside a time range, and it does not count window, topic or file rows.
        """
        if self.limit is not None or self.partition_predicate is not None:
            return False
        if self.granularity == METADATA_GRANULARITY:
            return True
        if self.granularity in (MESSAGE_GRANULARITY, ATTACHMENT_GRANULARITY):
            if self.video is not None:
                # Decoded rows are frames, not messages: ``fps`` thins them and
                # undecodable ones are dropped.
                return False
            return self.selection.time_range is None
        return False

    @override
    def push_filters(self, predicate: Expr) -> Tuple["MCAPScanner", Optional[Expr]]:
        """Fold ``topic`` and ``log_time`` conjuncts into the selection.

        Only message rows have both as scalar columns. A coarse row holds
        ``log_time`` as a list, so at any other granularity the whole predicate
        stays with the ``Filter`` above the read.
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
