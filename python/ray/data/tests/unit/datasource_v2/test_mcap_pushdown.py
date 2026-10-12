"""Unit tests for filter pushdown and count-from-statistics on the V2 MCAP reader."""

import importlib.util
import os

import pytest

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_pushdown import (
    narrow_selection,
)
from ray.data._internal.datasource_v2.interfaces.supports_metadata import MetadataType
from ray.data.expressions import col, lit
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    BASE_TIME,
    STEP,
    list_manifests,
    read_all,
    round_robin_messages,
    scanner_for,
    write_mcap,
    write_mcap_without_summary_schemas,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)


# -- narrow_selection ---------------------------------------------------------


def test_narrow_selection_topic_equality_and_membership():
    """Topic equality and ``is_in`` fold into the selection's topics, intersected
    with the topics it already has."""
    base = MCAPSelection()
    narrowed = narrow_selection(base, col("topic") == "/a")
    assert narrowed.selection.topics == frozenset({"/a"})
    assert narrowed.residual is None
    assert narrowed.pushed is not None

    narrowed = narrow_selection(base, col("topic").is_in(["/a", "/b"]))
    assert narrowed.selection.topics == frozenset({"/a", "/b"})

    # Literal on the left, and intersection with an existing topic set.
    narrowed = narrow_selection(
        MCAPSelection.create(["/a", "/b"], None, None), lit("/b") == col("topic")
    )
    assert narrowed.selection.topics == frozenset({"/b"})

    # Disjoint: selects nothing.
    narrowed = narrow_selection(
        MCAPSelection.create(["/a"], None, None), col("topic") == "/z"
    )
    assert narrowed.selection.topics == frozenset()


def test_narrow_selection_log_time_bounds():
    """``log_time`` comparisons fold into a half-open time range, intersected with
    the range the selection already has."""
    base = MCAPSelection()
    narrowed = narrow_selection(
        base, (col("log_time") >= 100) & (col("log_time") < 200)
    )
    assert narrowed.selection.time_range == TimeRange(100, 200)
    assert narrowed.residual is None

    # Strict and inclusive bounds become half-open ones.
    narrowed = narrow_selection(
        base, (col("log_time") > 100) & (col("log_time") <= 200)
    )
    assert narrowed.selection.time_range == TimeRange(101, 201)
    narrowed = narrow_selection(base, col("log_time") == 150)
    assert narrowed.selection.time_range == TimeRange(150, 151)
    # Reversed operands.
    narrowed = narrow_selection(base, lit(300) > col("log_time"))
    assert narrowed.selection.time_range == TimeRange(0, 300)

    # Intersects an existing range, and an empty intersection selects nothing.
    narrowed = narrow_selection(
        MCAPSelection.create(None, TimeRange(100, 200), None), col("log_time") >= 150
    )
    assert narrowed.selection.time_range == TimeRange(150, 200)
    narrowed = narrow_selection(
        MCAPSelection.create(None, TimeRange(100, 200), None), col("log_time") >= 250
    )
    assert narrowed.selection.topics == frozenset()

    # Bounds below zero: no log time is negative, so an upper bound at or
    # below zero selects nothing, and a lower bound below zero is no bound.
    # Neither may crash ``TimeRange``'s validation.
    assert narrow_selection(base, col("log_time") < 0).selection.topics == frozenset()
    assert narrow_selection(base, col("log_time") <= -5).selection.topics == frozenset()
    narrowed = narrow_selection(base, col("log_time") >= -1)
    assert narrowed.selection.time_range == TimeRange(0, 2**63 - 1)
    assert narrowed.residual is None


def test_narrow_selection_keeps_what_it_cannot_fold():
    """Conjuncts on other columns stay residual, and a predicate with nothing to
    fold leaves the selection untouched."""
    base = MCAPSelection()
    predicate = (col("topic") == "/a") & (col("sequence") > 3) & (col("log_time") < 9)
    narrowed = narrow_selection(base, predicate)
    assert narrowed.selection.topics == frozenset({"/a"})
    assert narrowed.selection.time_range == TimeRange(0, 9)
    assert narrowed.residual is not None
    assert narrowed.residual.structurally_equals(col("sequence") > 3)

    # Nothing foldable: everything stays residual, the selection is untouched.
    untouched = narrow_selection(base, col("topic") != "/a")
    assert untouched.pushed is None and untouched.residual is not None
    assert untouched.selection is base
    assert (
        narrow_selection(base, (col("topic") == "/a") | (col("log_time") < 9)).pushed
        is None
    )
    assert narrow_selection(base, col("log_time") >= 1.5).pushed is None


# -- scanner and indexer ------------------------------------------------------


def test_scanner_pushes_topic_and_time_filters(chunked_file):
    """The scanner takes topic and time filters, the indexer prunes chunks on
    them, and the reader applies them."""
    datasource = MCAPDatasourceV2([chunked_file])
    scanner = scanner_for(datasource)

    pushed, residual = scanner.push_filters(
        (col("topic") == "/a") & (col("log_time") < BASE_TIME + 4 * STEP)
    )
    assert residual is None
    assert pushed.selection.topics == frozenset({"/a"})
    assert pushed.pushed_predicate() is not None

    # The second push narrows further and keeps the residual.
    pushed2, residual2 = pushed.push_filters(
        (col("log_time") >= BASE_TIME + 1) & (col("sequence") == 0)
    )
    assert residual2.structurally_equals(col("sequence") == 0)
    assert pushed2.selection.time_range == TimeRange(
        BASE_TIME + 1, BASE_TIME + 4 * STEP
    )

    (manifest,) = list_manifests(datasource, predicate=pushed2.pushed_predicate())
    assert len(manifest) == 1  # message 3 only (/a at BASE_TIME + 3 ms)
    table = read_all(datasource, manifest, scanner=pushed2)
    assert table.column("log_time").to_pylist() == [BASE_TIME + 3 * STEP]

    # Non-foldable predicates are refused whole.
    same, back = scanner.push_filters(col("sequence") > 1)
    assert same is scanner and back is not None


def test_coarse_granularity_refuses_filter_pushdown(chunked_file):
    """A window read refuses filters, and its row count is not exact."""
    datasource = MCAPDatasourceV2(
        [chunked_file], read_granularity="window", window=WindowSpec(length_s=1)
    )
    scanner = scanner_for(datasource)

    same, residual = scanner.push_filters(col("topic") == "/a")

    assert same is scanner and residual is not None
    assert not scanner.metadata_row_count_is_exact()


def test_pushed_predicate_selecting_nothing_lists_nothing(chunked_file):
    """A pushed filter that selects no topic leaves nothing to list."""
    datasource = MCAPDatasourceV2([chunked_file], topics=["/a"])
    scanner = scanner_for(datasource)

    pushed, _ = scanner.push_filters(col("topic") == "/z")

    assert list_manifests(datasource, predicate=pushed.pushed_predicate()) == []


# -- count from statistics --------------------------------------------------


def test_count_falls_back_to_reading_for_chunk_only_channels(chunked_file, monkeypatch):
    """A file whose summary omits a channel is counted by reading the chunks a
    read reads, so ``count()`` matches the rows the read returns."""
    from mcap.reader import SeekingReader

    real_get_summary = SeekingReader.get_summary

    def without_channel_1(self):
        summary = real_get_summary(self)
        assert summary is not None and 1 in summary.channels
        summary.channels = {cid: c for cid, c in summary.channels.items() if cid != 1}
        return summary

    monkeypatch.setattr(SeekingReader, "get_summary", without_channel_1)
    datasource = MCAPDatasourceV2([chunked_file])
    scanner = scanner_for(datasource)
    sample = sample_files(
        datasource._get_file_indexer(), datasource.paths, datasource.filesystem, []
    )
    reader = scanner.create_reader()

    # The read skips the chunks that hold only the omitted channel's messages.
    rows = read_all(datasource, *list_manifests(datasource), scanner=scanner).num_rows

    assert rows == 6
    assert [m.num_rows for m in reader.read_metadata(sample)] == [rows]


@pytest.mark.parametrize("dropped", ["every channel", "one channel"])
def test_count_reads_when_channel_counts_miss_messages(
    chunked_file, monkeypatch, dropped
):
    """Per-channel counts that do not add up to ``message_count`` are not summed.

    An empty map means the counts are not available, so the file is read.
    """
    from mcap.reader import SeekingReader

    real_get_summary = SeekingReader.get_summary

    def with_counts_missing(self):
        summary = real_get_summary(self)
        assert summary is not None and summary.statistics is not None
        counts = summary.statistics.channel_message_counts
        if dropped == "every channel":
            counts.clear()
        else:
            counts.pop(next(iter(counts)))
        return summary

    monkeypatch.setattr(SeekingReader, "get_summary", with_counts_missing)
    datasource = MCAPDatasourceV2([chunked_file])
    sample = sample_files(
        datasource._get_file_indexer(), datasource.paths, datasource.filesystem, []
    )
    reader = scanner_for(datasource).create_reader()

    assert [m.num_rows for m in reader.read_metadata(sample)] == [9]


def test_count_reads_when_message_types_need_a_schema_the_summary_lacks(tmp_path):
    """A ``message_types`` count reads the file when the summary lacks the schemas.

    The schemas declared in the chunks then filter the messages, so the count
    matches the read.
    """
    path = os.path.join(tmp_path, "noschemas.mcap")
    write_mcap_without_summary_schemas(path)

    everything = MCAPDatasourceV2([path])
    scanner = scanner_for(everything)
    sample = sample_files(
        everything._get_file_indexer(), everything.paths, everything.filesystem, []
    )
    assert [m.num_rows for m in scanner.create_reader().read_metadata(sample)] == [6]

    by_schema = MCAPDatasourceV2([path], message_types=["other_schema"])
    scanner = scanner_for(by_schema)
    assert scanner.metadata_row_count_is_exact()
    assert [m.num_rows for m in scanner.create_reader().read_metadata(sample)] == [3]
    assert (
        read_all(by_schema, *list_manifests(by_schema), scanner=scanner).num_rows == 3
    )


def test_count_from_statistics(tmp_path, chunked_file):
    """Counts come from the summary statistics, per file and per selected topic,
    and a file without statistics is scanned. A time range or a limit is not
    counted from statistics."""
    other = os.path.join(tmp_path, "other.mcap")
    write_mcap(other, round_robin_messages(4, topics=("/a",)))
    unindexed = os.path.join(tmp_path, "unindexed.mcap")
    write_mcap(unindexed, round_robin_messages(2, topics=("/a",)), index=False)
    paths = [chunked_file, other, unindexed]

    everything = MCAPDatasourceV2(paths)
    scanner = scanner_for(everything)
    reader = scanner.create_reader()
    assert scanner.metadata_row_count_is_exact()
    assert reader.available_metadata() == {MetadataType.NUM_ROWS}
    sample = sample_files(
        everything._get_file_indexer(), everything.paths, everything.filesystem, []
    )
    assert [m.num_rows for m in reader.read_metadata(sample)] == [9, 4, 2]

    # A topic selection is summed from the per-channel statistics.
    by_topic = MCAPDatasourceV2(paths, topics=["/a"])
    counts = [
        m.num_rows for m in scanner_for(by_topic).create_reader().read_metadata(sample)
    ]
    assert counts == [3, 4, 2]

    # A pushed filter narrows the counted selection too.
    pushed, _ = scanner.push_filters(col("topic") == "/b")
    assert pushed.metadata_row_count_is_exact()
    assert [m.num_rows for m in pushed.create_reader().read_metadata(sample)] == [
        3,
        0,
        0,
    ]

    ranged = MCAPDatasourceV2(
        [chunked_file], time_range=TimeRange(BASE_TIME, BASE_TIME + 1)
    )
    ranged_scanner = scanner_for(ranged)
    assert not ranged_scanner.metadata_row_count_is_exact()
    assert ranged_scanner.create_reader().available_metadata() == set()
    assert not scanner.push_limit(5).metadata_row_count_is_exact()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
