"""Unit tests for the samples dataset of ``read_mcap(samples=...)``
(``mcap_samples``): its schema checks and its manifest rows."""

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    check_samples_schema,
    sample_manifest,
)
from ray.data.context import DataContext

SAMPLES_SCHEMA = pa.schema(
    [
        ("sample_id", pa.string()),
        ("files", pa.list_(pa.string())),
        ("clip_start", pa.int64()),
        ("clip_end", pa.int64()),
        ("anchor", pa.int64()),
        ("instruction", pa.string()),
    ]
)


@pytest.mark.parametrize(
    "change, error",
    [
        (lambda s: s.remove(s.get_field_index("clip_end")), "lacks the columns"),
        (lambda s: s.set(1, pa.field("files", pa.string())), "list of paths"),
        (lambda s: s.set(2, pa.field("clip_start", pa.float64())), "integer"),
        (lambda s: s.append(pa.field("is_pad:/cam", pa.bool_())), "the read adds"),
        (lambda s: s.append(pa.field("__path", pa.string())), "the read adds"),
    ],
)
def test_check_samples_schema(change, error):
    """A samples dataset needs its files and clip columns with their types, and
    no column the read adds."""
    check_samples_schema(SAMPLES_SCHEMA, ["/cam"])

    with pytest.raises(ValueError, match=error):
        check_samples_schema(change(SAMPLES_SCHEMA), ["/cam"])


def test_check_samples_schema_needs_an_anchor_only_when_a_topic_measures_from_it():
    """Without an ``anchor`` column, a read whose topics all measure from the
    clip passes, and one with a topic that measures from the anchor fails,
    naming the topic."""
    no_anchor = SAMPLES_SCHEMA.remove(SAMPLES_SCHEMA.get_field_index("anchor"))

    check_samples_schema(no_anchor, ["/cam"], anchor_topics=[])
    with pytest.raises(ValueError, match=r"topics \['/cam'\] measure from it"):
        check_samples_schema(no_anchor, ["/cam"], anchor_topics=["/cam"])


def samples_batch(**changes):
    """Two valid samples as a batch of the samples dataset, with ``changes``
    replacing whole columns."""
    columns = {
        "sample_id": ["a", "b"],
        "files": [["x.mcap"], ["y.mcap"]],
        "clip_start": [0, 10],
        "clip_end": [100, 20],
        "anchor": [5, 15],
    }
    columns.update(changes)
    return pa.table(columns)


def manifest_of(batch, **options):
    """The one table ``sample_manifest`` yields for ``batch``."""
    (manifest,) = sample_manifest(batch, **options)
    return manifest


def test_sample_manifest_names_each_samples_file():
    """Each sample becomes a manifest row: its file as the path, no size and no
    chunks, with its own columns kept."""
    manifest = manifest_of(samples_batch())

    assert manifest.column("__path").to_pylist() == ["x.mcap", "y.mcap"]
    assert manifest.column("__file_size").null_count == 2
    assert manifest.column("__file_chunk_metadata").null_count == 2
    assert manifest.column("sample_id").to_pylist() == ["a", "b"]


@pytest.mark.parametrize(
    "changes, error, message",
    [
        ({"files": [["x.mcap"], ["y.mcap", "z.mcap"]]}, NotImplementedError, "2 files"),
        ({"files": [["x.mcap"], []]}, ValueError, "Sample 'b' lists no file"),
        (
            {"anchor": [5, 20]},
            ValueError,
            r"Sample 'b' has clip \[10, 20\) and anchor 20",
        ),
        ({"clip_end": [0, 20]}, ValueError, "Sample 'a' has clip"),
        ({"clip_start": [0, None]}, ValueError, "Sample 'b' has clip"),
    ],
)
def test_sample_manifest_rejects_bad_samples(changes, error, message):
    """A sample must list exactly one file, and its anchor must lie inside a
    clip that is not empty. The error names the first bad sample."""
    with pytest.raises(error, match=message):
        manifest_of(samples_batch(**changes))


def test_sample_manifest_without_anchors():
    """A sample may have no anchor when no topic measures from it, whether the
    column is missing or the value is null. When a topic does, a missing or null
    anchor fails, naming the sample."""
    without_column = samples_batch().drop(["anchor"])
    with_null = samples_batch(anchor=[5, None])

    assert manifest_of(without_column, needs_anchor=False).num_rows == 2
    assert manifest_of(with_null, needs_anchor=False).num_rows == 2
    with pytest.raises(ValueError, match="Sample 'b' has no anchor"):
        manifest_of(with_null, needs_anchor=True)
    with pytest.raises(ValueError, match="Sample 'a' has no anchor"):
        manifest_of(without_column, needs_anchor=True)


def test_sample_manifest_skips_only_the_bad_samples(monkeypatch):
    """When the job skips errors, a bad sample costs only its own row. The good
    rows come first, then one error counts the bad samples and names the first."""
    monkeypatch.setattr(DataContext.get_current(), "max_errored_blocks", 1)
    # 'b' lists no file and 'c' has its anchor at its clip's end. 'b' fails the
    # first check, so the later checks see only 'a', 'c' and 'd'.
    samples = samples_batch(
        sample_id=["a", "b", "c", "d"],
        files=[["x.mcap"], [], ["y.mcap"], ["z.mcap"]],
        clip_start=[0, 0, 10, 10],
        clip_end=[100, 100, 20, 20],
        anchor=[5, 5, 20, 15],
    )

    manifests = sample_manifest(samples)
    kept = next(manifests)
    with pytest.raises(ValueError) as error:
        next(manifests)

    assert kept.column("sample_id").to_pylist() == ["a", "d"]
    assert kept.column("__path").to_pylist() == ["x.mcap", "z.mcap"]
    assert str(error.value).startswith(
        "Skipped 2 bad samples of 4 in this batch, and kept the others. The "
        "first: ValueError: Sample 'b' lists no file"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
