"""Training samples: the samples dataset a sample read takes, and its checks.

``read_mcap(samples=...)`` reads one row per sample. A sample names its file,
a clip ``[clip_start, clip_end)`` and, when a topic measures from it, an
``anchor`` inside the clip, in nanoseconds of log time. ``check_samples_schema``
checks the dataset's columns when the read is planned. ``sample_manifest``
checks each sample as a read task takes it, and fails on the first bad one,
naming it.
"""

from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    Sequence,
    Tuple,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)

if TYPE_CHECKING:
    import numpy as np

# The columns every samples dataset has. ``files`` lists a sample's recordings,
# one for now. The times are log times in nanoseconds; ``clip_end`` is
# exclusive. ``anchor`` lies inside the clip, and is needed only when a topic
# measures from it.
FILES_COLUMN = "files"
CLIP_START_COLUMN = "clip_start"
CLIP_END_COLUMN = "clip_end"
ANCHOR_COLUMN = "anchor"
SAMPLE_COLUMNS: Tuple[str, ...] = (FILES_COLUMN, CLIP_START_COLUMN, CLIP_END_COLUMN)
# An optional column whose value names a sample in error messages.
SAMPLE_ID_COLUMN = "sample_id"

# A sample row's columns per topic.
DATA_PREFIX = "data:"
TIMES_PREFIX = "times:"
VALID_PREFIX = "valid:"
TOPIC_PREFIXES: Tuple[str, ...] = (DATA_PREFIX, TIMES_PREFIX, VALID_PREFIX)

# -- the samples dataset and the row layout ---------------------------------


def sample_manifest(batch: pa.Table, *, needs_anchor: bool = True) -> pa.Table:
    """A batch of samples as manifest rows for the read tasks.

    Each sample keeps its columns and gains the manifest's: its file as the
    path, with no size and no chunks. Fails on a sample that lists no file or
    several, whose clip is empty, or whose anchor lies outside its clip. With
    ``needs_anchor``, because a topic measures from it, a sample must have an
    anchor; otherwise one may have none.
    """
    import numpy as np

    files = batch.column(FILES_COLUMN).combine_chunks()
    counts = files.value_lengths().fill_null(0).to_numpy(zero_copy_only=False)
    _check(batch, counts == 1, _files_problem)
    paths = files.flatten()
    _check(batch, ~paths.is_null().to_numpy(zero_copy_only=False), _files_problem)
    start, end = (batch.column(name) for name in (CLIP_START_COLUMN, CLIP_END_COLUMN))
    known = ~np.logical_or(
        start.is_null().to_numpy(zero_copy_only=False),
        end.is_null().to_numpy(zero_copy_only=False),
    )
    _check(batch, known, _clip_problem)
    start, end = start.to_numpy(), end.to_numpy()
    _check(batch, start < end, _clip_problem)
    if needs_anchor and ANCHOR_COLUMN not in batch.column_names:
        raise _anchor_problem(batch.slice(0, 1).to_pylist()[0])
    if ANCHOR_COLUMN in batch.column_names:
        anchor = batch.column(ANCHOR_COLUMN)
        if pa.types.is_null(anchor.type):
            anchor = anchor.cast(pa.int64())
        missing = anchor.is_null().to_numpy(zero_copy_only=False)
        if needs_anchor:
            _check(batch, ~missing, _anchor_problem)
        values = anchor.fill_null(0).to_numpy()
        inside = np.logical_and(start <= values, values < end)
        _check(batch, np.logical_or(missing, inside), _clip_problem)
    num_rows = batch.num_rows
    return (
        batch.append_column(PATH_COLUMN_NAME, paths)
        .append_column(FILE_SIZE_COLUMN_NAME, pa.nulls(num_rows, pa.int64()))
        .append_column(FILE_CHUNK_METADATA_COLUMN_NAME, pa.nulls(num_rows))
    )


def _check(
    batch: pa.Table, ok: "np.ndarray", problem: Callable[[dict], Exception]
) -> None:
    """Raise ``problem(row)`` for the first sample of ``batch`` that is not ``ok``."""
    if ok.all():
        return
    first = int(ok.argmin())
    raise problem(batch.slice(first, 1).to_pylist()[0])


def _describe(row: dict) -> str:
    """How an error names a sample: by its id, else by its files and clip."""
    if row.get(SAMPLE_ID_COLUMN) is not None:
        return f"Sample {row[SAMPLE_ID_COLUMN]!r}"
    return (
        f"The sample with files {row[FILES_COLUMN]} and clip "
        f"[{row[CLIP_START_COLUMN]}, {row[CLIP_END_COLUMN]})"
    )


def _files_problem(row: dict) -> Exception:
    files = row[FILES_COLUMN] or []
    if len(files) > 1:
        return NotImplementedError(
            f"{_describe(row)} lists {len(files)} files. A sample that spans "
            "several files is not supported yet: give each sample one file."
        )
    return ValueError(f"{_describe(row)} lists no file: give each sample one.")


def _clip_problem(row: dict) -> Exception:
    return ValueError(
        f"{_describe(row)} has clip [{row[CLIP_START_COLUMN]}, "
        f"{row[CLIP_END_COLUMN]}) and anchor {row.get(ANCHOR_COLUMN)}. A clip must "
        "not be empty, and the anchor must lie inside it: clip_start <= anchor < "
        "clip_end, in nanoseconds of log time."
    )


def _anchor_problem(row: dict) -> Exception:
    return ValueError(
        f"{_describe(row)} has no anchor, and a topic measures from it. Give "
        "each sample an anchor, or measure from the clip (at='clip_start' or "
        "at='clip_end', steps.across, steps.at)."
    )


def check_samples_schema(
    schema: pa.Schema, topics: Sequence[str], anchor_topics: Sequence[str] = ()
) -> None:
    """Check that a samples dataset has the columns a sample read needs.

    ``files`` must be a list of strings, and the clip columns integers.
    ``anchor_topics`` are the topics that measure from the anchor: with any,
    the dataset needs an integer ``anchor`` column. No column may take the
    name of a column the read adds.
    """
    names = set(schema.names)
    missing = [name for name in SAMPLE_COLUMNS if name not in names]
    if missing:
        raise ValueError(
            f"The samples dataset lacks the columns {missing}. It needs "
            f"{list(SAMPLE_COLUMNS)}: the sample's files and its clip "
            "[clip_start, clip_end), in nanoseconds of log time."
        )
    if anchor_topics and ANCHOR_COLUMN not in names:
        raise ValueError(
            f"The samples dataset lacks the column {ANCHOR_COLUMN!r}, and topics "
            f"{sorted(anchor_topics)} measure from it. Add each sample's anchor, "
            "or measure those topics from the clip (at='clip_start' or "
            "at='clip_end', steps.across, steps.at)."
        )
    files_type = schema.field(FILES_COLUMN).type
    if not (
        (pa.types.is_list(files_type) or pa.types.is_large_list(files_type))
        and (
            pa.types.is_string(files_type.value_type)
            or pa.types.is_large_string(files_type.value_type)
        )
    ):
        raise ValueError(
            f"The samples column {FILES_COLUMN!r} must be a list of paths, got "
            f"{files_type}"
        )
    for name in (CLIP_START_COLUMN, CLIP_END_COLUMN, ANCHOR_COLUMN):
        type_ = schema.field(name).type if name in names else None
        if type_ is not None and not (
            pa.types.is_integer(type_)
            or (name == ANCHOR_COLUMN and pa.types.is_null(type_))
        ):
            raise ValueError(
                f"The samples column {name!r} must hold integer nanoseconds of log "
                f"time, got {type_}"
            )
    added = {name for topic in topics for name in topic_column_names(topic)}
    added |= {PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_CHUNK_METADATA_COLUMN_NAME}
    taken = sorted(names & added)
    if taken:
        raise ValueError(
            f"The samples dataset has columns {taken} that the read adds. Rename "
            "or drop them."
        )


def topic_column_names(topic: str) -> List[str]:
    """Every name a sample row may give one topic's columns."""
    return [f"{prefix}{topic}" for prefix in TOPIC_PREFIXES]
