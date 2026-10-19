import io
import logging
import math
from decimal import Decimal
from typing import Any, cast
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
import pytest

from ray.data._internal.datasource_v2.formats.orc import (
    orc_batch_size_estimator as sizing,
)


def _fragment(tmp_path, table, **options):
    path = tmp_path / "data.orc"
    orc.write_table(table, str(path), **options)
    return next(pds.dataset(str(path), format="orc").get_fragments())


def _footer(fragment):
    with fragment.filesystem.open_input_file(fragment.path) as source:
        data = orc.ORCFile(source).reader.serialized_file_tail()
    # These runtime protobuf messages do not have generated Python stubs.
    return cast(Any, sizing._file_tail_message_type().FromString(data)).footer


@pytest.mark.parametrize("compression", ["uncompressed", "zstd"])
def test_first_wide_batch_tracks_target(tmp_path, compression):
    table = pa.table({"id": range(256), "payload": ["x" * 4096] * 256})
    fragment = _fragment(tmp_path, table, compression=compression)
    assert [
        sizing.estimate_batch_size(fragment, None, target) for target in (65536, 131072)
    ] == [16, 32]
    assert sizing.estimate_batch_size(fragment, None, 1) == 1


@pytest.mark.parametrize(
    "array,expected_bytes",
    [
        # Three slots and one validity byte; bool values occupy one byte.
        (pa.array([True, None, False]), 2),
        (pa.array([1, None, 3], type=pa.int8()), 4),
        (pa.array([1, None, 3], type=pa.int64()), 25),
        (pa.array([1.0, None, 3.0], type=pa.float32()), 13),
        (pa.array([1, None, 3], type=pa.date32()), 13),
        (pa.array([1, None, 3], type=pa.timestamp("ns")), 25),
        (
            pa.array(
                [Decimal("1.23"), None, Decimal("4.56")], type=pa.decimal128(10, 2)
            ),
            49,
        ),
        # One validity byte + four int32 offsets + UTF-8/binary payload bytes.
        (pa.array(["你好", None, ""]), 23),
        (pa.array([b"abc", None, b""]), 20),
        (
            pa.array(
                [None, {"s": "x" * 64, "n": 1}, {"s": None, "n": None}],
                type=pa.struct([("s", pa.string()), ("n", pa.int64())]),
            ),
            107,  # 1 + string (1 + 16 + 64) + int64 (1 + 24).
        ),
        (
            pa.array([["x" * 64, "y"], None, []], type=pa.list_(pa.string())),
            95,  # List (1 + 16) + two string slots (1 + 12 + 65).
        ),
        (
            pa.array(
                [[["x" * 64], []], None, []], type=pa.list_(pa.list_(pa.string()))
            ),
            103,  # Outer list (1 + 16) + inner lists (1 + 12) + string (1 + 8 + 64).
        ),
        (
            pa.array(
                [[{"s": "x" * 64}, {"s": None}], None, []],
                type=pa.list_(pa.struct([("s", pa.string())])),
            ),
            95,  # List (1 + 16) + struct validity 1 + string (1 + 12 + 64).
        ),
        (
            pa.array(
                [[("a", "x" * 64), ("b", None)], None, []],
                type=pa.map_(pa.string(), pa.string()),
            ),
            109,  # Map (1 + 16) + keys (1 + 12 + 2) + values (1 + 12 + 64).
        ),
        (
            pa.array(
                [[("a", ["x" * 64]), ("b", None)], None, []],
                type=pa.map_(pa.string(), pa.list_(pa.string())),
            ),
            118,  # Map 17 + keys 15 + value lists 13 + string 73.
        ),
    ],
    ids=[
        "bool",
        "int8",
        "int64",
        "float",
        "date",
        "timestamp",
        "decimal",
        "unicode",
        "binary",
        "struct",
        "list",
        "nested_list",
        "list_struct",
        "map",
        "map_list",
    ],
)
def test_supported_layouts_use_real_metadata(tmp_path, array, expected_bytes):
    table = pa.table({"value": array})
    fragment = _fragment(tmp_path, table)
    footer = _footer(fragment)
    assert (
        sizing._estimate_projected_size(footer, table.schema, ["value"])
        == expected_bytes
    )
    assert sizing.estimate_batch_size(fragment, None, 16) == min(
        math.ceil(16 * len(table) / expected_bytes), len(table)
    )
    assert sizing.estimate_batch_size(fragment, None, 1_000_000) == len(table)


@pytest.mark.parametrize(
    "array,expected_bytes,target,expected_rows",
    [
        pytest.param(
            pa.array(
                [None, {"s": "x" * 64, "n": 1}, {"s": None, "n": None}],
                type=pa.struct([("s", pa.string()), ("n", pa.int64())]),
            ),
            # Three parent slots, including the null struct:
            # validity 1 + string (1 + 4 * 4 + 64) + int64 (1 + 3 * 8).
            107,
            64,
            2,  # ceil(64 * 3 / 107)
            id="nullable_struct",
        ),
        pytest.param(
            pa.array([["x" * 64, "y"], None, []], type=pa.list_(pa.string())),
            # Three list slots, but two element slots with 65 payload bytes:
            # list (1 + 4 * 4) + elements (1 + 3 * 4 + 65).
            95,
            32,
            2,  # ceil(32 * 3 / 95)
            id="list",
        ),
        pytest.param(
            pa.array(
                [[("a", "x" * 64), ("b", None)], None, []],
                type=pa.map_(pa.string(), pa.string()),
            ),
            # Two entries give both children two slots, even with a null value:
            # map (1 + 4 * 4) + keys (1 + 3 * 4 + 2) + values (1 + 3 * 4 + 64).
            109,
            36,
            1,  # ceil(36 * 3 / 109)
            id="map_nullable_value",
        ),
    ],
)
def test_nested_layouts_match_manual_sizes(
    tmp_path, array, expected_bytes, target, expected_rows
):
    table = pa.table({"value": array})
    fragment = _fragment(tmp_path, table)
    assert (
        sizing._estimate_projected_size(_footer(fragment), table.schema, ["value"])
        == expected_bytes
    )
    assert sizing.estimate_batch_size(fragment, None, target) == expected_rows


@pytest.mark.parametrize(
    "array",
    [
        pa.array([[None, "wide" * 64]], type=pa.list_(pa.string())),
        pa.array([[None, []]], type=pa.list_(pa.list_(pa.string()))),
    ],
)
def test_nullable_list_elements_fall_back(tmp_path, array):
    fragment = _fragment(tmp_path, pa.table({"value": array}))
    assert sizing.estimate_batch_size(fragment, None, 128) is None


def test_projection_uses_original_type_ids(tmp_path):
    table = pa.table(
        {
            "nested": [["x" * 4096]] * 256,
            "id": range(256),
            "payload": ["y" * 4096] * 256,
        }
    )
    fragment = _fragment(tmp_path, table)
    assert sizing.estimate_batch_size(fragment, ["id"], 65536) == 256
    assert sizing.estimate_batch_size(fragment, ["payload"], 65536) == 16
    for columns in (["id", "payload"], ["payload", "id"]):
        assert sizing.estimate_batch_size(fragment, columns, 65536) == 16
    assert sizing.estimate_batch_size(fragment, [], 65536) is None
    assert (
        sizing.estimate_batch_size(
            fragment, None, 65536, excluded_columns=["nested", "payload"]
        )
        == 256
    )
    assert sizing.estimate_batch_size(fragment, ["missing"], 65536) is None
    assert (
        sizing.estimate_batch_size(
            fragment, ["id"], 65536, schema=pa.schema([("id", pa.float64())])
        )
        is None
    )


def test_unselected_unsupported_type_does_not_disable_estimation(tmp_path):
    union = pa.UnionArray.from_dense(
        pa.array([0, 1], type=pa.int8()),
        pa.array([0, 0], type=pa.int32()),
        [pa.array([1]), pa.array(["x"])],
    )
    fragment = _fragment(tmp_path, pa.table({"unsupported": union, "id": [1, 2]}))
    assert sizing.estimate_batch_size(fragment, ["id"], 128) == 2
    assert sizing.estimate_batch_size(fragment, None, 128) is None


def test_initial_estimate_has_no_default_row_cap(tmp_path):
    rows = 131_073
    fragment = _fragment(tmp_path, pa.table({"flag": [True] * rows}))
    assert sizing.estimate_batch_size(fragment, None, 131_072) == rows


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_rows",
        "missing_sum",
        "negative_sum",
        "missing_count",
        "count_exceeds_slots",
        "bad_child",
        "cycle",
        "missing_statistics",
    ],
)
def test_invalid_metadata_is_not_partially_summed(tmp_path, mutation):
    table = pa.table({"id": [1, 2], "payload": ["x" * 64, "y" * 64]})
    fragment = _fragment(tmp_path, table)
    footer = _footer(fragment)
    statistics = footer.statistics
    types = footer.types
    string_stats = statistics[2].string_statistics
    if mutation == "missing_rows":
        footer.ClearField("number_of_rows")
    elif mutation == "missing_sum":
        string_stats.ClearField("sum")
    elif mutation == "negative_sum":
        string_stats.sum = -1
    elif mutation == "missing_count":
        statistics[2].ClearField("number_of_values")
    elif mutation == "count_exceeds_slots":
        statistics[2].number_of_values = 3
    elif mutation == "bad_child":
        types[0].subtypes[1] = 99
    elif mutation == "cycle":
        types[0].subtypes[1] = 0
    else:
        del statistics[-1]
    assert (
        sizing._estimate_projected_size(footer, table.schema, ["id", "payload"]) is None
    )
    if mutation in {
        "missing_sum",
        "negative_sum",
        "missing_count",
        "count_exceeds_slots",
    }:
        assert sizing._estimate_projected_size(footer, table.schema, ["id"]) is not None


def test_zero_payload_is_distinct_from_missing_statistics(tmp_path):
    fragment = _fragment(
        tmp_path, pa.table({"value": pa.array(["", None, ""], type=pa.string())})
    )
    assert sizing.estimate_batch_size(fragment, None, 128) == 3
    empty = _fragment(tmp_path, pa.table({"value": pa.array([], type=pa.string())}))
    assert sizing.estimate_batch_size(empty, None, 128) is None


@pytest.mark.parametrize("row_index_stride", [0, 10000])
def test_indexless_collection_counts_fall_back(tmp_path, monkeypatch, row_index_stride):
    table = pa.table(
        {
            "nested": pa.array([[1, 2], [3]], type=pa.list_(pa.int64())),
            "id": [1, 2],
        }
    )
    fragment = _fragment(tmp_path, table)
    native = orc.ORCFile

    class OrcFile:
        def __init__(self, source):
            self._file = native(source)

        @property
        def row_index_stride(self):
            return row_index_stride

        def __getattr__(self, name):
            return getattr(self._file, name)

    # PyArrow 17/23 cannot write stride 0. Keep valid metadata and vary only
    # the reader property to test propagation through the public estimator.
    monkeypatch.setattr(orc, "ORCFile", OrcFile)
    expected = None if row_index_stride == 0 else 1
    assert sizing.estimate_batch_size(fragment, ["nested"], 16) == expected
    assert sizing.estimate_batch_size(fragment, ["id"], 16) == 2


@pytest.mark.parametrize("columns", [[], ["path"]])
def test_no_physical_output_does_not_open_metadata(columns):
    fragment = Mock(spec=pds.FileFragment)
    fragment.filesystem.open_input_file.side_effect = AssertionError(
        "No physical output"
    )
    assert (
        sizing.estimate_batch_size(fragment, columns, 128, excluded_columns=["path"])
        is None
    )
    fragment.filesystem.open_input_file.assert_not_called()


def test_missing_element_null_flag_cannot_supply_list_slots(tmp_path):
    table = pa.table({"payload": [["x" * 64]], "id": [1]})
    fragment = _fragment(tmp_path, table)
    footer = _footer(fragment)
    footer.statistics[2].ClearField("has_null")
    assert sizing._estimate_projected_size(footer, table.schema, ["payload"]) is None
    assert sizing._estimate_projected_size(footer, table.schema, ["id"]) is not None


class _CountingFile(io.BufferedReader):
    def __init__(self, path):
        super().__init__(open(path, "rb", buffering=0))
        self.reads = []

    def read(self, size=-1):
        offset = self.tell()
        value = super().read(size)
        self.reads.append((offset, len(value)))
        return value


class _TrackingFilesystem:
    def __init__(self):
        self.files = []

    def open_input_file(self, path):
        raw = _CountingFile(path)
        source = pa.PythonFile(raw, mode="r")
        self.files.append((raw, source))
        return source


def test_estimation_only_reads_metadata_and_releases_file(tmp_path, monkeypatch):
    table = pa.table({"payload": [f"{i:08d}" + "abcdefgh" * 512 for i in range(512)]})
    fragment = _fragment(
        tmp_path,
        table,
        stripe_size=65536,
        batch_size=32,
        compression="uncompressed",
        dictionary_key_size_threshold=0.0,
    )
    filesystem = _TrackingFilesystem()
    with filesystem.open_input_file(fragment.path) as source:
        baseline = orc.ORCFile(source)
        assert baseline.nstripes > 1
        assert baseline.nrows == len(table)
        assert baseline.schema == table.schema
    baseline_reads = list(filesystem.files[0][0].reads)
    monkeypatch.setattr(
        orc.ORCFile, "read", Mock(side_effect=AssertionError("No data reads"))
    )
    monkeypatch.setattr(
        orc.ORCFile, "read_stripe", Mock(side_effect=AssertionError("No stripe reads"))
    )
    tracked = Mock(spec=pds.FileFragment)
    tracked.path, tracked.filesystem = fragment.path, filesystem
    assert sizing.estimate_batch_size(tracked, None, 65536) == 16
    assert filesystem.files[-1][0].reads == baseline_reads
    assert (
        sum(size for _, size in baseline_reads) < (tmp_path / "data.orc").stat().st_size
    )
    assert all(raw.closed and source.closed for raw, source in filesystem.files)


@pytest.mark.parametrize(
    "failure",
    [
        "missing_reader",
        "missing_interface",
        "bad_type",
        "bad_proto",
        "missing_footer",
        "old_writer",
        "io_error",
    ],
)
def test_metadata_failure_falls_back_and_closes_input(
    tmp_path, monkeypatch, caplog, failure
):
    fragment = _fragment(tmp_path, pa.table({"payload": ["x" * 4096] * 32}))
    filesystem = _TrackingFilesystem()
    tracked = Mock(spec=pds.FileFragment)
    tracked.path, tracked.filesystem = fragment.path, filesystem
    native = orc.ORCFile

    class OrcFile:
        def __init__(self, source):
            self._file = native(source)
            self.reader = Mock()
            self.reader.serialized_file_tail.return_value = (
                self._file.reader.serialized_file_tail()
            )
            if failure == "missing_reader":
                del self.reader
            elif failure == "missing_interface":
                self.reader.serialized_file_tail = None
            elif failure == "bad_type":
                self.reader.serialized_file_tail.return_value = None
            elif failure == "io_error":
                self.reader.serialized_file_tail.side_effect = OSError(
                    "metadata unavailable"
                )
            elif failure == "bad_proto":
                self.reader.serialized_file_tail.return_value = b"\xff"
            elif failure == "missing_footer":
                self.reader.serialized_file_tail.return_value = b""

        @property
        def writer_version(self):
            return "ORIGINAL" if failure == "old_writer" else self._file.writer_version

        def __getattr__(self, name):
            if failure == "missing_reader" and name == "reader":
                raise AttributeError(name)
            return getattr(self._file, name)

    monkeypatch.setattr(orc, "ORCFile", OrcFile)
    # Ray Data does not propagate records to caplog's root handler.
    monkeypatch.setattr(sizing.logger, "handlers", [caplog.handler])
    monkeypatch.setattr(sizing.logger, "propagate", False)
    with caplog.at_level(logging.DEBUG, logger=sizing.__name__):
        assert sizing.estimate_batch_size(tracked, None, 65536) is None
    missing_interface = failure in {"missing_reader", "missing_interface"}
    assert caplog.record_tuples.count(
        (
            sizing.__name__,
            logging.DEBUG,
            "ORC batch sizing: serialized_file_tail is unavailable",
        )
    ) == int(missing_interface)
    assert len(filesystem.files) == 1
    assert all(raw.closed and source.closed for raw, source in filesystem.files)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
