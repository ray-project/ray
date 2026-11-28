import json
import os

import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.logical.operators import ReadFiles
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.mock_http_server import (
    data_file,
    no_ranges_file,
    no_ranges_num_lines,
)
from ray.tests.conftest import *  # noqa


@pytest.fixture(params=[False, True], ids=["v1", "v2"])
def text_reader_version(request, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", request.param)
    return request.param


def _to_lines(rows):
    return [row["text"] for row in rows]


@pytest.fixture
def recorded_text_filesystem(tmp_path):
    class RecordedTextFile:
        """Record real file IO and optionally fail after partial progress."""

        def __init__(self, file, path, event_path, fail_after):
            self._file = file
            self._path = path
            self._event_path = event_path
            self._failure_path = event_path + ".failed"
            self._fail_after = fail_after
            self._fail_this_open = fail_after is not None and not os.path.exists(
                self._failure_path
            )
            self._record("open")

        def _record(self, event, **kwargs):
            with open(self._event_path, "a") as log:
                log.write(json.dumps({"event": event, "path": self._path, **kwargs}))
                log.write("\n")

        def read(self, size=-1):
            if self._fail_this_open and self._file.tell() >= self._fail_after:
                with open(self._failure_path, "w"):
                    pass
                self._record("error")
                raise OSError("injected text stream failure")
            if self._fail_after is not None and not self._fail_this_open and size > 0:
                # A retry can see different short reads. Batch boundaries must
                # still match, since replay skips already-yielded batches.
                size = min(size, 17)
            data = self._file.read(size)
            self._record("read", size=len(data))
            return data

        def close(self):
            if not self._file.closed:
                self._record("close")
                self._file.close()

        def __getattr__(self, name):
            return getattr(self._file, name)

    class RecordedTextFileSystemHandler(pafs.FSSpecHandler):
        def __init__(self, event_path, fail_after=None):
            super().__init__(LocalFileSystem(skip_instance_cache=True))
            self._event_path = event_path
            self._fail_after = fail_after

        def open_input_stream(self, path):
            return pa.PythonFile(
                RecordedTextFile(
                    super().open_input_stream(path),
                    path,
                    self._event_path,
                    self._fail_after,
                ),
                mode="r",
            )

    def make(fail_after=None):
        event_path = str(tmp_path / "events.jsonl")
        filesystem = pafs.PyFileSystem(
            RecordedTextFileSystemHandler(event_path, fail_after=fail_after)
        )
        return filesystem, event_path

    return make


def _text_file_events(event_path):
    with open(event_path) as log:
        return [json.loads(line) for line in log]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "encoding",
    [pytest.param("utf-8", id="utf8"), pytest.param("utf-16-le", id="utf16_le")],
)
def test_read_text_streaming_line_boundaries(
    ray_start_regular_shared, tmp_path, datasource_v2, restore_data_context, encoding
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    restore_data_context.streaming_read_buffer_size = 7
    restore_data_context.target_max_block_size = 64
    text = "你好\r\n\n  \t\r🌍\vfin\fmore\x1cend\x1dnext\x1e\x85\u2028\u2029last"
    path = tmp_path / "year=2024" / "data.txt"
    path.parent.mkdir()
    path.write_text(text, encoding=encoding)

    ds = ray.data.read_text(
        str(path),
        encoding=encoding,
        drop_empty_lines=False,
        include_paths=True,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
        ),
    )

    assert sorted(ds.take_all(), key=lambda row: row["text"]) == [
        {"text": line, "year": 2024, "path": str(path)}
        for line in sorted(text.splitlines())
    ]
    assert ds.select_columns(["year", "path"]).take_all() == [
        {"year": 2024, "path": str(path)}
    ] * len(text.splitlines())
    assert ds.count() == len(text.splitlines())


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "codec,suffix",
    [
        pytest.param("gzip", ".gz", id="gzip"),
        pytest.param("bz2", ".bz2", id="bz2"),
        pytest.param("zstd", ".zst", id="zstd"),
        pytest.param("lz4", ".lz4", id="lz4"),
    ],
)
def test_read_text_streaming_compression(
    ray_start_regular_shared,
    tmp_path,
    datasource_v2,
    restore_data_context,
    codec,
    suffix,
):
    restore_data_context.streaming_read_buffer_size = 7
    restore_data_context.target_max_block_size = 64
    path = str(tmp_path / ("data.txt" + suffix))
    text = "你好\r\n🌍\n" * 20
    with pafs.LocalFileSystem().open_output_stream(path, compression=codec) as stream:
        stream.write(text.encode("utf-8"))

    assert sorted(_to_lines(ray.data.read_text(path).take_all())) == sorted(
        text.splitlines()
    )
    assert _to_lines(ray.data.read_text(path).limit(1).take_all()) == ["你好"]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "encoding",
    [pytest.param("utf-8", id="utf8"), pytest.param("UTF_8", id="utf8_alias")],
)
def test_read_text_limit_stops_io(
    ray_start_regular_shared,
    tmp_path,
    datasource_v2,
    restore_data_context,
    recorded_text_filesystem,
    encoding,
):
    restore_data_context.streaming_read_buffer_size = 4096
    path = tmp_path / "large.txt"
    path.write_text("hello\n" * 200_000)
    filesystem, event_path = recorded_text_filesystem()

    rows = (
        ray.data.read_text(str(path), filesystem=filesystem, encoding=encoding)
        .limit(1)
        .take_all()
    )

    events = _text_file_events(event_path)
    assert rows == [{"text": "hello"}]
    assert sum(e.get("size", 0) for e in events) < path.stat().st_size
    assert [e["event"] for e in events].count("close") == 1


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_read_text_retries_after_yielding_batches(
    ray_start_regular_shared,
    tmp_path,
    datasource_v2,
    restore_data_context,
    recorded_text_filesystem,
):
    restore_data_context.streaming_read_buffer_size = 32
    restore_data_context.target_max_block_size = 64
    restore_data_context.retried_io_errors = ["injected text stream failure"]
    restore_data_context.execution_options.preserve_order = True
    path = tmp_path / "data.txt"
    path.write_text("".join(f"{i:04d}\n\n" for i in range(200)))
    filesystem, event_path = recorded_text_filesystem(fail_after=128)

    rows = (
        ray.data.read_text(str(path), filesystem=filesystem, override_num_blocks=1)
        .limit(150)
        .take_all()
    )

    events = _text_file_events(event_path)
    assert _to_lines(rows) == [f"{i:04d}" for i in range(150)]
    assert [e["event"] for e in events].count("error") == 1
    assert [e["event"] for e in events].count("open") == 2
    assert [e["event"] for e in events].count("close") == 2


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_read_text_partition_predicate_does_not_open_pruned_file(
    ray_start_regular_shared, tmp_path, datasource_v2, recorded_text_filesystem
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    root = tmp_path / "root.txt"
    root.write_text("root\n")
    nested = tmp_path / "year=2024" / "nested.txt"
    nested.parent.mkdir()
    nested.write_text("nested\n")
    filesystem, event_path = recorded_text_filesystem()

    rows = (
        ray.data.read_text(
            [str(root), str(nested)],
            filesystem=filesystem,
            partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        )
        .filter(expr=col("year").is_null())
        .take_all()
    )

    assert rows == [{"text": "root", "year": None}]
    assert [
        e["path"] for e in _text_file_events(event_path) if e["event"] == "open"
    ] == [str(root)]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_read_text_limit_does_not_decode_unread_suffix(
    ray_start_regular_shared, tmp_path, datasource_v2, restore_data_context
):
    restore_data_context.streaming_read_buffer_size = 64
    path = tmp_path / "data.txt"
    path.write_bytes(b"hello\n" * 1000 + b"\xff")

    assert ray.data.read_text(str(path)).limit(1).take_all() == [{"text": "hello"}]
    with pytest.raises(UnicodeDecodeError, match="utf-8"):
        ray.data.read_text(str(path)).take_all()


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "encoding,data,expected",
    [
        pytest.param("punycode", b"hello-", ["hello"], id="punycode"),
        pytest.param("idna", b"hello\n" * 200, ["hello"] * 200, id="idna"),
        pytest.param("cp1252", b" \n\x80\nworld\n", ["€", "world"], id="cp1252"),
        pytest.param(
            "utf-16", "a\nb".encode("utf-16")[2:], ["a", "b"], id="utf16_no_bom"
        ),
        pytest.param(
            "utf-32", "a\nb".encode("utf-32")[4:], ["a", "b"], id="utf32_no_bom"
        ),
        pytest.param(
            "utf-8-sig", "a\nb".encode("utf-8-sig"), ["a", "b"], id="utf8_with_bom"
        ),
        pytest.param(
            "unicode_escape", b"hello\\nworld", ["hello", "world"], id="unicode_escape"
        ),
    ],
)
def test_read_text_preserves_whole_file_codec_semantics(
    ray_start_regular_shared,
    tmp_path,
    datasource_v2,
    restore_data_context,
    encoding,
    data,
    expected,
):
    restore_data_context.streaming_read_buffer_size = 4
    path = tmp_path / "data.txt"
    path.write_bytes(data)

    ds = ray.data.read_text(str(path), encoding=encoding)

    assert sorted(_to_lines(ds.take_all())) == sorted(expected)
    assert _to_lines(ds.limit(1).take_all()) == expected[:1]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
@pytest.mark.parametrize(
    "encoding,data",
    [
        pytest.param("cp1252", b"hello\n" * 1000 + b"\x81", id="invalid_suffix"),
        pytest.param("utf-8-sig", b"\xef", id="one_byte_of_bom"),
        pytest.param("utf-8-sig", b"\xef\xbb", id="two_bytes_of_bom"),
    ],
)
def test_read_text_fallback_checks_encoding_before_limit(
    ray_start_regular_shared,
    tmp_path,
    datasource_v2,
    restore_data_context,
    encoding,
    data,
):
    restore_data_context.streaming_read_buffer_size = 64
    path = tmp_path / "data.txt"
    path.write_bytes(data)

    with pytest.raises(UnicodeDecodeError, match="codec can't decode"):
        ray.data.read_text(str(path), encoding=encoding).limit(1).take_all()


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_read_text_keeps_emitted_rows_on_tolerated_decode_error(
    ray_start_regular_shared, tmp_path, datasource_v2, restore_data_context
):
    restore_data_context.streaming_read_buffer_size = 4096
    restore_data_context.target_max_block_size = 4096
    restore_data_context.max_errored_blocks = 1
    path = tmp_path / "damaged.txt"
    # Each row occupies 8 bytes. The invalid byte starts a fresh decode chunk.
    expected = [f"{i:07d}" for i in range(2048)]
    path.write_bytes(("\n".join(expected) + "\n").encode() + b"\xff")

    rows = ray.data.read_text(str(path)).take_all()

    assert sorted(_to_lines(rows)) == expected


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_read_text_limit_is_global_across_read_tasks(
    ray_start_regular_shared, tmp_path, datasource_v2, restore_data_context
):
    restore_data_context.execution_options.preserve_order = True
    restore_data_context.target_max_block_size = 64
    paths = [tmp_path / f"{index}.txt" for index in range(4)]
    for index, path in enumerate(paths):
        path.write_text("".join(f"{index}:{row}\n" for row in range(20)))

    rows = (
        ray.data.read_text([str(path) for path in paths], override_num_blocks=4)
        .limit(7)
        .take_all()
    )

    assert _to_lines(rows) == [f"0:{row}" for row in range(7)]


@pytest.mark.parametrize("datasource_v2", [True], indirect=True)
def test_read_text_limit_counts_rows_after_residual_filter(
    ray_start_regular_shared, tmp_path, datasource_v2, restore_data_context
):
    from ray.data.expressions import col

    restore_data_context.target_max_block_size = 64
    path = tmp_path / "data.txt"
    path.write_text("skip\n" * 100 + "keep\n")

    rows = (
        ray.data.read_text(str(path))
        .filter(expr=col("text") == "keep")
        .limit(1)
        .take_all()
    )

    assert rows == [{"text": "keep"}]


def test_empty_text_files(ray_start_regular_shared, tmp_path, text_reader_version):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    # 2 empty files.
    _ = open(os.path.join(path, "file1.txt"), "w")
    _ = open(os.path.join(path, "file2.txt"), "w")
    ds = ray.data.read_text(path)
    assert ds.count() == 0
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 0


def test_read_text(ray_start_regular_shared, tmp_path, text_reader_version):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")
    with open(os.path.join(path, "file3.txt"), "w") as f:
        f.write("ray\n")
    ds = ray.data.read_text(path)
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "ray", "world"]
    assert ds.schema().names == ["text"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 4


def test_read_text_partitioned_with_paths_and_pushdowns(
    ray_start_regular_shared, tmp_path, text_reader_version
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    for year, content in (("2023", "one\n\n"), ("2024", "two\nthree\n")):
        os.mkdir(os.path.join(tmp_path, f"year={year}"))
        with open(os.path.join(tmp_path, f"year={year}", "data.txt"), "w") as f:
            f.write(content)
    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
    )

    ds = ray.data.read_text(
        str(tmp_path), partitioning=partitioning, include_paths=True
    )
    assert ds.schema().names == ["text", "year", "path"]
    rows = sorted(ds.take_all(), key=lambda row: row["text"])
    assert [(row["text"], row["year"]) for row in rows] == [
        ("one", 2023),
        ("three", 2024),
        ("two", 2024),
    ]
    assert all(row["path"].endswith(f"year={row['year']}/data.txt") for row in rows)

    pruned = ds.filter(expr=col("year") == 2024)
    assert sorted(_to_lines(pruned.take_all())) == ["three", "two"]
    assert sorted(_to_lines(ds.filter(expr=col("text") != "two").take_all())) == [
        "one",
        "three",
    ]
    assert ds.select_columns(["year"]).count() == 3
    assert ds.limit(1).count() == 1
    assert ds.count() == 3


def test_read_text_root_files_next_to_partitions(
    ray_start_regular_shared, tmp_path, text_reader_version
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    if not text_reader_version:
        pytest.skip(
            "V1 blocks from files outside partition directories lack the column"
        )

    with open(os.path.join(tmp_path, "root.txt"), "w") as f:
        f.write("root\n")
    os.mkdir(os.path.join(tmp_path, "year=2024"))
    with open(os.path.join(tmp_path, "year=2024", "data.txt"), "w") as f:
        f.write("nested\n")
    partitioning = Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path))

    ds = ray.data.read_text(
        str(tmp_path), partitioning=partitioning, include_paths=True
    )

    rows = sorted(ds.take_all(), key=lambda row: row["text"])
    assert [(row["text"], row["year"]) for row in rows] == [
        ("nested", "2024"),
        ("root", None),
    ]
    assert _to_lines(ds.filter(expr=col("year").is_null()).take_all()) == ["root"]
    assert _to_lines(ds.filter(expr=col("year") != "2024").take_all()) == []
    assert _to_lines(ds.filter(expr=col("year") == "2024").take_all()) == ["nested"]
    root_path = os.path.join(tmp_path, "root.txt")
    assert _to_lines(ds.filter(expr=col("path") == root_path).take_all()) == ["root"]


def test_read_text_preserve_order_keeps_files_in_order(
    ray_start_regular_shared, tmp_path, text_reader_version, monkeypatch
):
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx.execution_options, "preserve_order", True)
    # Two read tasks for five files: round-robin would deal files 0, 2, 4 to
    # the first task and 1, 3 to the second.
    monkeypatch.setattr(ctx, "read_op_min_num_blocks", 2)
    monkeypatch.setattr(ctx, "target_min_block_size", 1)
    paths = []
    for i in range(5):
        path = os.path.join(tmp_path, f"{i}.txt")
        with open(path, "w") as f:
            f.write(f"{i}a\n{i}b\n")
        paths.append(path)

    ds = ray.data.read_text(paths)

    assert _to_lines(ds.take_all()) == [f"{i}{s}" for i in range(5) for s in "ab"]


def test_read_text_compressed(ray_start_regular_shared, tmp_path, text_reader_version):
    import gzip

    with gzip.open(os.path.join(tmp_path, "data.txt.gz"), "wt") as f:
        f.write("hello\nworld\n")

    ds = ray.data.read_text(str(tmp_path))

    assert sorted(_to_lines(ds.take_all())) == ["hello", "world"]


def test_read_text_remote_args(ray_start_cluster, tmp_path, text_reader_version):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.shutdown()
    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")

    ds = ray.data.read_text(
        path, override_num_blocks=2, ray_remote_args={"resources": {"bar": 1}}
    )

    block_refs = _ref_bundles_iterator_to_block_refs_list(
        ds.iter_internal_ref_bundles()
    )
    ray.wait(block_refs, num_returns=len(block_refs), fetch_local=False)
    location_data = ray.experimental.get_object_locations(block_refs)
    locations = []
    for block in block_refs:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {bar_node_id}, locations
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "world"]


@pytest.mark.parametrize(
    "url, num_lines",
    [(data_file, 1000), (no_ranges_file, no_ranges_num_lines)],
    ids=["no-size", "no-ranges"],
)
@pytest.mark.parametrize("filesystem", [HTTPFileSystem(), None], ids=["fsspec", "auto"])
@pytest.mark.parametrize("num_paths", [1, 2])
def test_fsspec_http_file_system(
    ray_start_regular_shared,
    http_server,
    text_reader_version,
    url,
    num_lines,
    filesystem,
    num_paths,
):
    """``no-size``: the server sends no ``Content-Length``, so the filesystem
    reports no size for the file; V2 must still read it, not skip it.
    ``no-ranges``: the server sends the size but ignores range requests, and
    the file is larger than fsspec's 5 MiB read block; V2 reads it in blocks of
    ``streaming_read_buffer_size``, in one request, as V1 does."""
    ds = ray.data.read_text([url] * num_paths, filesystem=filesystem)
    assert isinstance(ds._logical_plan.dag, ReadFiles) == text_reader_version
    assert _to_lines(ds.take_all()) == ["some test data"] * num_lines * num_paths


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
