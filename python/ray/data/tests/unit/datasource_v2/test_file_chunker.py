"""Unit tests for ``FileChunker`` implementations in DataSourceV2."""
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    LineDelimitedFileChunker,
    LineDelimitedFileChunkMetadata,
    ParquetFileChunker,
    ParquetFileChunkMetadata,
    WholeFileChunker,
    create_chunk_metadata,
)


def _write_parquet_with_row_groups(
    path: str, num_row_groups: int, rows_per_group: int = 10
) -> int:
    """Write a Parquet file with exactly ``num_row_groups`` row groups.

    Returns the on-disk file size in bytes.
    """
    n = num_row_groups * rows_per_group
    table = pa.table({"a": list(range(n))})
    pq.write_table(table, path, row_group_size=rows_per_group)
    return Path(path).stat().st_size


class TestCreateChunkMetadata:
    def test_validates_missing_keys(self):
        with pytest.raises(ValueError, match="Missing required keys"):
            create_chunk_metadata(ParquetFileChunkMetadata, row_group_start=0)

    def test_validates_unexpected_keys(self):
        with pytest.raises(ValueError, match="Unexpected keys"):
            create_chunk_metadata(
                ParquetFileChunkMetadata,
                row_group_start=0,
                row_group_end=1,
                extra_field="boom",
            )

    def test_returns_dict_with_keys(self):
        md = create_chunk_metadata(
            ParquetFileChunkMetadata, row_group_start=2, row_group_end=5
        )
        assert md == {"row_group_start": 2, "row_group_end": 5}


class TestWholeFileChunker:
    def test_yields_single_none_chunk(self):
        chunker = WholeFileChunker()
        chunks = list(chunker.generate_chunk_metadatas("foo.bin", 12345))
        assert chunks == [(None, 12345)]

    def test_does_not_read_metadata(self):
        assert WholeFileChunker.reads_file_metadata is False


class TestLineDelimitedFileChunker:
    def test_chunks_uncompressed_file(self):
        chunker = LineDelimitedFileChunker()
        # 600MB file at 256MB chunks -> 3 chunks (256, 256, 88).
        chunks = list(chunker.generate_chunk_metadatas("data.jsonl", 600 * 1024 * 1024))
        assert len(chunks) == 3
        for i, (md, size) in enumerate(chunks):
            assert md is not None
            md = cast(LineDelimitedFileChunkMetadata, md)
            assert md["chunk_byte_start_idx"] == i * 256 * 1024 * 1024
            assert size == md["chunk_byte_end_idx"] - md["chunk_byte_start_idx"]
        # Final chunk should clip to file_size.
        last_md = cast(LineDelimitedFileChunkMetadata, chunks[-1][0])
        assert last_md["chunk_byte_end_idx"] == 600 * 1024 * 1024

    def test_compressed_file_yields_whole(self):
        chunker = LineDelimitedFileChunker()
        chunks = list(chunker.generate_chunk_metadatas("data.jsonl.gz", 1024))
        assert chunks == [(None, 1024)]

    def test_does_not_read_metadata(self):
        assert LineDelimitedFileChunker.reads_file_metadata is False


class TestParquetFileChunker:
    def test_reads_file_metadata_flag(self):
        assert ParquetFileChunker.reads_file_metadata is True

    @pytest.mark.parametrize(
        "target_chunk_size, expected_ranges",
        [
            # target < row-group size → K=1, one chunk per row group.
            (1, [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]),
            # target ≫ file size → all row groups bundled into one chunk.
            (10 * 1024**3, [(0, 5)]),
        ],
        ids=["target_below_rg_size", "target_above_file_size"],
    )
    def test_row_group_bundling_by_target(
        self, tmp_path, target_chunk_size, expected_ranges
    ):
        p = str(tmp_path / "d.parquet")
        size = _write_parquet_with_row_groups(p, num_row_groups=5)
        chunker = ParquetFileChunker(target_chunk_size=target_chunk_size)
        chunks = list(chunker.generate_chunk_metadatas(p, size))
        ranges = [(m["row_group_start"], m["row_group_end"]) for m, _ in chunks]
        assert ranges == expected_ranges

    def test_chunk_size_equals_summed_row_group_bytes(self, tmp_path):
        p = str(tmp_path / "d.parquet")
        size = _write_parquet_with_row_groups(p, num_row_groups=4)
        chunker = ParquetFileChunker(target_chunk_size=1)
        chunks = list(chunker.generate_chunk_metadatas(p, size))
        md = pq.read_metadata(p)
        expected_total = sum(
            md.row_group(i).column(c).total_compressed_size
            for i in range(md.num_row_groups)
            for c in range(md.row_group(i).num_columns)
        )
        assert sum(sz for _, sz in chunks) == expected_total

    def test_ranges_are_contiguous_and_cover_all_row_groups(self, tmp_path):
        """Bundled ranges partition [0, num_row_groups) with no gaps/overlap."""
        p = str(tmp_path / "d.parquet")
        # Many small row groups + a moderate target → some bundling.
        size = _write_parquet_with_row_groups(p, num_row_groups=12, rows_per_group=5)
        n = pq.read_metadata(p).num_row_groups
        chunker = ParquetFileChunker(target_chunk_size=2_000)
        chunks = list(chunker.generate_chunk_metadatas(p, size))
        ranges = [(m["row_group_start"], m["row_group_end"]) for m, _ in chunks]
        # Contiguous, starts at 0, ends at n, each non-empty.
        assert ranges[0][0] == 0
        assert ranges[-1][1] == n
        for (_, prev_end), (next_start, _) in zip(ranges, ranges[1:]):
            assert prev_end == next_start
        assert all(start < end for start, end in ranges)

    def test_corrupt_footer_falls_back_to_whole_file(self, tmp_path):
        p = str(tmp_path / "bad.parquet")
        Path(p).write_bytes(b"this is not a parquet file")
        chunker = ParquetFileChunker(target_chunk_size=1)
        chunks = list(chunker.generate_chunk_metadatas(p, 26))
        assert chunks == [(None, 26)]

    @pytest.mark.parametrize(
        "ctor_arg, ctx_chunk_size, ctx_min_block, expected",
        [
            # ctor arg wins over the context knobs.
            (2048, 1024, 7777, 2048),
            # An explicit 0 is honored (resolved with ``is not None``, not
            # ``or``), so it isn't silently treated as "unset" and overridden.
            (0, 1024, 7777, 0),
            # ctx chunk-size knob used when there's no ctor arg.
            (None, 1024, 7777, 1024),
            # Falls back to target_min_block_size when the chunk-size knob is unset.
            (None, None, 7777, 7777),
        ],
        ids=["ctor_arg", "ctor_arg_zero", "ctx_knob", "fallback_min_block"],
    )
    def test_target_chunk_size_resolution(
        self, restore_data_context, ctor_arg, ctx_chunk_size, ctx_min_block, expected
    ):
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        ctx.parquet_chunker_target_chunk_size = ctx_chunk_size
        ctx.target_min_block_size = ctx_min_block
        chunker = ParquetFileChunker(target_chunk_size=ctor_arg)
        assert chunker._target_chunk_size == expected


def test_chunk_metadata_subclasses_are_typeddicts():
    # Ensures the subclasses don't accidentally inherit unrelated keys.
    pmd: ChunkMetadata = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=0, row_group_end=1
    )
    lmd: ChunkMetadata = create_chunk_metadata(
        LineDelimitedFileChunkMetadata,
        chunk_byte_start_idx=0,
        chunk_byte_end_idx=10,
    )
    assert set(pmd.keys()) == {"row_group_start", "row_group_end"}
    assert set(lmd.keys()) == {"chunk_byte_start_idx", "chunk_byte_end_idx"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
