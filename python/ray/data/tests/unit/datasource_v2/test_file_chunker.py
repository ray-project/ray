"""Unit tests for ``FileChunker`` implementations in DataSourceV2."""
from typing import cast

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


class TestCreateChunkMetadata:
    def test_validates_missing_keys(self):
        with pytest.raises(ValueError, match="Missing required keys"):
            create_chunk_metadata(ParquetFileChunkMetadata, chunk_idx=0)

    def test_validates_unexpected_keys(self):
        with pytest.raises(ValueError, match="Unexpected keys"):
            create_chunk_metadata(
                ParquetFileChunkMetadata,
                chunk_idx=0,
                total_num_chunks=1,
                extra_field="boom",
            )

    def test_returns_dict_with_keys(self):
        md = create_chunk_metadata(
            ParquetFileChunkMetadata, chunk_idx=2, total_num_chunks=5
        )
        assert md == {"chunk_idx": 2, "total_num_chunks": 5}


class TestWholeFileChunker:
    def test_yields_single_none_chunk(self):
        chunker = WholeFileChunker()
        chunks = list(chunker.generate_chunk_metadatas("foo.bin", 12345))
        assert chunks == [(None, 12345)]


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


class TestParquetFileChunker:
    def test_small_file_yields_whole(self):
        chunker = ParquetFileChunker(target_chunk_size=256 * 1024 * 1024)
        chunks = list(
            chunker.generate_chunk_metadatas("data.parquet", 100 * 1024 * 1024)
        )
        assert chunks == [(None, 100 * 1024 * 1024)]

    def test_at_target_yields_whole(self):
        chunker = ParquetFileChunker(target_chunk_size=256 * 1024 * 1024)
        chunks = list(
            chunker.generate_chunk_metadatas("data.parquet", 256 * 1024 * 1024)
        )
        assert chunks == [(None, 256 * 1024 * 1024)]

    @pytest.mark.parametrize(
        "file_size,expected_num_chunks",
        [
            (257 * 1024 * 1024, 2),
            (300 * 1024 * 1024, 2),
            (512 * 1024 * 1024, 2),
            (600 * 1024 * 1024, 3),
            (1024 * 1024 * 1024, 4),
        ],
    )
    def test_large_files_produce_chunks(self, file_size, expected_num_chunks):
        chunker = ParquetFileChunker(target_chunk_size=256 * 1024 * 1024)
        chunks = list(chunker.generate_chunk_metadatas("data.parquet", file_size))
        assert len(chunks) == expected_num_chunks
        for i, (md, _) in enumerate(chunks):
            assert isinstance(md, dict)
            assert md["chunk_idx"] == i
            assert md["total_num_chunks"] == expected_num_chunks

    def test_default_target_chunk_size_from_context(self, restore_data_context):
        from ray.data.context import DataContext

        DataContext.get_current().parquet_chunker_target_chunk_size = 1024
        chunker = ParquetFileChunker()
        assert chunker._target_chunk_size == 1024

    def test_ctor_arg_takes_precedence_over_context(self, restore_data_context):
        from ray.data.context import DataContext

        DataContext.get_current().parquet_chunker_target_chunk_size = 1024
        chunker = ParquetFileChunker(target_chunk_size=2048)
        assert chunker._target_chunk_size == 2048


def test_chunk_metadata_subclasses_are_typeddicts():
    # Ensures the subclasses don't accidentally inherit unrelated keys.
    pmd: ChunkMetadata = create_chunk_metadata(
        ParquetFileChunkMetadata, chunk_idx=0, total_num_chunks=1
    )
    lmd: ChunkMetadata = create_chunk_metadata(
        LineDelimitedFileChunkMetadata,
        chunk_byte_start_idx=0,
        chunk_byte_end_idx=10,
    )
    assert set(pmd.keys()) == {"chunk_idx", "total_num_chunks"}
    assert set(lmd.keys()) == {"chunk_byte_start_idx", "chunk_byte_end_idx"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
