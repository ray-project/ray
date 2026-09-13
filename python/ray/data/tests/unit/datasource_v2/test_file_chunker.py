"""Unit tests for ``FileChunker`` implementations in DataSourceV2."""
from typing import cast

import pytest

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    LineDelimitedFileChunker,
    LineDelimitedFileChunkMetadata,
    ParquetRowGroupChunkMetadata,
    WholeFileChunker,
    create_chunk_metadata,
)


class TestCreateChunkMetadata:
    def test_validates_missing_keys(self):
        with pytest.raises(ValueError, match="Missing required keys"):
            create_chunk_metadata(ParquetRowGroupChunkMetadata, row_group_ids=(0,))

    def test_validates_unexpected_keys(self):
        with pytest.raises(ValueError, match="Unexpected keys"):
            create_chunk_metadata(
                ParquetRowGroupChunkMetadata,
                row_group_ids=(0,),
                num_rows=1,
                uncompressed_size=10,
                fully_matched=True,
                rg_sizes=(),
                rg_rows=(),
                extra_field="boom",
            )

    def test_returns_dict_with_keys(self):
        md = create_chunk_metadata(
            ParquetRowGroupChunkMetadata,
            row_group_ids=(0, 1),
            num_rows=5,
            uncompressed_size=10,
            fully_matched=True,
            rg_sizes=(),
            rg_rows=(),
        )
        assert md == {
            "row_group_ids": (0, 1),
            "num_rows": 5,
            "uncompressed_size": 10,
            "fully_matched": True,
            "rg_sizes": (),
            "rg_rows": (),
        }


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


def test_chunk_metadata_subclasses_are_typeddicts():
    # Ensures the subclasses don't accidentally inherit unrelated keys.
    pmd: ChunkMetadata = create_chunk_metadata(
        ParquetRowGroupChunkMetadata,
        row_group_ids=(0,),
        num_rows=1,
        uncompressed_size=10,
        fully_matched=True,
        rg_sizes=(),
        rg_rows=(),
    )
    lmd: ChunkMetadata = create_chunk_metadata(
        LineDelimitedFileChunkMetadata,
        chunk_byte_start_idx=0,
        chunk_byte_end_idx=10,
    )
    assert set(pmd.keys()) == {
        "row_group_ids",
        "num_rows",
        "uncompressed_size",
        "fully_matched",
        "rg_sizes",
        "rg_rows",
    }
    assert set(lmd.keys()) == {"chunk_byte_start_idx", "chunk_byte_end_idx"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
