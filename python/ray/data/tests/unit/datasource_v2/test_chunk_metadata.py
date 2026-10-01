"""Unit tests for the manifest chunk-metadata types in DataSourceV2."""
import pytest

from ray.data._internal.datasource_v2.formats.parquet.parquet_footer_types import (
    ParquetRowGroupChunkMetadata,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    ChunkMetadata,
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


def test_chunk_metadata_subclass_is_a_typeddict():
    # Ensures the subclass doesn't accidentally inherit unrelated keys.
    pmd: ChunkMetadata = create_chunk_metadata(
        ParquetRowGroupChunkMetadata,
        row_group_ids=(0,),
        num_rows=1,
        uncompressed_size=10,
        fully_matched=True,
        rg_sizes=(),
        rg_rows=(),
    )
    assert set(pmd.keys()) == {
        "row_group_ids",
        "num_rows",
        "uncompressed_size",
        "fully_matched",
        "rg_sizes",
        "rg_rows",
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
