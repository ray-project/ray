import io

import pyarrow as pa
import pytest
from pyarrow import csv


@pytest.mark.parametrize("hadoop", [False, True])
def test_snappy_schema_sampling_is_incremental(hadoop):
    snappy = pytest.importorskip("snappy")
    from ray.data.datasource.file_based_datasource import _SnappyInputStream

    payload = b"value\n" + b"abcdefgh\n" * (1 << 20)
    compressed = io.BytesIO()
    compressor_cls = (
        snappy.HadoopStreamCompressor if hadoop else snappy.StreamCompressor
    )
    snappy.stream_compress(
        io.BytesIO(payload), compressed, compressor_cls=compressor_cls
    )

    class CountingFile(io.BytesIO):
        bytes_read = 0

        def read(self, size=-1):
            assert 0 < size <= 64 * 1024
            data = super().read(size)
            self.bytes_read += len(data)
            return data

    file = CountingFile(compressed.getvalue())
    decompressor_cls = (
        snappy.HadoopStreamDecompressor if hadoop else snappy.StreamDecompressor
    )
    with pa.PythonFile(
        _SnappyInputStream(file, decompressor_cls()), mode="r"
    ) as stream:
        reader = csv.open_csv(stream)
        assert reader.schema == pa.schema([("value", pa.string())])
        assert file.bytes_read < len(compressed.getvalue())
        assert reader.read_all().num_rows == 1 << 20
    assert file.closed


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
