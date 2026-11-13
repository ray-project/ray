"""File statistics pruning against the existing Moto S3 fixture."""

import pyarrow as pa
import pyarrow.orc as orc
import pytest

from ray.data._internal.datasource_v2.formats.orc import orc_file_reader
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.expressions import col, lit


@pytest.mark.parametrize("mode", ["off", "file"])
@pytest.mark.parametrize(
    "predicate", [col("x") > 0, col("flag").is_not_null() & ~(col("flag") != lit(True))]
)
def test_orc_file_statistics_s3(s3_fs, s3_path, monkeypatch, mode, predicate):
    root = s3_path.removeprefix("s3://")
    paths = [root + "/excluded.orc", root + "/candidate.orc"]
    tables = [
        pa.table({"x": [-3, -1], "flag": pa.array([None, None], type=pa.bool_())}),
        pa.table({"x": [1, 2], "flag": [True, True]}),
    ]
    sizes = []
    for path, table in zip(paths, tables):
        buffer = pa.BufferOutputStream()
        orc.write_table(table, buffer, compression="zlib")
        data = buffer.getvalue()
        with s3_fs.open_output_stream(path) as sink:
            sink.write(data)
        sizes.append(data.size)
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=sizes, chunk_metadatas=[None, None]
    )
    streamed = []
    original = orc_file_reader.OrcFileReader._scan_with_statistics

    def scan(self, fragment, *args, **kwargs):
        class Fragment:
            def __getattr__(self, name):
                return getattr(fragment, name)

            def scanner(self, **scanner_kwargs):
                native = fragment.scanner(**scanner_kwargs)

                class Scanner:
                    def scan_batches(self):
                        streamed.append(fragment.path)
                        yield from native.scan_batches()

                return Scanner()

        yield from original(self, Fragment(), *args, **kwargs)

    monkeypatch.setattr(orc_file_reader.OrcFileReader, "_scan_with_statistics", scan)
    scanner = OrcScanner(
        schema=tables[0].schema,
        filesystem=s3_fs,
        predicate=predicate,
        statistics_pruning=mode,
    )
    actual = [
        row
        for batch in scanner.create_reader().read(manifest)
        for row in batch.to_pylist()
    ]
    expected = pa.concat_tables(tables).filter(predicate.to_pyarrow()).to_pylist()
    assert actual == expected
    assert sorted(streamed) == sorted(paths if mode == "off" else paths[1:])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
