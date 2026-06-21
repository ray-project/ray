"""Integration tests for glob pattern support in read_xxx APIs.

These tests verify that glob patterns (*, **, ?, [...]) work correctly
in the path argument of read_parquet, read_csv, read_json, etc.
"""

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture(params=[False, True], ids=["v1", "v2"])
def use_datasource_v2(request, restore_data_context):
    restore_data_context.use_datasource_v2 = request.param


@pytest.fixture
def glob_test_dir(tmp_path):
    """Create standard glob test directory structure under tmp_path."""
    root = tmp_path / "glob_test"

    # flat/ (3 parquet files)
    flat = root / "flat"
    flat.mkdir(parents=True)
    for i in range(3):
        pd.DataFrame({"val": [i]}).to_parquet(flat / f"part-{i}.parquet")

    # nested/sub-a/ + nested/sub-b/ (3 parquet files)
    for subdir, files in [
        ("sub-a", ["data-0.parquet", "data-1.parquet"]),
        ("sub-b", ["data-2.parquet"]),
    ]:
        d = root / "nested" / subdir
        d.mkdir(parents=True)
        for f in files:
            idx = int(f.split("-")[1].split(".")[0])
            pd.DataFrame({"val": [idx]}).to_parquet(d / f)

    # sub/ (3 parquet files for ? pattern)
    sub = root / "sub"
    sub.mkdir(parents=True)
    for i in range(3):
        pd.DataFrame({"val": [i]}).to_parquet(sub / f"part-{i}.parquet")

    # sub1/, sub2/ (for bracket pattern)
    for name in ("sub1", "sub2"):
        d = root / name
        d.mkdir(parents=True)
        pd.DataFrame({"val": [int(name[-1])]}).to_parquet(d / "file.parquet")

    # mixed/ (CSV + JSON)
    mixed = root / "mixed"
    mixed.mkdir(parents=True)
    for i in range(2):
        pd.DataFrame({"id": [i], "val": [i]}).to_csv(
            mixed / f"log-{i}.csv", index=False
        )
        pd.DataFrame({"id": [i]}).to_json(
            mixed / f"events-{i}.json", orient="records", lines=True
        )

    # deep/** (11 layers)
    deep = root / "deep"
    for c in "abcdefghij":
        deep = deep / c
    deep.mkdir(parents=True)
    pd.DataFrame({"val": [42]}).to_parquet(deep / "file.parquet")

    # abc/**/c.parquet (** matching 0/1/2 intermediate dirs)
    (root / "abc").mkdir(parents=True)
    pd.DataFrame({"val": [0]}).to_parquet(root / "abc" / "c.parquet")
    (root / "abc" / "b").mkdir(parents=True)
    pd.DataFrame({"val": [1]}).to_parquet(root / "abc" / "b" / "c.parquet")
    (root / "abc" / "b" / "d").mkdir(parents=True)
    pd.DataFrame({"val": [2]}).to_parquet(root / "abc" / "b" / "d" / "c.parquet")

    return root


# --- Local filesystem tests ---


@pytest.mark.usefixtures("use_datasource_v2")
class TestLocalGlob:
    """Local filesystem glob tests."""

    def test_single_wildcard(self, ray_start_regular_shared, glob_test_dir):
        """*.parquet matches all parquet files in a directory."""
        ds = ray.data.read_parquet(str(glob_test_dir / "flat" / "*.parquet"))
        assert ds.count() == 3

    def test_recursive_glob(self, ray_start_regular_shared, glob_test_dir):
        """**/*.parquet recursively matches across subdirectories."""
        ds = ray.data.read_parquet(str(glob_test_dir / "nested" / "**" / "*.parquet"))
        assert ds.count() == 3

    def test_single_char(self, ray_start_regular_shared, glob_test_dir):
        """part-?.parquet matches part-0, part-1, part-2."""
        ds = ray.data.read_parquet(str(glob_test_dir / "sub" / "part-?.parquet"))
        assert ds.count() == 3

    def test_bracket_expr(self, ray_start_regular_shared, glob_test_dir):
        """sub[12]/file.parquet matches sub1 and sub2."""
        ds = ray.data.read_parquet(str(glob_test_dir / "sub[12]" / "file.parquet"))
        assert ds.count() == 2

    def test_deep_nesting(self, ray_start_regular_shared, glob_test_dir):
        """** matches through 11 levels of nesting."""
        ds = ray.data.read_parquet(str(glob_test_dir / "deep" / "**" / "file.parquet"))
        assert ds.count() == 1

    def test_double_star_middle(self, ray_start_regular_shared, glob_test_dir):
        """abc/**/c.parquet matches 0, 1, and 2 intermediate dirs."""
        ds = ray.data.read_parquet(str(glob_test_dir / "abc" / "**" / "c.parquet"))
        assert ds.count() == 3

    def test_mixed_paths(self, ray_start_regular_shared, glob_test_dir):
        """Glob + explicit path list, part-0 counted twice."""
        explicit = str(glob_test_dir / "flat" / "part-0.parquet")
        glob_pat = str(glob_test_dir / "flat" / "*.parquet")
        ds = ray.data.read_parquet([explicit, glob_pat])
        assert ds.count() == 4  # 1 explicit + 3 glob (part-0 duplicated)

    def test_relative_pattern(
        self, ray_start_regular_shared, glob_test_dir, monkeypatch
    ):
        """*.parquet without directory prefix resolves against cwd.

        Note: monkeypatch.chdir only affects the driver process. The glob
        expansion happens on the driver (in _resolve_paths_and_filesystem),
        so this test is valid even though workers have independent cwds.
        """
        monkeypatch.chdir(glob_test_dir / "flat")
        ds = ray.data.read_parquet("*.parquet")
        assert ds.count() == 3

    def test_relative_pattern_subdir(
        self, ray_start_regular_shared, glob_test_dir, monkeypatch
    ):
        """./nested/**/*.parquet with explicit relative prefix."""
        monkeypatch.chdir(glob_test_dir)
        ds = ray.data.read_parquet("./nested/**/*.parquet")
        assert ds.count() == 3

    def test_no_match_error(self, ray_start_regular_shared, glob_test_dir):
        """Glob matching no files raises ValueError."""
        with pytest.raises(ValueError, match="matched no files"):
            ray.data.read_parquet(str(glob_test_dir / "no_match_*.parquet"))

    def test_no_match_ignore(self, ray_start_regular_shared, glob_test_dir):
        """ignore_missing_paths=True returns empty Dataset."""
        ds = ray.data.read_parquet(
            str(glob_test_dir / "no_match_*.parquet"),
            ignore_missing_paths=True,
        )
        assert ds.count() == 0

    def test_plain_path_unchanged(self, ray_start_regular_shared, glob_test_dir):
        """Non-glob path behavior unchanged."""
        ds = ray.data.read_parquet(str(glob_test_dir / "flat" / "part-0.parquet"))
        assert ds.count() == 1

    def test_directory_path_unchanged(self, ray_start_regular_shared, glob_test_dir):
        """Directory path still works via directory expansion."""
        ds = ray.data.read_parquet(str(glob_test_dir / "flat"))
        assert ds.count() == 3

    def test_deterministic_order(self, ray_start_regular_shared, glob_test_dir):
        """Same glob produces same path order across multiple runs."""
        pattern = str(glob_test_dir / "flat" / "*.parquet")
        results = []
        for _ in range(3):
            ds = ray.data.read_parquet(pattern)
            results.append(ds.input_files())
        assert results[0] == results[1] == results[2]

    def test_multi_glob_patterns(self, ray_start_regular_shared, glob_test_dir):
        """Multiple glob patterns in one call."""
        ds = ray.data.read_parquet(
            [
                str(glob_test_dir / "flat" / "*.parquet"),
                str(glob_test_dir / "nested" / "**" / "*.parquet"),
            ]
        )
        assert ds.count() == 6


# --- Multi-datasource tests ---


class TestMultiDatasource:
    """Glob works across different datasource types.

    Only Parquet has a V2 datasource; other formats always use V1.
    """

    def test_csv_glob(self, ray_start_regular_shared, glob_test_dir):
        """read_csv with glob."""
        ds = ray.data.read_csv(str(glob_test_dir / "mixed" / "*.csv"))
        assert ds.count() == 2

    def test_json_glob(self, ray_start_regular_shared, glob_test_dir):
        """read_json with glob."""
        ds = ray.data.read_json(str(glob_test_dir / "mixed" / "*.json"))
        assert ds.count() == 2

    def test_text_glob(self, ray_start_regular_shared, glob_test_dir):
        """read_text with glob."""
        ds = ray.data.read_text(str(glob_test_dir / "mixed" / "*.csv"))
        assert ds.count() > 0

    def test_binary_glob(self, ray_start_regular_shared, glob_test_dir):
        """read_binary_files with glob."""
        ds = ray.data.read_binary_files(str(glob_test_dir / "mixed" / "*.csv"))
        assert ds.count() == 2


# --- S3 (moto) tests ---


class TestS3Glob:
    """S3 glob tests using moto mock server.

    Uses Ray's s3_fs / s3_path fixtures which spin up a moto_server
    subprocess and connect via pa.fs.S3FileSystem(endpoint_override=...).
    """

    def test_s3_single_wildcard(self, ray_start_regular_shared, s3_fs, s3_path):
        """s3://.../*.parquet matches all parquet files under a prefix."""
        setup_path = _unwrap_protocol(s3_path)
        for i in range(3):
            table = pa.Table.from_pandas(pd.DataFrame({"val": [i]}))
            pq.write_table(
                table,
                os.path.join(setup_path, f"part-{i}.parquet"),
                filesystem=s3_fs,
            )

        ds = ray.data.read_parquet(
            os.path.join(s3_path, "*.parquet"),
            filesystem=s3_fs,
        )
        assert ds.count() == 3

    def test_s3_recursive_glob(self, ray_start_regular_shared, s3_fs, s3_path):
        """s3://.../**/*.parquet recursively matches across prefixes."""
        setup_path = _unwrap_protocol(s3_path)
        # Create nested structure: sub/a.parquet, sub/deep/b.parquet, top.parquet
        s3_fs.create_dir(os.path.join(setup_path, "sub"))
        s3_fs.create_dir(os.path.join(setup_path, "sub", "deep"))

        table_a = pa.Table.from_pandas(pd.DataFrame({"val": [1]}))
        pq.write_table(
            table_a,
            os.path.join(setup_path, "sub", "a.parquet"),
            filesystem=s3_fs,
        )

        table_b = pa.Table.from_pandas(pd.DataFrame({"val": [2]}))
        pq.write_table(
            table_b,
            os.path.join(setup_path, "sub", "deep", "b.parquet"),
            filesystem=s3_fs,
        )

        table_top = pa.Table.from_pandas(pd.DataFrame({"val": [3]}))
        pq.write_table(
            table_top,
            os.path.join(setup_path, "top.parquet"),
            filesystem=s3_fs,
        )

        ds = ray.data.read_parquet(
            os.path.join(s3_path, "**", "*.parquet"),
            filesystem=s3_fs,
        )
        assert ds.count() == 3

    def test_s3_bucket_glob_rejected(self, ray_start_regular_shared, s3_fs, s3_path):
        """Glob in bucket/host name raises ValueError."""
        with pytest.raises(ValueError, match="wildcards in the bucket/host name"):
            ray.data.read_parquet("s3://bucket-*/*.parquet", filesystem=s3_fs)


# --- Edge case tests ---


@pytest.mark.usefixtures("use_datasource_v2")
class TestGlobEdgeCases:
    """Tests for edge cases caught by automated code review."""

    def test_ignore_missing_glob_returns_empty(
        self, ray_start_regular_shared, tmp_path
    ):
        """ignore_missing_paths=True with non-matching glob returns empty Dataset."""
        ds = ray.data.read_parquet(
            str(tmp_path / "nonexistent" / "*.parquet"),
            ignore_missing_paths=True,
        )
        assert ds.count() == 0

    def test_ignore_missing_multi_glob_mixed(
        self, ray_start_regular_shared, glob_test_dir
    ):
        """Mix of matching and non-matching globs with ignore_missing_paths."""
        existing = str(glob_test_dir / "flat" / "*.parquet")
        missing = str(glob_test_dir / "nope" / "*.parquet")
        ds = ray.data.read_parquet(
            [existing, missing],
            ignore_missing_paths=True,
        )
        assert ds.count() == 3

    def test_empty_glob_with_filter_no_crash(self, ray_start_regular_shared, tmp_path):
        """Empty glob result with file_extensions filter should not crash
        when ignore_missing_paths=True."""
        ds = ray.data.read_csv(
            str(tmp_path / "empty" / "*.csv"),
            ignore_missing_paths=True,
        )
        assert ds.count() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
