"""Unit tests for glob helper functions in path_util.py.

These tests verify the pure helper functions used by glob pattern
expansion without requiring Ray initialization or filesystem access.
"""

import os

import pytest

from ray.data.datasource.path_util import (
    _expand_glob,
    _glob_match_path,
    _has_glob_chars,
    _split_glob_base,
)

# --- _has_glob_chars ---


class TestHasGlobChars:
    def test_star(self):
        assert _has_glob_chars("*.parquet")

    def test_question_mark_local(self):
        # '?' in local paths is treated as literal (not a glob char)
        assert not _has_glob_chars("file?.parquet")

    def test_question_mark_cloud(self):
        # '?' in cloud paths is consumed by urlparse as a query separator,
        # so it cannot be detected as a glob char.  This is acceptable
        # because '?' in S3/GCS keys is uncommon and users can use '*' instead.
        assert not _has_glob_chars("s3://bucket/file?.parquet")

    def test_bracket_expression_local(self):
        # '[...]' in local paths is treated as literal (not a glob char)
        assert not _has_glob_chars("sub[12]/file.parquet")

    def test_bracket_expression_cloud(self):
        # '[...]' in cloud paths IS treated as a glob char
        assert _has_glob_chars("s3://bucket/sub[12]/file.parquet")

    def test_no_glob(self):
        assert not _has_glob_chars("/data/file.parquet")

    def test_literal_bracket_with_space(self):
        """file [1].parquet — space before [ means literal, not glob."""
        assert not _has_glob_chars("file [1].parquet")

    def test_bracket_without_closing(self):
        """Lone [ without ] is literal."""
        assert not _has_glob_chars("file[.parquet")

    def test_uri_query_question_mark(self):
        """? in URI query string is not a glob char."""
        assert not _has_glob_chars("s3://bucket/path?X-Amz-Signature=x")

    def test_uri_path_with_glob(self):
        assert _has_glob_chars("s3://bucket/prefix/*.parquet")

    def test_uri_netloc_glob(self):
        """* in bucket name (netloc) should be detected."""
        assert _has_glob_chars("s3://bucket-*/file.parquet")

    def test_double_star(self):
        assert _has_glob_chars("**/*.parquet")


# --- _split_glob_base ---


class TestSplitGlobBase:
    def test_local_simple(self):
        base, pat = _split_glob_base("/data/*.parquet")
        assert base == "/data"
        assert pat == "*.parquet"

    def test_local_nested(self):
        base, pat = _split_glob_base("/data/sub/**/*.parquet")
        assert base == "/data/sub"
        assert pat == "**/*.parquet"

    def test_local_no_dir(self):
        base, pat = _split_glob_base("*.parquet")
        assert base == ""
        assert pat == "*.parquet"

    def test_uri_simple(self):
        base, pat = _split_glob_base("s3://bucket/prefix/*.parquet")
        assert base == "s3://bucket/prefix"
        assert pat == "*.parquet"

    def test_uri_bracket_in_dir(self):
        base, pat = _split_glob_base("s3://bucket/sub[12]/file.parquet")
        assert base == "s3://bucket"
        assert pat == "sub[12]/file.parquet"

    def test_uri_bucket_level_glob(self):
        base, pat = _split_glob_base("s3://bucket-*/file.parquet")
        assert base == "s3://bucket-*"
        assert pat == "file.parquet"

    def test_gs_uri(self):
        base, pat = _split_glob_base("gs://my-bucket/data/*.csv")
        assert base == "gs://my-bucket/data"
        assert pat == "*.csv"


# --- _glob_match_path / _glob_match_segs ---


class TestGlobMatchPath:
    def test_star_single_segment(self):
        assert _glob_match_path("*.parquet", "file.parquet")

    def test_star_no_cross_slash(self):
        """* should NOT match across / boundaries."""
        assert not _glob_match_path("*.parquet", "sub/file.parquet")

    def test_double_star_crosses_segments(self):
        assert _glob_match_path("**/*.parquet", "sub/file.parquet")

    def test_double_star_deep(self):
        assert _glob_match_path("**/*.parquet", "a/b/c/file.parquet")

    def test_double_star_zero_segments(self):
        assert _glob_match_path("**/file.parquet", "file.parquet")

    def test_bracket(self):
        assert _glob_match_path("sub[12]/file.parquet", "sub1/file.parquet")
        assert _glob_match_path("sub[12]/file.parquet", "sub2/file.parquet")
        assert not _glob_match_path("sub[12]/file.parquet", "sub3/file.parquet")

    def test_question_mark(self):
        assert _glob_match_path("part-?.parquet", "part-0.parquet")
        assert not _glob_match_path("part-?.parquet", "part-10.parquet")

    def test_literal(self):
        assert _glob_match_path("file.parquet", "file.parquet")
        assert not _glob_match_path("file.parquet", "other.parquet")

    def test_double_star_prefix(self):
        """**/c.parquet matches c.parquet, b/c.parquet, b/d/c.parquet."""
        assert _glob_match_path("**/c.parquet", "c.parquet")
        assert _glob_match_path("**/c.parquet", "b/c.parquet")
        assert _glob_match_path("**/c.parquet", "b/d/c.parquet")


# --- _expand_glob ---


class TestExpandGlob:
    """Unit tests for _expand_glob using the local filesystem."""

    def test_simple_wildcard(self, tmp_path):
        (tmp_path / "a.parquet").write_text("a")
        (tmp_path / "b.parquet").write_text("b")
        (tmp_path / "c.json").write_text("c")

        result = _expand_glob(str(tmp_path / "*.parquet"))

        assert len(result) == 2
        assert all(p.endswith(".parquet") for p in result)
        assert result == sorted(result)
        assert all(os.path.isabs(p) for p in result)

    def test_recursive_glob(self, tmp_path):
        (tmp_path / "flat.parquet").write_text("flat")
        nested = tmp_path / "sub" / "deep"
        nested.mkdir(parents=True)
        (nested / "nested.parquet").write_text("nested")

        result = _expand_glob(str(tmp_path / "**" / "*.parquet"))

        assert len(result) == 2
        assert any("flat.parquet" in p for p in result)
        assert any("nested.parquet" in p for p in result)

    def test_no_match_raises(self, tmp_path):
        with pytest.raises(ValueError, match="matched no files"):
            _expand_glob(str(tmp_path / "*.parquet"))

    def test_no_match_ignored(self, tmp_path):
        result = _expand_glob(str(tmp_path / "*.parquet"), ignore_missing_paths=True)
        assert result == []

    def test_relative_pattern(self, tmp_path, monkeypatch):
        (tmp_path / "a.parquet").write_text("a")
        (tmp_path / "b.parquet").write_text("b")
        monkeypatch.chdir(tmp_path)

        result = _expand_glob("*.parquet")

        assert len(result) == 2
        assert all(os.path.isabs(p) for p in result)

    def test_sorted_absolute_results(self, tmp_path):
        for name in ["z.parquet", "a.parquet", "m.parquet"]:
            (tmp_path / name).write_text(name)

        result = _expand_glob(str(tmp_path / "*.parquet"))

        assert result == sorted(result)
        assert [os.path.basename(p) for p in result] == [
            "a.parquet",
            "m.parquet",
            "z.parquet",
        ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
