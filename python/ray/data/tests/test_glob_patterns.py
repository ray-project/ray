"""Tests for glob pattern support in Ray Data read APIs."""

from pathlib import Path

import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture
def sample_parquet_files(tmp_path: Path):
    """Fixture providing sample parquet files for testing.

    Args:
        tmp_path: Pytest fixture providing a temporary directory path.

    Returns:
        tuple: (tmp_path, df1, df2, df3) with three parquet files created.
    """
    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df2 = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    df3 = pd.DataFrame({"a": [13, 14, 15], "b": [16, 17, 18]})

    df1.to_parquet(tmp_path / "file1.parquet")
    df2.to_parquet(tmp_path / "file2.parquet")
    df3.to_parquet(tmp_path / "file3.parquet")

    return tmp_path, df1, df2, df3


@pytest.fixture
def nested_parquet_files(tmp_path: Path):
    """Fixture providing nested directory structure with parquet files.

    Args:
        tmp_path: Pytest fixture providing a temporary directory path.

    Returns:
        tuple: (tmp_path, df1, df2, df3) with nested directories.
    """
    (tmp_path / "2024").mkdir()
    (tmp_path / "2025").mkdir()
    (tmp_path / "2025" / "Q1").mkdir()

    df1 = pd.DataFrame({"year": [2024], "value": [100]})
    df2 = pd.DataFrame({"year": [2025], "value": [200]})
    df3 = pd.DataFrame({"year": [2025], "value": [300]})

    df1.to_parquet(tmp_path / "2024" / "data.parquet")
    df2.to_parquet(tmp_path / "2025" / "data.parquet")
    df3.to_parquet(tmp_path / "2025" / "Q1" / "data.parquet")

    return tmp_path, df1, df2, df3


def test_single_wildcard_parquet(ray_start_regular_shared, sample_parquet_files):
    """Test single wildcard pattern (*.parquet) for parquet files."""
    tmp_path, df1, df2, df3 = sample_parquet_files

    # Create a non-parquet file that should be ignored
    (tmp_path / "file4.txt").write_text("not a parquet file")

    pattern = str(tmp_path / "*.parquet")
    ds = ray.data.read_parquet(pattern)

    result_df = ds.to_pandas().sort_values("a").reset_index(drop=True)
    expected_df = pd.concat([df1, df2, df3]).sort_values("a").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df)
    assert ds.count() == 9


def test_single_wildcard_csv(ray_start_regular_shared, tmp_path):
    """Test single wildcard pattern (*.csv) for CSV files."""
    df1 = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    df2 = pd.DataFrame({"x": [5, 6], "y": [7, 8]})

    df1.to_csv(tmp_path / "data1.csv", index=False)
    df2.to_csv(tmp_path / "data2.csv", index=False)

    pattern = str(tmp_path / "*.csv")
    ds = ray.data.read_csv(pattern)

    result_df = ds.to_pandas().sort_values("x").reset_index(drop=True)
    expected_df = pd.concat([df1, df2]).sort_values("x").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_recursive_glob_parquet(ray_start_regular_shared, nested_parquet_files):
    """Test recursive glob pattern (**/*.parquet) for nested directories."""
    tmp_path, df1, df2, df3 = nested_parquet_files

    pattern = str(tmp_path / "**" / "*.parquet")
    ds = ray.data.read_parquet(pattern)

    result_df = ds.to_pandas().sort_values("value").reset_index(drop=True)
    expected_df = pd.concat([df1, df2, df3]).sort_values("value").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df)
    assert ds.count() == 3


def test_character_wildcard(ray_start_regular_shared, tmp_path):
    """Test character wildcard pattern (file_?.csv) matching single characters."""
    df1 = pd.DataFrame({"id": [1]})
    df2 = pd.DataFrame({"id": [2]})
    df3 = pd.DataFrame({"id": [3]})
    df_long = pd.DataFrame({"id": [99]})

    df1.to_csv(tmp_path / "file_1.csv", index=False)
    df2.to_csv(tmp_path / "file_2.csv", index=False)
    df3.to_csv(tmp_path / "file_3.csv", index=False)
    df_long.to_csv(tmp_path / "file_10.csv", index=False)

    # ? should match file_1, file_2, file_3 but not file_10
    pattern = str(tmp_path / "file_?.csv")
    ds = ray.data.read_csv(pattern)

    result_df = ds.to_pandas().sort_values("id").reset_index(drop=True)
    expected_df = pd.concat([df1, df2, df3]).sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df)
    assert ds.count() == 3


def test_character_class(ray_start_regular_shared, tmp_path):
    """Test character class pattern ([0-9].json) matching character ranges."""
    df1 = pd.DataFrame({"num": [1]})
    df2 = pd.DataFrame({"num": [5]})
    df3 = pd.DataFrame({"num": [9]})
    df_letter = pd.DataFrame({"num": [99]})

    df1.to_json(tmp_path / "1.json", orient="records", lines=True)
    df2.to_json(tmp_path / "5.json", orient="records", lines=True)
    df3.to_json(tmp_path / "9.json", orient="records", lines=True)
    df_letter.to_json(tmp_path / "a.json", orient="records", lines=True)

    pattern = str(tmp_path / "[0-9].json")
    ds = ray.data.read_json(pattern)

    result_df = ds.to_pandas().sort_values("num").reset_index(drop=True)
    expected_df = pd.concat([df1, df2, df3]).sort_values("num").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df, expected_df)
    assert ds.count() == 3


def test_partitioned_paths_with_glob(ray_start_regular_shared, tmp_path):
    """Test glob patterns with partitioned directory structures."""
    for year in [2023, 2024]:
        for month in [1, 2]:
            path = tmp_path / f"year={year}" / f"month={month}"
            path.mkdir(parents=True)
            df = pd.DataFrame({"value": [year * 100 + month]})
            df.to_parquet(path / "data.parquet")

    # Match only 2024 data
    pattern = str(tmp_path / "year=2024" / "month=*" / "*.parquet")
    ds = ray.data.read_parquet(pattern)

    assert ds.count() == 2
    result_df = ds.to_pandas()
    assert all(val >= 2024 * 100 for val in result_df["value"])


def test_mixing_glob_and_explicit_paths(ray_start_regular_shared, tmp_path):
    """Test mixing glob patterns with explicit file paths."""
    df1 = pd.DataFrame({"val": [1]})
    df2 = pd.DataFrame({"val": [2]})
    df3 = pd.DataFrame({"val": [3]})

    df1.to_parquet(tmp_path / "file1.parquet")
    df2.to_parquet(tmp_path / "file2.parquet")
    df3.to_parquet(tmp_path / "file3.parquet")

    pattern = str(tmp_path / "file[12].parquet")
    explicit = str(tmp_path / "file3.parquet")

    ds = ray.data.read_parquet([pattern, explicit])

    assert ds.count() == 3


@pytest.mark.parametrize(
    "pattern_suffix,error_match",
    [
        pytest.param("empty/*.parquet", "matched no files", id="empty_directory"),
        pytest.param("nonexistent/*.parquet", "matched no files", id="nonexistent_dir"),
    ],
)
def test_glob_no_matches_error(
    ray_start_regular_shared, tmp_path, pattern_suffix, error_match
):
    """Test that glob pattern with no matches raises clear error."""
    if "empty" in pattern_suffix:
        (tmp_path / "empty").mkdir()

    pattern = str(tmp_path / pattern_suffix)

    with pytest.raises(ValueError, match=error_match):
        ray.data.read_parquet(pattern)


def test_glob_with_file_extensions_parameter(ray_start_regular_shared, tmp_path):
    """Test that glob works correctly with file_extensions parameter."""
    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [3, 4]})

    df1.to_parquet(tmp_path / "data.parquet")
    df2.to_parquet(tmp_path / "data.parquet.backup")

    # Glob should expand, then file_extensions should filter
    pattern = str(tmp_path / "*")
    ds = ray.data.read_parquet(pattern, file_extensions=["parquet"])

    assert ds.count() == 2


def test_non_glob_paths_still_work(ray_start_regular_shared, tmp_path):
    """Test that non-glob paths continue to work as before."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    df.to_parquet(tmp_path / "data.parquet")

    ds = ray.data.read_parquet(str(tmp_path / "data.parquet"))

    pd.testing.assert_frame_equal(ds.to_pandas(), df)


def test_directory_path_still_works(ray_start_regular_shared, tmp_path):
    """Test that directory paths continue to work as before."""
    df1 = pd.DataFrame({"x": [1]})
    df2 = pd.DataFrame({"x": [2]})

    df1.to_parquet(tmp_path / "file1.parquet")
    df2.to_parquet(tmp_path / "file2.parquet")

    ds = ray.data.read_parquet(str(tmp_path), file_extensions=["parquet"])

    assert ds.count() == 2


def test_glob_deterministic_ordering(ray_start_regular_shared, tmp_path):
    """Test that glob expansion returns files in deterministic order."""
    for i in range(10):
        df = pd.DataFrame({"id": [i]})
        df.to_parquet(tmp_path / f"file{i:02d}.parquet")

    pattern = str(tmp_path / "*.parquet")

    paths1 = ray.data.read_parquet(pattern)._plan.meta()["input_files"]
    paths2 = ray.data.read_parquet(pattern)._plan.meta()["input_files"]

    assert paths1 == paths2


def test_multiple_glob_patterns(ray_start_regular_shared, tmp_path):
    """Test multiple glob patterns in a single read call."""
    (tmp_path / "dir1").mkdir()
    (tmp_path / "dir2").mkdir()

    df1 = pd.DataFrame({"source": ["dir1"], "val": [1]})
    df2 = pd.DataFrame({"source": ["dir2"], "val": [2]})

    df1.to_parquet(tmp_path / "dir1" / "data.parquet")
    df2.to_parquet(tmp_path / "dir2" / "data.parquet")

    pattern1 = str(tmp_path / "dir1" / "*.parquet")
    pattern2 = str(tmp_path / "dir2" / "*.parquet")

    ds = ray.data.read_parquet([pattern1, pattern2])

    assert ds.count() == 2


def test_glob_with_text_files(ray_start_regular_shared, tmp_path):
    """Test glob patterns with text files."""
    (tmp_path / "log1.txt").write_text("line1\nline2\n")
    (tmp_path / "log2.txt").write_text("line3\nline4\n")
    (tmp_path / "readme.md").write_text("not a log\n")

    pattern = str(tmp_path / "log*.txt")
    ds = ray.data.read_text(pattern)

    assert ds.count() == 4  # 4 lines total from 2 files


def test_glob_with_binary_files(ray_start_regular_shared, tmp_path):
    """Test glob patterns with binary files."""
    (tmp_path / "data1.bin").write_bytes(b"binary1")
    (tmp_path / "data2.bin").write_bytes(b"binary2")
    (tmp_path / "other.txt").write_bytes(b"text")

    pattern = str(tmp_path / "*.bin")
    ds = ray.data.read_binary_files(pattern)

    assert ds.count() == 2


def test_empty_pattern(ray_start_regular_shared):
    """Test that empty patterns are handled appropriately."""
    with pytest.raises(ValueError, match="Must provide at least one path"):
        ray.data.read_parquet([])


def test_glob_with_symlinks(ray_start_regular_shared, tmp_path):
    """Test glob patterns with symbolic links."""
    df = pd.DataFrame({"val": [1]})
    actual_file = tmp_path / "actual.parquet"
    df.to_parquet(actual_file)

    symlink = tmp_path / "link.parquet"
    try:
        symlink.symlink_to(actual_file)
    except OSError:
        pytest.skip("Symlinks not supported on this platform")

    pattern = str(tmp_path / "*.parquet")
    ds = ray.data.read_parquet(pattern)

    # Should work without error, may read both or just one
    assert ds.count() >= 1


# Cloud storage tests (requires credentials)
@pytest.mark.skip(reason="Requires S3 credentials and setup")
def test_s3_glob_pattern():
    """Test glob patterns with S3 filesystem."""
    pass


@pytest.mark.skip(reason="Requires GCS credentials and setup")
def test_gcs_glob_pattern():
    """Test glob patterns with GCS filesystem."""
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
