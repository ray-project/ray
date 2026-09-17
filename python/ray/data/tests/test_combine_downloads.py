from typing import List

import pyarrow.fs as pa_fs
import pytest

import ray
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.one_to_one_operator import Download
from ray.data._internal.logical.rules.combine_downloads import CombineDownloads
from ray.data.context import DataContext
from ray.data.expressions import download
from ray.data.tests.conftest import *  # noqa


def _apply_and_collect_ops_for(dag: LogicalOperator) -> List[Download]:
    """Apply CombineDownloads and collect Download ops from the optimized plan."""
    plan = LogicalPlan(dag, DataContext.get_current())
    optimized_plan = CombineDownloads().apply(plan)
    return [
        op for op in optimized_plan.dag.post_order_iter() if isinstance(op, Download)
    ]


def test_combine_sequential_downloads():
    # Create a chain: InputData -> Download -> Download -> Download
    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        input_dependencies=[download1],
    )
    download3 = Download(
        uri_column_names=["uri3"],
        output_bytes_column_names=["bytes3"],
        input_dependencies=[download2],
    )

    download_ops = _apply_and_collect_ops_for(download3)

    # Should have only 1 Download operator with all 3 columns
    assert len(download_ops) == 1
    combined = download_ops[0]
    assert combined.uri_column_names == ["uri1", "uri2", "uri3"]
    assert combined.output_bytes_column_names == ["bytes1", "bytes2", "bytes3"]
    assert combined.ray_remote_args == {}


def test_single_download_unchanged():
    """A single download should not be modified."""
    source = InputData([])
    download_op = Download(
        uri_column_names=["uri1", "uri2"],
        output_bytes_column_names=["bytes1", "bytes2"],
        input_dependencies=[source],
    )

    plan = LogicalPlan(download_op, DataContext.get_current())
    optimized_plan = CombineDownloads().apply(plan)

    # Plan should be unchanged
    assert optimized_plan is plan
    download_ops = [
        op for op in optimized_plan.dag.post_order_iter() if isinstance(op, Download)
    ]
    assert len(download_ops) == 1
    assert download_ops[0] is download_op


def test_different_ray_remote_args_not_combined():
    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        ray_remote_args={"num_cpus": 1},
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        ray_remote_args={"num_cpus": 2},
        input_dependencies=[download1],
    )

    download_ops = _apply_and_collect_ops_for(download2)

    # Should have 2 separate Download operators due to different resources
    assert len(download_ops) == 2
    assert download_ops[0].ray_remote_args == {"num_cpus": 1}
    assert download_ops[1].ray_remote_args == {"num_cpus": 2}


def test_same_ray_remote_args_are_combined():
    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        ray_remote_args={"num_cpus": 2},
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        ray_remote_args={"num_cpus": 2},
        input_dependencies=[download1],
    )
    download3 = Download(
        uri_column_names=["uri3"],
        output_bytes_column_names=["bytes3"],
        ray_remote_args={"num_cpus": 2},
        input_dependencies=[download2],
    )

    download_ops = _apply_and_collect_ops_for(download3)

    assert len(download_ops) == 1
    combined = download_ops[0]
    assert combined.uri_column_names == ["uri1", "uri2", "uri3"]
    assert combined.output_bytes_column_names == ["bytes1", "bytes2", "bytes3"]
    assert combined.ray_remote_args == {"num_cpus": 2}


@pytest.mark.parametrize(
    "inheritable_arg",
    [
        {"scheduling_strategy": "SPREAD"},
        {"label_selector": {"zone": "us-west-2a"}},
    ],
    ids=["scheduling_strategy", "label_selector"],
)
def test_upstream_inheritable_args_are_preserved(inheritable_arg):
    """Inheritable upstream args must survive combining.

    `are_remote_args_compatible` deems these pairs compatible by inheriting the
    upstream value, so the merged operator has to carry it rather than keeping only
    the downstream args.
    """
    upstream_args = {"num_cpus": 1, **inheritable_arg}

    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        ray_remote_args=upstream_args,
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        ray_remote_args={"num_cpus": 1},
        input_dependencies=[download1],
    )

    download_ops = _apply_and_collect_ops_for(download2)

    # The pair is compatible, so they combine...
    assert len(download_ops) == 1
    combined = download_ops[0]
    assert combined.uri_column_names == ["uri1", "uri2"]
    # ...and the upstream scheduling settings must not be lost.
    assert combined.ray_remote_args == upstream_args


def test_filesystem_is_preserved_when_combining():
    """The combined operator must retain the filesystem, not silently drop it."""
    filesystem = pa_fs.LocalFileSystem()

    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        filesystem=filesystem,
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        filesystem=filesystem,
        input_dependencies=[download1],
    )

    download_ops = _apply_and_collect_ops_for(download2)

    assert len(download_ops) == 1
    combined = download_ops[0]
    assert combined.uri_column_names == ["uri1", "uri2"]
    assert combined.filesystem is filesystem


def test_different_filesystems_not_combined():
    """Operators reading through different filesystems must stay separate."""
    fs1 = pa_fs.S3FileSystem(region="us-west-2", anonymous=True)
    fs2 = pa_fs.S3FileSystem(region="us-east-1", anonymous=True)

    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        filesystem=fs1,
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        filesystem=fs2,
        input_dependencies=[download1],
    )

    download_ops = _apply_and_collect_ops_for(download2)

    assert len(download_ops) == 2
    assert download_ops[0].filesystem is fs1
    assert download_ops[1].filesystem is fs2


def test_custom_filesystem_not_combined_with_default():
    """An explicit filesystem isn't interchangeable with scheme auto-detection."""
    filesystem = pa_fs.LocalFileSystem()

    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        filesystem=filesystem,
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        filesystem=None,
        input_dependencies=[download1],
    )

    download_ops = _apply_and_collect_ops_for(download2)

    assert len(download_ops) == 2
    assert download_ops[0].filesystem is filesystem
    assert download_ops[1].filesystem is None


def test_equal_but_distinct_filesystems_are_combined():
    """Distinct-but-equal filesystem instances should still fuse."""
    fs1 = pa_fs.S3FileSystem(region="us-west-2", anonymous=True)
    fs2 = pa_fs.S3FileSystem(region="us-west-2", anonymous=True)
    assert fs1 is not fs2 and fs1 == fs2

    source = InputData([])
    download1 = Download(
        uri_column_names=["uri1"],
        output_bytes_column_names=["bytes1"],
        filesystem=fs1,
        input_dependencies=[source],
    )
    download2 = Download(
        uri_column_names=["uri2"],
        output_bytes_column_names=["bytes2"],
        filesystem=fs2,
        input_dependencies=[download1],
    )

    download_ops = _apply_and_collect_ops_for(download2)

    assert len(download_ops) == 1
    assert download_ops[0].filesystem == fs1


def test_combine_downloads_correctness(ray_start_10_cpus_shared, tmp_path):
    path1 = tmp_path / "file1.txt"
    path1.write_bytes("spam".encode())
    path2 = tmp_path / "file2.txt"
    path2.write_bytes("ham".encode())

    ds = (
        ray.data.from_items([{"uri1": str(path1), "uri2": str(path2)}])
        .with_column("bytes1", download("uri1"))
        .with_column("bytes2", download("uri2"))
    )
    results = ds.take_all()

    assert len(results) == 1
    result = results[0]
    assert result["bytes1"] == b"spam"
    assert result["bytes2"] == b"ham"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
