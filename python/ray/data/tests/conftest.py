import copy
import os
import posixpath

import pytest
import pyarrow as pa
import pandas as pd
import numpy as np

import ray

from ray.data.block import BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.tests.mock_server import *  # noqa
from ray.data.datasource.file_based_datasource import BlockWritePathProvider
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray
from ray._private.utils import _get_pyarrow_version

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa
from ray.tests.conftest import _ray_start
from ray.tests.conftest import *  # noqa


@pytest.fixture(scope="module")
def ray_start_2_cpus_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
        yield res


@pytest.fixture(scope="module")
def ray_start_10_cpus_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=10, **param) as res:
        yield res


@pytest.fixture(scope="module")
def enable_strict_mode():
    ctx = ray.data.DataContext.get_current()
    ctx.strict_mode = True
    yield
    ctx.strict_mode = False


@pytest.fixture(scope="module")
def enable_nonstrict_mode():
    ctx = ray.data.DataContext.get_current()
    ctx.strict_mode = False
    yield
    ctx.strict_mode = True


@pytest.fixture(scope="function")
def aws_credentials():
    import os

    # Credentials dict that can be passed as kwargs to pa.fs.S3FileSystem
    credentials = dict(
        access_key="testing", secret_key="testing", session_token="testing"
    )

    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = credentials["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials["secret_key"]
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = credentials["session_token"]

    yield credentials
    os.environ = old_env


@pytest.fixture(scope="function")
def data_dir():
    yield "test_data"


@pytest.fixture(scope="function")
def data_dir_with_space():
    yield "test data"


@pytest.fixture(scope="function")
def data_dir_with_special_chars():
    yield "test data#fragment?query=test/"


@pytest.fixture(scope="function")
def s3_path(tmp_path, data_dir):
    yield "s3://" + posixpath.join(tmp_path, data_dir).strip("/")


@pytest.fixture(scope="function")
def s3_path_with_space(tmp_path, data_dir_with_space):
    yield "s3://" + posixpath.join(tmp_path, data_dir_with_space).strip("/")


@pytest.fixture(scope="function")
def s3_path_with_special_chars(tmp_path, data_dir_with_special_chars):
    yield "s3://" + posixpath.join(tmp_path, data_dir_with_special_chars).lstrip("/")


@pytest.fixture(scope="function")
def s3_path_with_anonymous_crendential(tmp_path, data_dir):
    yield "s3://" + "anonymous@" + posixpath.join(tmp_path, data_dir).lstrip("/")


@pytest.fixture(scope="function")
def s3_fs(aws_credentials, s3_server, s3_path):
    yield from _s3_fs(aws_credentials, s3_server, s3_path)


@pytest.fixture(scope="function")
def s3_fs_with_space(aws_credentials, s3_server, s3_path_with_space):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_space)


@pytest.fixture(scope="function")
def s3_fs_with_special_chars(aws_credentials, s3_server, s3_path_with_special_chars):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_special_chars)


@pytest.fixture(scope="function")
def s3_fs_with_anonymous_crendential(
    aws_credentials, s3_server, s3_path_with_anonymous_crendential
):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_anonymous_crendential)


def _s3_fs(aws_credentials, s3_server, s3_path):
    from pkg_resources._vendor.packaging.version import parse as parse_version
    import urllib.parse

    kwargs = aws_credentials.copy()

    if parse_version(_get_pyarrow_version()) >= parse_version("9.0.0"):
        kwargs["allow_bucket_creation"] = True
        kwargs["allow_bucket_deletion"] = True

    fs = pa.fs.S3FileSystem(
        region="us-west-2",
        endpoint_override=s3_server,
        **kwargs,
    )
    if s3_path.startswith("s3://"):
        if "@" in s3_path:
            s3_path = s3_path.split("@")[-1]
        else:
            s3_path = s3_path[len("s3://") :]
    s3_path = urllib.parse.quote(s3_path)
    fs.create_dir(s3_path)
    yield fs


@pytest.fixture(scope="function")
def local_path(tmp_path, data_dir):
    path = os.path.join(tmp_path, data_dir)
    os.mkdir(path)
    yield path


@pytest.fixture(scope="function")
def local_fs():
    yield pa.fs.LocalFileSystem()


@pytest.fixture(scope="function")
def test_block_write_path_provider():
    class TestBlockWritePathProvider(BlockWritePathProvider):
        def _get_write_path_for_block(
            self,
            base_path,
            *,
            filesystem=None,
            datastream_uuid=None,
            block=None,
            block_index=None,
            file_format=None,
        ):
            num_rows = BlockAccessor.for_block(block).num_rows()
            suffix = (
                f"{block_index:06}_{num_rows:02}_{datastream_uuid}"
                f".test.{file_format}"
            )
            return posixpath.join(base_path, suffix)

    yield TestBlockWritePathProvider()


@pytest.fixture(scope="function")
def base_partitioned_df():
    yield pd.DataFrame(
        {"one": [1, 1, 1, 3, 3, 3], "two": ["a", "b", "c", "e", "f", "g"]}
    )


@pytest.fixture(scope="function")
def write_partitioned_df():
    def _write_partitioned_df(
        df,
        partition_keys,
        partition_path_encoder,
        file_writer_fn,
        file_name_suffix="_1",
    ):
        import urllib.parse

        df_partitions = [df for _, df in df.groupby(partition_keys, as_index=False)]
        paths = []
        for df_partition in df_partitions:
            partition_values = []
            for key in partition_keys:
                partition_values.append(str(df_partition[key].iloc[0]))
            path = partition_path_encoder(partition_values)
            partition_path_encoder.scheme.resolved_filesystem.create_dir(path)
            base_dir = partition_path_encoder.scheme.base_dir
            parsed_base_dir = urllib.parse.urlparse(base_dir)
            file_name = f"test_{file_name_suffix}.tmp"
            if parsed_base_dir.scheme:
                # replace the protocol removed by the partition path generator
                path = posixpath.join(f"{parsed_base_dir.scheme}://{path}", file_name)
            else:
                path = os.path.join(path, file_name)
            file_writer_fn(df_partition, path)
            paths.append(path)
        return paths

    yield _write_partitioned_df


@pytest.fixture(scope="function")
def write_base_partitioned_df(base_partitioned_df, write_partitioned_df):
    def _write_base_partitioned_df(
        partition_keys,
        partition_path_encoder,
        file_writer_fn,
    ):
        write_partitioned_df(
            base_partitioned_df,
            partition_keys,
            partition_path_encoder,
            file_writer_fn,
        )

    yield _write_base_partitioned_df


@pytest.fixture(scope="function")
def assert_base_partitioned_ds():
    def _assert_base_partitioned_ds(
        ds,
        count=6,
        num_input_files=2,
        num_rows=6,
        schema="{one: int64, two: string}",
        num_computed=2,
        sorted_values=None,
        ds_take_transform_fn=lambda taken: [[s["one"], s["two"]] for s in taken],
        sorted_values_transform_fn=lambda sorted_values: sorted_values,
    ):
        if sorted_values is None:
            sorted_values = [[1, "a"], [1, "b"], [1, "c"], [3, "e"], [3, "f"], [3, "g"]]
        # Test metadata ops.
        assert ds._plan.execute()._num_computed() == 0
        assert ds.count() == count, f"{ds.count()} != {count}"
        assert ds.size_bytes() > 0, f"{ds.size_bytes()} <= 0"
        assert ds.schema() is not None
        actual_input_files = ds.input_files()
        assert len(actual_input_files) == num_input_files, actual_input_files

        # For Datastreams with long string representations, the format will include
        # whitespace and newline characters, which is difficult to generalize
        # without implementing the formatting logic again (from
        # `ExecutionPlan.get_plan_as_string()`). Therefore, we remove whitespace
        # characters to test the string contents regardless of the string repr length.
        def _remove_whitespace(ds_str):
            for c in ["\n", "   ", " "]:
                ds_str = ds_str.replace(c, "")
            return ds_str

        assert "Datastream(num_blocks={},num_rows={},schema={})".format(
            num_input_files,
            num_rows,
            _remove_whitespace(schema),
        ) == _remove_whitespace(str(ds)), ds
        assert "Datastream(num_blocks={},num_rows={},schema={})".format(
            num_input_files,
            num_rows,
            _remove_whitespace(schema),
        ) == _remove_whitespace(repr(ds)), ds

        if num_computed is not None:
            assert (
                ds._plan.execute()._num_computed() == num_computed
            ), f"{ds._plan.execute()._num_computed()} != {num_computed}"

        # Force a data read.
        values = ds_take_transform_fn(ds.take_all())
        if num_computed is not None:
            assert (
                ds._plan.execute()._num_computed() == num_computed
            ), f"{ds._plan.execute()._num_computed()} != {num_computed}"
        actual_sorted_values = sorted_values_transform_fn(sorted(values))
        assert (
            actual_sorted_values == sorted_values
        ), f"{actual_sorted_values} != {sorted_values}"

    yield _assert_base_partitioned_ds


@pytest.fixture
def restore_data_context(request):
    """Restore any DataContext changes after the test runs"""
    original = copy.deepcopy(ray.data.context.DataContext.get_current())
    yield
    ray.data.context.DataContext._set_current(original)


@pytest.fixture(params=[True, False])
def use_push_based_shuffle(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.use_push_based_shuffle
    ctx.use_push_based_shuffle = request.param
    yield request.param
    ctx.use_push_based_shuffle = original


@pytest.fixture(params=[True, False])
def enable_automatic_tensor_extension_cast(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_tensor_extension_casting
    ctx.enable_tensor_extension_casting = request.param
    yield request.param
    ctx.enable_tensor_extension_casting = original


@pytest.fixture(params=[True, False])
def enable_auto_log_stats(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_auto_log_stats
    ctx.enable_auto_log_stats = request.param
    yield request.param
    ctx.enable_auto_log_stats = original


@pytest.fixture(params=[True])
def enable_dynamic_block_splitting(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.block_splitting_enabled
    ctx.block_splitting_enabled = request.param
    yield request.param
    ctx.block_splitting_enabled = original


@pytest.fixture(params=[1024])
def target_max_block_size(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.target_max_block_size
    ctx.target_max_block_size = request.param
    yield request.param
    ctx.target_max_block_size = original


@pytest.fixture
def enable_optimizer():
    ctx = ray.data.context.DataContext.get_current()
    original_backend = ctx.new_execution_backend
    original_optimizer = ctx.optimizer_enabled
    ctx.new_execution_backend = True
    ctx.optimizer_enabled = True
    yield
    ctx.new_execution_backend = original_backend
    ctx.optimizer_enabled = original_optimizer


@pytest.fixture
def enable_streaming_executor():
    ctx = ray.data.context.DataContext.get_current()
    original_backend = ctx.new_execution_backend
    use_streaming_executor = ctx.use_streaming_executor
    ctx.new_execution_backend = True
    ctx.use_streaming_executor = True
    yield
    ctx.new_execution_backend = original_backend
    ctx.use_streaming_executor = use_streaming_executor


# ===== Pandas datastream formats =====
@pytest.fixture(scope="function")
def ds_pandas_single_column_format(ray_start_regular_shared):
    in_df = pd.DataFrame({"column_1": [1, 2, 3, 4]})
    yield ray.data.from_pandas(in_df)


@pytest.fixture(scope="function")
def ds_pandas_multi_column_format(ray_start_regular_shared):
    in_df = pd.DataFrame({"column_1": [1, 2, 3, 4], "column_2": [1, -1, 1, -1]})
    yield ray.data.from_pandas(in_df)


@pytest.fixture(scope="function")
def ds_pandas_list_multi_column_format(ray_start_regular_shared):
    in_df = pd.DataFrame({"column_1": [1], "column_2": [1]})
    yield ray.data.from_pandas([in_df] * 4)


# ===== Arrow datastream formats =====
@pytest.fixture(scope="function")
def ds_arrow_single_column_format(ray_start_regular_shared):
    yield ray.data.from_arrow(pa.table({"column_1": [1, 2, 3, 4]}))


@pytest.fixture(scope="function")
def ds_arrow_single_column_tensor_format(ray_start_regular_shared):
    yield ray.data.from_arrow(
        pa.table(
            {
                TENSOR_COLUMN_NAME: ArrowTensorArray.from_numpy(
                    np.arange(16).reshape((4, 2, 2))
                )
            }
        )
    )


@pytest.fixture(scope="function")
def ds_arrow_multi_column_format(ray_start_regular_shared):
    yield ray.data.from_arrow(
        pa.table(
            {
                "column_1": [1, 2, 3, 4],
                "column_2": [1, -1, 1, -1],
            }
        )
    )


@pytest.fixture(scope="function")
def ds_list_arrow_multi_column_format(ray_start_regular_shared):
    yield ray.data.from_arrow([pa.table({"column_1": [1], "column_2": [1]})] * 4)


# ===== Numpy datastream formats =====
@pytest.fixture(scope="function")
def ds_numpy_single_column_tensor_format(ray_start_regular_shared):
    yield ray.data.from_numpy(np.arange(16).reshape((4, 2, 2)))


@pytest.fixture(scope="function")
def ds_numpy_list_of_ndarray_tensor_format(ray_start_regular_shared):
    yield ray.data.from_numpy([np.arange(4).reshape((1, 2, 2))] * 4)


@pytest.fixture(params=["5.0.0"])
def unsupported_pyarrow_version(request):
    orig_version = pa.__version__
    pa.__version__ = request.param
    # Unset pyarrow version cache.
    import ray._private.utils as utils

    utils._PYARROW_VERSION = None
    yield request.param
    pa.__version__ = orig_version


@pytest.fixture
def disable_pyarrow_version_check():
    os.environ["RAY_DISABLE_PYARROW_VERSION_CHECK"] = "1"
    yield
    del os.environ["RAY_DISABLE_PYARROW_VERSION_CHECK"]


# ===== Observability & Logging Fixtures =====
@pytest.fixture
def stage_two_block():
    block_params = {
        "num_rows": [10000, 5000],
        "size_bytes": [100, 50],
        "max_rss_bytes": [1024 * 1024 * 2, 1024 * 1024 * 1],
        "wall_time": [5, 10],
        "cpu_time": [1.2, 3.4],
        "node_id": ["a1", "b2"],
    }
    block_meta_list = []
    for i in range(len(block_params["num_rows"])):
        block_exec_stats = BlockExecStats()
        block_exec_stats.wall_time_s = block_params["wall_time"][i]
        block_exec_stats.cpu_time_s = block_params["cpu_time"][i]
        block_exec_stats.node_id = block_params["node_id"][i]
        block_exec_stats.max_rss_bytes = block_params["max_rss_bytes"][i]
        block_meta_list.append(
            BlockMetadata(
                num_rows=block_params["num_rows"][i],
                size_bytes=block_params["size_bytes"][i],
                schema=None,
                input_files=None,
                exec_stats=block_exec_stats,
            )
        )
    return block_params, block_meta_list
