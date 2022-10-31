import os
import posixpath

import pytest
import pyarrow as pa
import pandas as pd

import ray

from ray.data.block import BlockAccessor
from ray.data.tests.mock_server import *  # noqa
from ray.data.datasource.file_based_datasource import BlockWritePathProvider

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import pytest_runtest_makereport  # noqa


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
    import urllib.parse

    fs = pa.fs.S3FileSystem(
        region="us-west-2", endpoint_override=s3_server, **aws_credentials
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
            dataset_uuid=None,
            block=None,
            block_index=None,
            file_format=None,
        ):
            num_rows = BlockAccessor.for_block(ray.get(block)).num_rows()
            suffix = (
                f"{block_index:06}_{num_rows:02}_{dataset_uuid}" f".test.{file_format}"
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
    ):
        import urllib.parse

        df_partitions = [df for _, df in df.groupby(partition_keys, as_index=False)]
        for df_partition in df_partitions:
            partition_values = []
            for key in partition_keys:
                partition_values.append(str(df_partition[key].iloc[0]))
            path = partition_path_encoder(partition_values)
            partition_path_encoder.scheme.resolved_filesystem.create_dir(path)
            base_dir = partition_path_encoder.scheme.base_dir
            parsed_base_dir = urllib.parse.urlparse(base_dir)
            if parsed_base_dir.scheme:
                # replace the protocol removed by the partition path generator
                path = posixpath.join(f"{parsed_base_dir.scheme}://{path}", "test.tmp")
            else:
                path = os.path.join(path, "test.tmp")
            file_writer_fn(df_partition, path)

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
        if num_computed is not None:
            assert (
                ds._plan.execute()._num_computed() == 1
            ), f"{ds._plan.execute()._num_computed()} != 1"
        assert ds.count() == count, f"{ds.count()} != {count}"
        assert ds.size_bytes() > 0, f"{ds.size_bytes()} <= 0"
        assert ds.schema() is not None
        actual_input_files = ds.input_files()
        assert len(actual_input_files) == num_input_files, actual_input_files
        assert (
            str(ds) == f"Dataset(num_blocks={num_input_files}, num_rows={num_rows}, "
            f"schema={schema})"
        ), ds
        assert (
            repr(ds) == f"Dataset(num_blocks={num_input_files}, num_rows={num_rows}, "
            f"schema={schema})"
        ), ds
        if num_computed is not None:
            assert (
                ds._plan.execute()._num_computed() == num_computed
            ), f"{ds._plan.execute()._num_computed()} != {num_computed}"

        # Force a data read.
        values = ds_take_transform_fn(ds.take())
        if num_computed is not None:
            assert (
                ds._plan.execute()._num_computed() == num_computed
            ), f"{ds._plan.execute()._num_computed()} != {num_computed}"
        actual_sorted_values = sorted_values_transform_fn(sorted(values))
        assert (
            actual_sorted_values == sorted_values
        ), f"{actual_sorted_values} != {sorted_values}"

    yield _assert_base_partitioned_ds


@pytest.fixture(params=[True, False])
def use_push_based_shuffle(request):
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.use_push_based_shuffle
    ctx.use_push_based_shuffle = request.param
    yield request.param
    ctx.use_push_based_shuffle = original


@pytest.fixture(params=[True, False])
def enable_automatic_tensor_extension_cast(request):
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.enable_tensor_extension_casting
    ctx.enable_tensor_extension_casting = request.param
    yield request.param
    ctx.enable_tensor_extension_casting = original


@pytest.fixture(params=["5.0.0", "7.0.0"])
def unsupported_pyarrow_version(request):
    orig_version = pa.__version__
    pa.__version__ = request.param
    yield request.param
    pa.__version__ = orig_version


@pytest.fixture
def disable_pyarrow_version_check():
    os.environ["RAY_DISABLE_PYARROW_VERSION_CHECK"] = "1"
    yield
    del os.environ["RAY_DISABLE_PYARROW_VERSION_CHECK"]
