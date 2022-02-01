import os

import pytest
import pyarrow as pa

import ray

from ray.data.block import BlockAccessor
from ray.data.tests.mock_server import *  # noqa
from ray.data.datasource.file_based_datasource import BlockWritePathProvider


@pytest.fixture(scope="function")
def aws_credentials():
    import os

    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    yield
    os.environ = old_env


@pytest.fixture(scope="function")
def data_dir():
    yield "test_data"


@pytest.fixture(scope="function")
def data_dir_with_space():
    yield "test data"


@pytest.fixture(scope="function")
def s3_path(tmp_path, data_dir):
    yield "s3://" + os.path.join(tmp_path, data_dir).strip("/")


@pytest.fixture(scope="function")
def s3_path_with_space(tmp_path, data_dir_with_space):
    yield "s3://" + os.path.join(tmp_path, data_dir_with_space).strip("/")


@pytest.fixture(scope="function")
def s3_fs(aws_credentials, s3_server, s3_path):
    yield from _s3_fs(aws_credentials, s3_server, s3_path)


@pytest.fixture(scope="function")
def s3_fs_with_space(aws_credentials, s3_server, s3_path_with_space):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_space)


def _s3_fs(aws_credentials, s3_server, s3_path):
    import urllib.parse

    fs = pa.fs.S3FileSystem(region="us-west-2", endpoint_override=s3_server)
    if s3_path.startswith("s3://"):
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
            print(f"Writing to: {base_path}/{suffix}")
            return f"{base_path}/{suffix}"

    yield TestBlockWritePathProvider()
