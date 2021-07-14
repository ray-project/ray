import boto3
import pytest
from moto import mock_s3
from mock_server import *  # noqa
from ray.experimental.workflow import storage


@pytest.fixture(scope="function")
def filesystem_storage():
    # TODO: use tmp path once fixed the path issues
    storage.set_global_storage(
        storage.create_storage("/tmp/ray/workflow_data/"))
    yield storage.get_global_storage()


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
def s3_storage(aws_credentials, s3_server):
    with mock_s3():
        client = boto3.client(
            "s3", region_name="us-west-2", endpoint_url=s3_server)
        client.create_bucket(Bucket="test_bucket")
        url = ("s3://test_bucket/workflow"
               f"?region_name=us-west-2&endpoint_url={s3_server}")
        storage.set_global_storage(storage.create_storage(url))
        yield storage.get_global_storage()
