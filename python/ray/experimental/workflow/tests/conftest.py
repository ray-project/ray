import boto3
from contextlib import contextmanager
import pytest
from moto import mock_s3
from mock_server import *  # noqa
import ray
from ray.experimental import workflow
from ray.experimental.workflow import storage
from ray.tests.conftest import get_default_fixture_ray_kwargs


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


@contextmanager
def _workflow_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs)
    workflow.init()
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def workflow_start_regular(request):
    param = getattr(request, "param", {})
    with _workflow_start(**param) as res:
        yield res


@pytest.fixture(scope="module")
def workflow_start_regular_shared(request):
    param = getattr(request, "param", {})
    with _workflow_start(**param) as res:
        yield res
