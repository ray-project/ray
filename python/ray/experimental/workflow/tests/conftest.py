import boto3
from contextlib import contextmanager
import pytest
from moto import mock_s3
from mock_server import *  # noqa
from pytest_lazyfixture import lazy_fixture
import tempfile
import os

import ray
from ray.experimental import workflow
from ray.experimental.workflow import storage
from ray.tests.conftest import get_default_fixture_ray_kwargs


@pytest.fixture(scope="session")
def filesystem_storage():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture(scope="session")
def aws_credentials():
    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    yield (f"aws_access_key_id={os.environ['AWS_ACCESS_KEY_ID']}&"
           f"aws_secret_access_key={os.environ['AWS_SECRET_ACCESS_KEY']}&"
           f"aws_session_token={os.environ['AWS_SESSION_TOKEN']}")
    os.environ = old_env


@pytest.fixture(scope="session")
def s3_storage(aws_credentials, s3_server):
    with mock_s3():
        client = boto3.client(
            "s3", region_name="us-west-2", endpoint_url=s3_server)
        client.create_bucket(Bucket="test_bucket")
        url = ("s3://test_bucket/workflow"
               f"?region_name=us-west-2&endpoint_url={s3_server}"
               f"&{aws_credentials}")
        yield url


@contextmanager
def _workflow_start(storage_url, shared, **kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    if ray.is_initialized():
        ray.shutdown()
        storage.set_global_storage(None)
    # Sometimes pytest does not cleanup all global variables.
    # we have to manually reset the workflow storage. This
    # should not be an issue for normal use cases, because global variables
    # are freed after the driver exits.
    address_info = ray.init(**init_kwargs)
    workflow.init(storage_url)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    storage.set_global_storage(None)


@pytest.fixture(scope="function")
def workflow_start_regular(storage_url, request):
    param = getattr(request, "param", {})
    with _workflow_start(storage_url, False, **param) as res:
        yield res


@pytest.fixture
def reset_workflow():
    storage.set_global_storage(None)
    yield
    storage.set_global_storage(None)


@pytest.fixture(scope="session")
def workflow_start_regular_shared(storage_url, request):
    param = getattr(request, "param", {})
    with _workflow_start(storage_url, True, **param) as res:
        yield res


def pytest_generate_tests(metafunc):
    if "storage_url" in metafunc.fixturenames:
        metafunc.parametrize(
            "storage_url",
            [lazy_fixture("s3_storage"),
             lazy_fixture("filesystem_storage")],
            scope="session")
