import boto3
from contextlib import contextmanager
import pytest
import ray
from moto import mock_s3
from mock_server import start_service, stop_process

import tempfile
from ray.tests.conftest import get_default_fixture_ray_kwargs
import os
import uuid
from ray.workflow.tests import utils


@contextmanager
def aws_credentials():
    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    yield (
        f"aws_access_key_id={os.environ['AWS_ACCESS_KEY_ID']}&"
        f"aws_secret_access_key={os.environ['AWS_SECRET_ACCESS_KEY']}&"
        f"aws_session_token={os.environ['AWS_SESSION_TOKEN']}"
    )
    os.environ = old_env


@contextmanager
def moto_s3_server():
    host = "localhost"
    port = 5002
    url = "http://{host}:{port}".format(host=host, port=port)
    process = start_service("s3", host, port)
    yield url
    stop_process(process)


@contextmanager
def filesystem_storage():
    with tempfile.TemporaryDirectory() as d:
        yield d


@contextmanager
def s3_storage():
    with moto_s3_server() as s3_server, aws_credentials() as aws_cred, mock_s3():
        client = boto3.client("s3", region_name="us-west-2", endpoint_url=s3_server)
        bucket = str(uuid.uuid1())
        client.create_bucket(Bucket=bucket)
        url = (
            f"s3://{bucket}/workflow"
            f"?region_name=us-west-2&endpoint_url={s3_server}"
            f"&{aws_cred}"
        )
        yield url


@contextmanager
def storage(storage_type):
    if storage_type == "s3":
        with s3_storage() as url:
            yield url
    else:
        with filesystem_storage() as url:
            yield url


@contextmanager
def _workflow_start(storage_url, shared, **kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    if ray.is_initialized():
        ray.shutdown()
        ray.workflow.storage.set_global_storage(None)
    # Sometimes pytest does not cleanup all global variables.
    # we have to manually reset the workflow storage. This
    # should not be an issue for normal use cases, because global variables
    # are freed after the driver exits.
    address_info = ray.init(**init_kwargs)
    utils.clear_marks()
    ray.workflow.init(storage_url)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    ray.workflow.storage.set_global_storage(None)


@pytest.fixture(scope="function")
def workflow_start_regular(storage_type, request):
    param = getattr(request, "param", {})
    with storage(storage_type) as storage_url, _workflow_start(
        storage_url, False, **param
    ) as res:
        yield res


@pytest.fixture
def reset_workflow():
    ray.workflow.storage.set_global_storage(None)
    yield
    ray.workflow.storage.set_global_storage(None)


@pytest.fixture(scope="module")
def workflow_start_regular_shared(storage_type, request):
    param = getattr(request, "param", {})
    with storage(storage_type) as storage_url, _workflow_start(
        storage_url, True, **param
    ) as res:
        yield res


def pytest_generate_tests(metafunc):
    if "storage_type" in metafunc.fixturenames:
        metafunc.parametrize("storage_type", ["s3", "fs"], scope="session")
