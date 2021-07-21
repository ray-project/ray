import boto3
import pytest
from moto import mock_s3
from mock_server import *  # noqa
from ray.experimental.workflow import storage
from pytest_lazyfixture import lazy_fixture


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
    yield (f"aws_access_key_id={os.environ['AWS_ACCESS_KEY_ID']}&"
           f"aws_secret_access_key={os.environ['AWS_SECRET_ACCESS_KEY']}&"
           f"aws_session_token={os.environ['AWS_SESSION_TOKEN']}")
    os.environ = old_env


@pytest.fixture(scope="function")
def s3_storage(aws_credentials, s3_server):
    with mock_s3():
        import os
        client = boto3.client(
            "s3", region_name="us-west-2", endpoint_url=s3_server)
        client.create_bucket(Bucket="test_bucket")
        url = ("s3://test_bucket/workflow"
               f"?region_name=us-west-2&endpoint_url={s3_server}&{aws_credentials}")
        storage.set_global_storage(storage.create_storage(url))
        yield storage.get_global_storage()


@contextmanager
def _workflow_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs)
    # workflow.init()
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def workflow_start_regular(raw_storage, request):
    param = getattr(request, "param", {})
    with _workflow_start(**param) as res:
        yield res


@pytest.fixture(scope="module")
def workflow_start_regular_shared(raw_storage, request):
    param = getattr(request, "param", {})
    with _workflow_start(**param) as res:
        yield res

def pytest_generate_tests(metafunc):
    if "raw_storage" in metafunc.fixturenames:
        metafunc.parametrize("raw_storage", [lazy_fixture("s3_storage"), lazy_fixture("filesystem_storage")])
