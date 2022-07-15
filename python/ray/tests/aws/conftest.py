import pytest

from ray.autoscaler._private.constants import BOTO_MAX_RETRIES
from ray.autoscaler._private.aws.utils import resource_cache

from botocore.stub import Stubber


@pytest.fixture()
def iam_client_stub(request):
    region = getattr(request, "param", "us-west-2")
    resource = resource_cache("iam", region)
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ec2_client_stub(request):
    region = getattr(request, "param", "us-west-2")
    resource = resource_cache("ec2", region)
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ec2_client_stub_fail_fast():
    resource = resource_cache("ec2", "us-west-2", 0)
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ec2_client_stub_max_retries():
    resource = resource_cache("ec2", "us-west-2", BOTO_MAX_RETRIES)
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()
