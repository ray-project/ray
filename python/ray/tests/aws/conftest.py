import pytest

from ray.autoscaler._private.aws.config import _resource_cache

from botocore.stub import Stubber


@pytest.fixture()
def iam_client_stub():
    resource = _resource_cache("iam", "us-west-2")
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ec2_client_stub():
    resource = _resource_cache("ec2", "us-west-2")
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()
