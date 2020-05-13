import pytest

from ray.autoscaler.aws.config import _RESOURCE_CACHE
from ray.ray_constants import BOTO_MAX_RETRIES

import boto3
from botocore.stub import Stubber
from botocore.config import Config


@pytest.fixture()
def iam_client_stub():
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    resource = _RESOURCE_CACHE.setdefault(
        "iam", boto3.resource("iam", "us-east-1", config=boto_config))
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ec2_client_stub():
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    resource = _RESOURCE_CACHE.setdefault(
        "ec2", boto3.resource("ec2", "us-east-1", config=boto_config))
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()
