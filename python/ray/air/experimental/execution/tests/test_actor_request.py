import pytest
from ray.air.experimental.execution.actor_request import ActorRequest

from ray.air.experimental.execution.resources.request import ResourceRequest


def test_request_not_same():
    """Test that actor requests are not the same if they share the same properties."""
    resource_request = ResourceRequest([{"CPU": 1}])

    assert ActorRequest(cls=None, kwargs={}, resources=None) != ActorRequest(
        cls=None, kwargs={}, resources=None
    )

    assert ActorRequest(
        cls=None, kwargs={}, resources=resource_request
    ) != ActorRequest(cls=None, kwargs={}, resources=resource_request)

    assert ActorRequest(
        cls=Exception, kwargs={}, resources=resource_request
    ) != ActorRequest(cls=Exception, kwargs={}, resources=resource_request)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
