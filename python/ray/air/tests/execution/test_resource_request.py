import pytest

from ray.air.execution.resources.request import ResourceRequest


def test_request_same():
    """Test that resource requests are the same if they share the same properties."""

    assert ResourceRequest([{"CPU": 1}]) == ResourceRequest([{"CPU": 1}])

    # multiple bundles work
    assert ResourceRequest([{"CPU": 1}, {"CPU": 2}]) == ResourceRequest(
        [{"CPU": 1}, {"CPU": 2}]
    )

    # multiple resources work
    assert ResourceRequest([{"CPU": 1, "GPU": 1}]) == ResourceRequest(
        [{"CPU": 1, "GPU": 1}]
    )

    # 0 resources are ignored
    assert ResourceRequest([{"CPU": 0, "GPU": 1}]) == ResourceRequest([{"GPU": 1}])

    # PACK is implicit
    assert ResourceRequest([{"CPU": 1}], strategy="PACK") == ResourceRequest(
        [{"CPU": 1}]
    )

    # Non match: different strategy
    assert ResourceRequest([{"CPU": 1}], strategy="PACK") != ResourceRequest(
        [{"CPU": 1}], strategy="SPREAD"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
