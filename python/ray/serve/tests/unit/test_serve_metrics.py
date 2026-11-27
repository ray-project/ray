import sys
from unittest.mock import MagicMock, call

import pytest

from ray.serve import context as serve_context
from ray.serve.metrics import Histogram


def test_observe_many_fills_in_the_route_tag():
    """`_observe_many` must fill in the route tag exactly like `observe`."""
    histogram = Histogram(
        "test_observe_many_route",
        description="test",
        boundaries=[1.0, 3.0],
        tag_keys=("route",),
    )
    metric = MagicMock()
    histogram._metric = metric

    token = serve_context._serve_request_context.set(
        serve_context._RequestContext(route="/route")
    )
    try:
        histogram._observe_many([1.0, 2.0])
    finally:
        serve_context._serve_request_context.reset(token)

    assert metric.record.call_args_list == [
        call(1.0, tags={"route": "/route"}),
        call(2.0, tags={"route": "/route"}),
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
