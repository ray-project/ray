from unittest.mock import patch

import pytest

from ray.serve.common import EndpointInfo
from ray.serve.http_proxy import LongestPrefixRouter


@pytest.fixture
def mock_longest_prefix_router() -> LongestPrefixRouter:
    with patch("ray.serve.get_handle") as mock_get_handle:

        def side_effect(name, *args, **kwargs):
            return name

        mock_get_handle.side_effect = side_effect
        yield LongestPrefixRouter()


def test_no_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo({"POST"}, route="/hello")})
    route, handle = router.match_route("/nonexistent", "POST")
    assert route is None and handle is None


def test_default_route(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo({"POST"})})

    route, handle = router.match_route("/nonexistent", "POST")
    assert route is None and handle is None

    route, handle = router.match_route("/endpoint", "POST")
    assert route == "/endpoint" and handle == "endpoint"


def test_trailing_slash(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({
        "endpoint": EndpointInfo({"POST"}, route="/test"),
    })

    route, handle = router.match_route("/test/", "POST")
    assert route == "/test" and handle == "endpoint"

    router.update_routes({
        "endpoint": EndpointInfo({"POST"}, route="/test/"),
    })

    route, handle = router.match_route("/test", "POST")
    assert route is None and handle is None


def test_prefix_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({
        "endpoint1": EndpointInfo({"POST"}, route="/test/test2"),
        "endpoint2": EndpointInfo({"POST"}, route="/test"),
        "endpoint3": EndpointInfo({"POST"}, route="/"),
    })

    route, handle = router.match_route("/test/test2/subpath", "POST")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle = router.match_route("/test/test2/", "POST")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle = router.match_route("/test/test2", "POST")
    assert route == "/test/test2" and handle == "endpoint1"

    route, handle = router.match_route("/test/subpath", "POST")
    assert route == "/test" and handle == "endpoint2"
    route, handle = router.match_route("/test/", "POST")
    assert route == "/test" and handle == "endpoint2"
    route, handle = router.match_route("/test", "POST")
    assert route == "/test" and handle == "endpoint2"

    route, handle = router.match_route("/test2", "POST")
    assert route == "/" and handle == "endpoint3"
    route, handle = router.match_route("/", "POST")
    assert route == "/" and handle == "endpoint3"


def test_update_routes(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo({"POST"})})

    route, handle = router.match_route("/endpoint", "POST")
    assert route == "/endpoint" and handle == "endpoint"

    router.update_routes({"endpoint2": EndpointInfo({"POST"})})

    route, handle = router.match_route("/endpoint", "POST")
    assert route is None and handle is None

    route, handle = router.match_route("/endpoint2", "POST")
    assert route == "/endpoint2" and handle == "endpoint2"


def test_match_method(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({
        "endpoint": EndpointInfo({"POST", "GET"}, route="/test"),
        "endpoint2": EndpointInfo({"PATCH"}, route="/")
    })

    route, handle = router.match_route("/test", "POST")
    assert route == "/test" and handle == "endpoint"

    route, handle = router.match_route("/test", "GET")
    assert route == "/test" and handle == "endpoint"

    route, handle = router.match_route("/test", "PATCH")
    assert route == "/" and handle == "endpoint2"

    route, handle = router.match_route("/test", "OPTIONS")
    assert route is None and handle is None


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
