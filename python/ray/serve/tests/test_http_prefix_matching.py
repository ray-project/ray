import pytest

from ray.serve._private.common import EndpointInfo
from ray.serve._private.http_proxy import LongestPrefixRouter


@pytest.fixture
def mock_longest_prefix_router() -> LongestPrefixRouter:
    def mock_get_handle(name, *args, **kwargs):
        return name

    yield LongestPrefixRouter(mock_get_handle)


def test_no_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/hello", app_name="")})
    route, handle, app_name = router.match_route("/nonexistent")
    assert route is None and handle is None and app_name is None


def test_default_route(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/endpoint", app_name="")})

    route, handle, app_name = router.match_route("/nonexistent")
    assert route is None and handle is None and app_name is None

    route, handle, app_name = router.match_route("/endpoint")
    assert route == "/endpoint" and handle == "endpoint" and app_name == ""


def test_trailing_slash(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            "endpoint": EndpointInfo(route="/test", app_name=""),
        }
    )

    route, handle, _ = router.match_route("/test/")
    assert route == "/test" and handle == "endpoint"

    router.update_routes(
        {
            "endpoint": EndpointInfo(route="/test/", app_name=""),
        }
    )

    route, handle, app_name = router.match_route("/test")
    assert route is None and handle is None and app_name is None


def test_prefix_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            "endpoint1": EndpointInfo(route="/test/test2", app_name=""),
            "endpoint2": EndpointInfo(route="/test", app_name=""),
            "endpoint3": EndpointInfo(route="/", app_name=""),
        }
    )

    route, handle, _ = router.match_route("/test/test2/subpath")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle, _ = router.match_route("/test/test2/")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle, _ = router.match_route("/test/test2")
    assert route == "/test/test2" and handle == "endpoint1"

    route, handle, _ = router.match_route("/test/subpath")
    assert route == "/test" and handle == "endpoint2"
    route, handle, _ = router.match_route("/test/")
    assert route == "/test" and handle == "endpoint2"
    route, handle, _ = router.match_route("/test")
    assert route == "/test" and handle == "endpoint2"

    route, handle, _ = router.match_route("/test2")
    assert route == "/" and handle == "endpoint3"
    route, handle, _ = router.match_route("/")
    assert route == "/" and handle == "endpoint3"


def test_update_routes(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/endpoint", app_name="app1")})

    route, handle, app_name = router.match_route("/endpoint")
    assert route == "/endpoint" and handle == "endpoint" and app_name == "app1"

    router.update_routes(
        {"endpoint2": EndpointInfo(route="/endpoint2", app_name="app2")}
    )

    route, handle, app_name = router.match_route("/endpoint")
    assert route is None and handle is None and app_name is None

    route, handle, app_name = router.match_route("/endpoint2")
    assert route == "/endpoint2" and handle == "endpoint2" and app_name == "app2"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
