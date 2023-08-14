import pytest

from ray.serve._private.common import EndpointInfo
from ray.serve._private.http_proxy import LongestPrefixRouter


@pytest.fixture
def mock_longest_prefix_router() -> LongestPrefixRouter:
    class MockHandle:
        def __init__(self, name: str):
            self._name = name

        def options(self, *args, **kwargs):
            return self

        def __eq__(self, other_name: str):
            return self._name == other_name

    def mock_get_handle(name, *args, **kwargs):
        return MockHandle(name)

    yield LongestPrefixRouter(mock_get_handle)


def test_no_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/hello", app_name="")})
    assert router.match_route("/nonexistent") is None


def test_default_route(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/endpoint", app_name="")})

    assert router.match_route("/nonexistent") is None

    route, handle, app_name, app_is_cross_language = router.match_route("/endpoint")
    assert all(
        [
            route == "/endpoint",
            handle == "endpoint",
            app_name == "",
            not app_is_cross_language,
        ]
    )


def test_trailing_slash(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            "endpoint": EndpointInfo(route="/test", app_name=""),
        }
    )

    route, handle, _, _ = router.match_route("/test/")
    assert route == "/test" and handle == "endpoint"

    router.update_routes(
        {
            "endpoint": EndpointInfo(route="/test/", app_name=""),
        }
    )

    assert router.match_route("/test") is None


def test_prefix_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            "endpoint1": EndpointInfo(route="/test/test2", app_name=""),
            "endpoint2": EndpointInfo(route="/test", app_name=""),
            "endpoint3": EndpointInfo(route="/", app_name=""),
        }
    )

    route, handle, _, _ = router.match_route("/test/test2/subpath")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle, _, _ = router.match_route("/test/test2/")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle, _, _ = router.match_route("/test/test2")
    assert route == "/test/test2" and handle == "endpoint1"

    route, handle, _, _ = router.match_route("/test/subpath")
    assert route == "/test" and handle == "endpoint2"
    route, handle, _, _ = router.match_route("/test/")
    assert route == "/test" and handle == "endpoint2"
    route, handle, _, _ = router.match_route("/test")
    assert route == "/test" and handle == "endpoint2"

    route, handle, _, _ = router.match_route("/test2")
    assert route == "/" and handle == "endpoint3"
    route, handle, _, _ = router.match_route("/")
    assert route == "/" and handle == "endpoint3"


def test_update_routes(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/endpoint", app_name="app1")})

    route, handle, app_name, app_is_cross_language = router.match_route("/endpoint")
    assert all(
        [
            route == "/endpoint",
            handle == "endpoint",
            app_name == "app1",
            not app_is_cross_language,
        ]
    )

    router.update_routes(
        {
            "endpoint2": EndpointInfo(
                route="/endpoint2",
                app_name="app2",
                app_is_cross_language=True,
            )
        }
    )

    assert router.match_route("/endpoint") is None

    route, handle, app_name, app_is_cross_language = router.match_route("/endpoint2")
    assert route == "/endpoint2" and handle == "endpoint2" and app_name == "app2"
    assert all(
        [
            route == "/endpoint2",
            handle == "endpoint2",
            app_name == "app2",
            app_is_cross_language,
        ]
    )


def test_match_target(mock_longest_prefix_router):
    """Test match_target method
    When the target is None, `match_target()` returns None. When the target matched
    with an app, `match_target()` returns the correct route. When the target does
    not match with any app, `match_target()` returns the first route.
    """
    router = mock_longest_prefix_router
    router.update_routes({"endpoint": EndpointInfo(route="/endpoint", app_name="app1")})

    assert router.match_target(target=None) is None
    assert router.match_target(target="app1") == "/endpoint"
    assert router.match_target(target="app2") == "/endpoint"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
