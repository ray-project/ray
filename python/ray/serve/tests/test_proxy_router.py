import pytest

from ray.serve._private.common import EndpointInfo, EndpointTag
from ray.serve._private.proxy_router import EndpointRouter, LongestPrefixRouter


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


@pytest.fixture
def mock_endpoint_router() -> EndpointRouter:
    class MockHandle:
        def __init__(self, name: str):
            self._name = name

        def options(self, *args, **kwargs):
            return self

        def __eq__(self, other_name: str):
            return self._name == other_name

    def mock_get_handle(name, *args, **kwargs):
        return MockHandle(name)

    yield EndpointRouter(mock_get_handle)


@pytest.mark.parametrize(
    "mocked_router", ["mock_longest_prefix_router", "mock_endpoint_router"]
)
def test_no_match(mocked_router, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {EndpointTag("endpoint", "default"): EndpointInfo(route="/hello")}
    )
    assert router.match_route("/nonexistent") is None


@pytest.mark.parametrize(
    "mocked_router, target_route",
    [
        ("mock_longest_prefix_router", "/endpoint"),
        ("mock_endpoint_router", "default_endpoint"),
    ],
)
def test_default_route(mocked_router, target_route, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {EndpointTag("endpoint", "default"): EndpointInfo(route="/endpoint")}
    )

    assert router.match_route("/nonexistent") is None

    route, handle, app_name, app_is_cross_language = router.match_route(target_route)
    assert all(
        [
            route == "/endpoint",
            handle == "endpoint",
            app_name == "default",
            not app_is_cross_language,
        ]
    )


def test_trailing_slash(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            EndpointTag("endpoint", "default"): EndpointInfo(route="/test"),
        }
    )

    route, handle, _, _ = router.match_route("/test/")
    assert route == "/test" and handle == "endpoint"

    router.update_routes(
        {
            EndpointTag("endpoint", "default"): EndpointInfo(route="/test/"),
        }
    )

    assert router.match_route("/test") is None


def test_prefix_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            EndpointTag("endpoint1", "default"): EndpointInfo(route="/test/test2"),
            EndpointTag("endpoint2", "default"): EndpointInfo(route="/test"),
            EndpointTag("endpoint3", "default"): EndpointInfo(route="/"),
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


@pytest.mark.parametrize(
    "mocked_router, target_route1, target_route2",
    [
        ("mock_longest_prefix_router", "/endpoint", "/endpoint2"),
        ("mock_endpoint_router", "app1_endpoint", "app2_endpoint2"),
    ],
)
def test_update_routes(mocked_router, target_route1, target_route2, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {EndpointTag("endpoint", "app1"): EndpointInfo(route="/endpoint")}
    )

    route, handle, app_name, app_is_cross_language = router.match_route(target_route1)
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
            EndpointTag("endpoint2", "app2"): EndpointInfo(
                route="/endpoint2",
                app_is_cross_language=True,
            )
        }
    )

    assert router.match_route(target_route1) is None

    route, handle, app_name, app_is_cross_language = router.match_route(target_route2)
    assert route == "/endpoint2" and handle == "endpoint2" and app_name == "app2"
    assert all(
        [
            route == "/endpoint2",
            handle == "endpoint2",
            app_name == "app2",
            app_is_cross_language,
        ]
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
