import pytest
from typing import Callable

from ray.serve._private.common import EndpointInfo, EndpointTag, RequestProtocol
from ray.serve._private.proxy_router import (
    ProxyRouter,
    EndpointRouter,
    LongestPrefixRouter,
)


def get_handle_function(router: ProxyRouter) -> Callable:
    if isinstance(router, LongestPrefixRouter):
        return router.match_route
    else:
        return router.get_handle_for_endpoint


@pytest.fixture
def mock_longest_prefix_router() -> LongestPrefixRouter:
    class MockHandle:
        def __init__(self, name: str):
            self._name = name
            self._protocol = RequestProtocol.UNDEFINED

        def options(self, *args, **kwargs):
            return self

        def __eq__(self, other_name: str):
            return self._name == other_name

        def _set_request_protocol(self, protocol: RequestProtocol):
            self._protocol = protocol

    def mock_get_handle(name, *args, **kwargs):
        return MockHandle(name)

    yield LongestPrefixRouter(mock_get_handle, RequestProtocol.HTTP)


@pytest.fixture
def mock_endpoint_router() -> EndpointRouter:
    class MockHandle:
        def __init__(self, name: str):
            self._name = name
            self._protocol = RequestProtocol.UNDEFINED

        def options(self, *args, **kwargs):
            return self

        def __eq__(self, other_name: str):
            return self._name == other_name

        def _set_request_protocol(self, protocol: RequestProtocol):
            self._protocol = protocol

    def mock_get_handle(name, *args, **kwargs):
        return MockHandle(name)

    yield EndpointRouter(mock_get_handle, RequestProtocol.GRPC)


@pytest.mark.parametrize(
    "mocked_router", ["mock_longest_prefix_router", "mock_endpoint_router"]
)
def test_no_match(mocked_router, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {
            EndpointTag("endpoint", "default"): EndpointInfo(route="/hello"),
            EndpointTag("endpoint2", "default2"): EndpointInfo(route="/hello2"),
        }
    )
    assert get_handle_function(router)("/nonexistent") is None


@pytest.mark.parametrize(
    "mocked_router, target_route",
    [
        ("mock_longest_prefix_router", "/endpoint"),
        ("mock_endpoint_router", "default"),
    ],
)
def test_default_route(mocked_router, target_route, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {
            EndpointTag("endpoint", "default"): EndpointInfo(route="/endpoint"),
            EndpointTag("endpoint2", "default2"): EndpointInfo(route="/endpoint2"),
        }
    )

    assert get_handle_function(router)("/nonexistent") is None

    route, handle, app_name, app_is_cross_language = get_handle_function(router)(
        target_route
    )
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

    route, handle, _, _ = get_handle_function(router)("/test/")
    assert route == "/test" and handle == "endpoint"

    router.update_routes(
        {
            EndpointTag("endpoint", "default"): EndpointInfo(route="/test/"),
        }
    )

    assert get_handle_function(router)("/test") is None


def test_prefix_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            EndpointTag("endpoint1", "default"): EndpointInfo(route="/test/test2"),
            EndpointTag("endpoint2", "default"): EndpointInfo(route="/test"),
            EndpointTag("endpoint3", "default"): EndpointInfo(route="/"),
        }
    )

    route, handle, _, _ = get_handle_function(router)("/test/test2/subpath")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle, _, _ = get_handle_function(router)("/test/test2/")
    assert route == "/test/test2" and handle == "endpoint1"
    route, handle, _, _ = get_handle_function(router)("/test/test2")
    assert route == "/test/test2" and handle == "endpoint1"

    route, handle, _, _ = get_handle_function(router)("/test/subpath")
    assert route == "/test" and handle == "endpoint2"
    route, handle, _, _ = get_handle_function(router)("/test/")
    assert route == "/test" and handle == "endpoint2"
    route, handle, _, _ = get_handle_function(router)("/test")
    assert route == "/test" and handle == "endpoint2"

    route, handle, _, _ = get_handle_function(router)("/test2")
    assert route == "/" and handle == "endpoint3"
    route, handle, _, _ = get_handle_function(router)("/")
    assert route == "/" and handle == "endpoint3"


@pytest.mark.parametrize(
    "mocked_router, target_route1, target_route2",
    [
        ("mock_longest_prefix_router", "/endpoint", "/endpoint2"),
        ("mock_endpoint_router", "app1_endpoint", "app2"),
    ],
)
def test_update_routes(mocked_router, target_route1, target_route2, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {EndpointTag("endpoint", "app1"): EndpointInfo(route="/endpoint")}
    )

    route, handle, app_name, app_is_cross_language = get_handle_function(router)(
        target_route1
    )
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
            ),
            EndpointTag("endpoint3", "app3"): EndpointInfo(
                route="/endpoint3",
                app_is_cross_language=True,
            ),
        }
    )

    assert get_handle_function(router)(target_route1) is None

    route, handle, app_name, app_is_cross_language = get_handle_function(router)(
        target_route2
    )
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
