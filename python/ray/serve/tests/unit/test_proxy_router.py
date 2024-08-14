from typing import Callable

import pytest

from ray.serve._private.common import DeploymentID, EndpointInfo, RequestProtocol
from ray.serve._private.proxy_router import (
    NO_REPLICAS_MESSAGE,
    NO_ROUTES_MESSAGE,
    EndpointRouter,
    LongestPrefixRouter,
    ProxyRouter,
)
from ray.serve._private.test_utils import MockDeploymentHandle


def get_handle_function(router: ProxyRouter) -> Callable:
    if isinstance(router, LongestPrefixRouter):
        return router.match_route
    else:
        return router.get_handle_for_endpoint


@pytest.fixture
def mock_longest_prefix_router() -> LongestPrefixRouter:
    def mock_get_handle(deployment_name, app_name, *args, **kwargs):
        return MockDeploymentHandle(deployment_name, app_name)

    yield LongestPrefixRouter(mock_get_handle, RequestProtocol.HTTP)


@pytest.fixture
def mock_endpoint_router() -> EndpointRouter:
    def mock_get_handle(deployment_name, app_name, *args, **kwargs):
        return MockDeploymentHandle(deployment_name, app_name)

    yield EndpointRouter(mock_get_handle, RequestProtocol.GRPC)


@pytest.mark.parametrize(
    "mocked_router", ["mock_longest_prefix_router", "mock_endpoint_router"]
)
def test_no_match(mocked_router, request):
    router = request.getfixturevalue(mocked_router)
    router.update_routes(
        {
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/hello"
            ),
            DeploymentID(name="endpoint2", app_name="app2"): EndpointInfo(
                route="/hello2"
            ),
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
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/endpoint"
            ),
            DeploymentID(name="endpoint2", app_name="app2"): EndpointInfo(
                route="/endpoint2"
            ),
        }
    )

    assert get_handle_function(router)("/nonexistent") is None

    route, handle, app_is_cross_language = get_handle_function(router)(target_route)
    assert all(
        [
            route == "/endpoint",
            handle == ("endpoint", "default"),
            not app_is_cross_language,
        ]
    )


def test_trailing_slash(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {DeploymentID(name="endpoint", app_name="default"): EndpointInfo(route="/test")}
    )

    route, handle, _ = get_handle_function(router)("/test/")
    assert route == "/test" and handle == ("endpoint", "default")

    router.update_routes(
        {
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/test/"
            )
        }
    )

    assert get_handle_function(router)("/test") is None


def test_prefix_match(mock_longest_prefix_router):
    router = mock_longest_prefix_router
    router.update_routes(
        {
            DeploymentID(name="endpoint1", app_name="default"): EndpointInfo(
                route="/test/test2"
            ),
            DeploymentID(name="endpoint2", app_name="default"): EndpointInfo(
                route="/test"
            ),
            DeploymentID(name="endpoint3", app_name="default"): EndpointInfo(route="/"),
        }
    )

    route, handle, _ = get_handle_function(router)("/test/test2/subpath")
    assert route == "/test/test2" and handle == ("endpoint1", "default")
    route, handle, _ = get_handle_function(router)("/test/test2/")
    assert route == "/test/test2" and handle == ("endpoint1", "default")
    route, handle, _ = get_handle_function(router)("/test/test2")
    assert route == "/test/test2" and handle == ("endpoint1", "default")

    route, handle, _ = get_handle_function(router)("/test/subpath")
    assert route == "/test" and handle == ("endpoint2", "default")
    route, handle, _ = get_handle_function(router)("/test/")
    assert route == "/test" and handle == ("endpoint2", "default")
    route, handle, _ = get_handle_function(router)("/test")
    assert route == "/test" and handle == ("endpoint2", "default")

    route, handle, _ = get_handle_function(router)("/test2")
    assert route == "/" and handle == ("endpoint3", "default")
    route, handle, _ = get_handle_function(router)("/")
    assert route == "/" and handle == ("endpoint3", "default")


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
        {
            DeploymentID(name="endpoint", app_name="app1"): EndpointInfo(
                route="/endpoint"
            )
        }
    )

    route, handle, app_is_cross_language = get_handle_function(router)(target_route1)
    assert all(
        [
            route == "/endpoint",
            handle == ("endpoint", "app1"),
            not app_is_cross_language,
        ]
    )

    router.update_routes(
        {
            DeploymentID(name="endpoint2", app_name="app2"): EndpointInfo(
                route="/endpoint2",
                app_is_cross_language=True,
            ),
            DeploymentID(name="endpoint3", app_name="app3"): EndpointInfo(
                route="/endpoint3",
                app_is_cross_language=True,
            ),
        }
    )

    assert get_handle_function(router)(target_route1) is None

    route, handle, app_is_cross_language = get_handle_function(router)(target_route2)
    assert all(
        [route == "/endpoint2", handle == ("endpoint2", "app2"), app_is_cross_language]
    )


class TestReadyForTraffic:
    @pytest.mark.parametrize("is_head", [False, True])
    def test_route_table_not_populated(
        self, mock_endpoint_router: EndpointRouter, is_head: bool
    ):
        """Proxy router should NOT be ready for traffic if:
        - it has not received route table from controller
        """

        ready_for_traffic, msg = mock_endpoint_router.ready_for_traffic(is_head=is_head)
        assert not ready_for_traffic
        assert msg == NO_ROUTES_MESSAGE

    def test_head_route_table_populated_no_replicas(
        self, mock_endpoint_router: EndpointRouter
    ):
        """Proxy router should be ready for traffic if:
        - it has received route table from controller
        - it hasn't received any replicas yet
        - it lives on head node
        """

        d_id = DeploymentID(name="A", app_name="B")
        mock_endpoint_router.update_routes({d_id: EndpointInfo(route="/")})
        mock_endpoint_router.handles[d_id].set_running_replicas_populated(False)

        ready_for_traffic, msg = mock_endpoint_router.ready_for_traffic(is_head=True)
        assert ready_for_traffic
        assert not msg

    def test_worker_route_table_populated_no_replicas(
        self, mock_endpoint_router: EndpointRouter
    ):
        """Proxy router should NOT be ready for traffic if:
        - it has received route table from controller
        - it hasn't received any replicas yet
        - it lives on a worker node
        """

        d_id = DeploymentID(name="A", app_name="B")
        mock_endpoint_router.update_routes({d_id: EndpointInfo(route="/")})
        mock_endpoint_router.handles[d_id].set_running_replicas_populated(False)

        ready_for_traffic, msg = mock_endpoint_router.ready_for_traffic(is_head=False)
        assert not ready_for_traffic
        assert msg == NO_REPLICAS_MESSAGE

    @pytest.mark.parametrize("is_head", [False, True])
    def test_route_table_populated_with_replicas(
        self, mock_endpoint_router: EndpointRouter, is_head: bool
    ):
        """Proxy router should be ready for traffic if:
        - it has received route table from controller
        - it has received replicas from controller
        """

        d_id = DeploymentID(name="A", app_name="B")
        mock_endpoint_router.update_routes({d_id: EndpointInfo(route="/")})
        mock_endpoint_router.handles[d_id].set_running_replicas_populated(True)

        ready_for_traffic, msg = mock_endpoint_router.ready_for_traffic(is_head=is_head)
        assert ready_for_traffic
        assert not msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
