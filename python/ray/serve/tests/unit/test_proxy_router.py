import pytest

from ray.serve._private.common import DeploymentID, EndpointInfo
from ray.serve._private.proxy_router import (
    NO_REPLICAS_MESSAGE,
    NO_ROUTES_MESSAGE,
    ProxyRouter,
)
from ray.serve._private.test_utils import MockDeploymentHandle


@pytest.fixture
def mock_router():
    def mock_get_handle(endpoint, info):
        return MockDeploymentHandle(endpoint.name, endpoint.app_name)

    yield ProxyRouter(mock_get_handle)


def test_no_match(mock_router: ProxyRouter):
    router = mock_router
    router.update_routes(
        {
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/hello"
            ),
            DeploymentID(name="endpoint2", app_name="app2"): EndpointInfo(
                route="/hello2"
            ),
        },
    )
    assert router.match_route("/nonexistent") is None
    assert router.get_handle_for_endpoint("/nonexistent") is None


def test_default_route(mock_router: ProxyRouter):
    router = mock_router
    router.update_routes(
        {
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/endpoint"
            ),
            DeploymentID(name="endpoint2", app_name="app2"): EndpointInfo(
                route="/endpoint2"
            ),
        },
    )

    # Route based matching
    assert router.match_route("/nonexistent") is None

    route, handle, app_is_cross_language = router.match_route("/endpoint")
    assert route == "/endpoint"
    assert handle == ("endpoint", "default")
    assert not app_is_cross_language

    # Endpoint based matching
    assert router.get_handle_for_endpoint("/nonexistent") is None

    route, handle, app_is_cross_language = router.get_handle_for_endpoint("default")
    assert route == "/endpoint"
    assert handle == ("endpoint", "default")
    assert not app_is_cross_language


def test_trailing_slash(mock_router: ProxyRouter):
    router = mock_router
    router.update_routes(
        {
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/test"
            )
        },
    )

    route, handle, _ = router.match_route("/test/")
    assert route == "/test" and handle == ("endpoint", "default")

    router.update_routes(
        {
            DeploymentID(name="endpoint", app_name="default"): EndpointInfo(
                route="/test/"
            )
        },
    )

    assert router.match_route("/test") is None


def test_prefix_match(mock_router):
    router = mock_router
    router.update_routes(
        {
            DeploymentID(name="endpoint1", app_name="default"): EndpointInfo(
                route="/test/test2"
            ),
            DeploymentID(name="endpoint2", app_name="default"): EndpointInfo(
                route="/test"
            ),
            DeploymentID(name="endpoint3", app_name="default"): EndpointInfo(route="/"),
        },
    )

    route, handle, _ = router.match_route("/test/test2/subpath")
    assert route == "/test/test2" and handle == ("endpoint1", "default")
    route, handle, _ = router.match_route("/test/test2/")
    assert route == "/test/test2" and handle == ("endpoint1", "default")
    route, handle, _ = router.match_route("/test/test2")
    assert route == "/test/test2" and handle == ("endpoint1", "default")

    route, handle, _ = router.match_route("/test/subpath")
    assert route == "/test" and handle == ("endpoint2", "default")
    route, handle, _ = router.match_route("/test/")
    assert route == "/test" and handle == ("endpoint2", "default")
    route, handle, _ = router.match_route("/test")
    assert route == "/test" and handle == ("endpoint2", "default")

    route, handle, _ = router.match_route("/test2")
    assert route == "/" and handle == ("endpoint3", "default")
    route, handle, _ = router.match_route("/")
    assert route == "/" and handle == ("endpoint3", "default")


def test_update_routes(mock_router):
    router = mock_router
    router.update_routes(
        {DeploymentID("endpoint", "app1"): EndpointInfo(route="/endpoint")},
    )

    route, handle, app_is_cross_language = router.match_route("/endpoint")
    assert route == "/endpoint"
    assert handle == ("endpoint", "app1")
    assert not app_is_cross_language

    route, handle, app_is_cross_language = router.get_handle_for_endpoint("app1")
    assert route == "/endpoint"
    assert handle == ("endpoint", "app1")
    assert not app_is_cross_language

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
        },
    )

    assert router.match_route("/endpoint") is None
    assert router.match_route("app1") is None

    route, handle, app_is_cross_language = router.match_route("/endpoint2")
    assert route == "/endpoint2"
    assert handle == ("endpoint2", "app2")
    assert app_is_cross_language

    route, handle, app_is_cross_language = router.get_handle_for_endpoint("app2")
    assert route == "/endpoint2"
    assert handle == ("endpoint2", "app2")
    assert app_is_cross_language


class TestReadyForTraffic:
    @pytest.mark.parametrize("is_head", [False, True])
    def test_route_table_not_populated(self, mock_router, is_head: bool):
        """Proxy router should NOT be ready for traffic if:
        - it has not received route table from controller
        """

        ready_for_traffic, msg = mock_router.ready_for_traffic(is_head=is_head)
        assert not ready_for_traffic
        assert msg == NO_ROUTES_MESSAGE

    def test_head_route_table_populated_no_replicas(self, mock_router):
        """Proxy router should be ready for traffic if:
        - it has received route table from controller
        - it hasn't received any replicas yet
        - it lives on head node
        """

        d_id = DeploymentID(name="A", app_name="B")
        mock_router.update_routes({d_id: EndpointInfo(route="/")})
        mock_router.handles[d_id].set_running_replicas_populated(False)

        ready_for_traffic, msg = mock_router.ready_for_traffic(is_head=True)
        assert ready_for_traffic
        assert not msg

    def test_worker_route_table_populated_no_replicas(self, mock_router):
        """Proxy router should NOT be ready for traffic if:
        - it has received route table from controller
        - it hasn't received any replicas yet
        - it lives on a worker node
        """

        d_id = DeploymentID(name="A", app_name="B")
        mock_router.update_routes({d_id: EndpointInfo(route="/")})
        mock_router.handles[d_id].set_running_replicas_populated(False)

        ready_for_traffic, msg = mock_router.ready_for_traffic(is_head=False)
        assert not ready_for_traffic
        assert msg == NO_REPLICAS_MESSAGE

    @pytest.mark.parametrize("is_head", [False, True])
    def test_route_table_populated_with_replicas(self, mock_router, is_head: bool):
        """Proxy router should be ready for traffic if:
        - it has received route table from controller
        - it has received replicas from controller
        """

        d_id = DeploymentID(name="A", app_name="B")
        mock_router.update_routes({d_id: EndpointInfo(route="/")})
        mock_router.handles[d_id].set_running_replicas_populated(True)

        ready_for_traffic, msg = mock_router.ready_for_traffic(is_head=is_head)
        assert ready_for_traffic
        assert not msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
