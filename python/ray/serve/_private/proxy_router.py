import logging
from typing import Callable, Dict, List, Optional, Tuple

from ray.serve._private.common import ApplicationName, DeploymentID, EndpointInfo
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.handle import DeploymentHandle

logger = logging.getLogger(SERVE_LOGGER_NAME)

NO_ROUTES_MESSAGE = "Route table is not populated yet."
NO_REPLICAS_MESSAGE = "No replicas are available yet."


class ProxyRouter:
    """Router interface for the proxy to use."""

    def __init__(
        self,
        get_handle: Callable[[str, str], DeploymentHandle],
    ):
        # Function to get a handle given a name. Used to mock for testing.
        self._get_handle = get_handle
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[DeploymentID, DeploymentHandle] = dict()
        # Flipped to `True` once the route table has been updated at least once.
        # The proxy router is not ready for traffic until the route table is populated
        self._route_table_populated = False

        # Info used for HTTP proxy
        # Routes sorted in order of decreasing length.
        self.sorted_routes: List[str] = list()
        # Endpoints associated with the routes.
        self.route_info: Dict[str, DeploymentID] = dict()
        # Map of application name to is_cross_language.
        self.app_to_is_cross_language: Dict[ApplicationName, bool] = dict()

        # Info used for gRPC proxy
        # Endpoints info associated with endpoints.
        self.endpoints: Dict[DeploymentID, EndpointInfo] = dict()

    def ready_for_traffic(self, is_head: bool) -> Tuple[bool, str]:
        """Whether the proxy router is ready to serve traffic.

        The first return value will be false if any of the following hold:
        - The route table has not been populated yet with a non-empty set of routes
        - The route table has been populated, but none of the handles
          have received running replicas yet AND it lives on a worker node.

        Otherwise, the first return value will be true.
        """

        if not self._route_table_populated:
            return False, NO_ROUTES_MESSAGE

        # NOTE(zcin): For the proxy on the head node, even if none of its handles have
        # been populated with running replicas yet, we MUST mark the proxy as ready for
        # traffic. This is to handle the case when all deployments have scaled to zero.
        # If the deployments (more precisely, ingress deployments) have all scaled down
        # to zero, at least one proxy needs to be able to receive incoming requests to
        # trigger upscale.
        if is_head:
            return True, ""

        for handle in self.handles.values():
            if handle.running_replicas_populated():
                return True, ""

        return False, NO_REPLICAS_MESSAGE

    def update_routes(self, endpoints: Dict[DeploymentID, EndpointInfo]):
        logger.info(
            f"Got updated endpoints: {endpoints}.", extra={"log_to_stderr": True}
        )
        if endpoints:
            self._route_table_populated = True

        self.endpoints = endpoints

        existing_handles = set(self.handles.keys())
        routes = []
        route_info = {}
        app_to_is_cross_language = {}
        for endpoint, info in endpoints.items():
            routes.append(info.route)
            route_info[info.route] = endpoint
            app_to_is_cross_language[endpoint.app_name] = info.app_is_cross_language
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                self.handles[endpoint] = self._get_handle(endpoint, info)

        # Clean up any handles that are no longer used.
        if len(existing_handles) > 0:
            logger.info(
                f"Deleting {len(existing_handles)} unused handles.",
                extra={"log_to_stderr": False},
            )
        for endpoint in existing_handles:
            del self.handles[endpoint]

        # Routes are sorted in order of decreasing length to enable longest
        # prefix matching.
        self.sorted_routes = sorted(routes, key=lambda x: len(x), reverse=True)
        self.route_info = route_info
        self.app_to_is_cross_language = app_to_is_cross_language

    def match_route(
        self, target_route: str
    ) -> Optional[Tuple[str, DeploymentHandle, bool]]:
        """Return the longest prefix match among existing routes for the route.
        Args:
            target_route: route to match against.
        Returns:
            (route, handle, is_cross_language) if found, else None.
        """

        for route in self.sorted_routes:
            if target_route.startswith(route):
                matched = False
                # If the route we matched on ends in a '/', then so does the
                # target route and this must be a match.
                if route.endswith("/"):
                    matched = True
                # If the route we matched on doesn't end in a '/', we need to
                # do another check to ensure that either this is an exact match
                # or the next character in the target route is a '/'. This is
                # to guard against the scenario where we have '/route' as a
                # prefix and there's a request to '/routesuffix'. In this case,
                # it should *not* be a match.
                elif len(target_route) == len(route) or target_route[len(route)] == "/":
                    matched = True

                if matched:
                    endpoint = self.route_info[route]
                    return (
                        route,
                        self.handles[endpoint],
                        self.app_to_is_cross_language[endpoint.app_name],
                    )

        return None

    def get_handle_for_endpoint(
        self, target_app_name: str
    ) -> Optional[Tuple[str, DeploymentHandle, bool]]:
        """Return the handle that matches with endpoint.

        Args:
            target_app_name: app_name to match against.
        Returns:
            (route, handle, is_cross_language) for the single app if there
            is only one, else find the app and handle for exact match. Else return None.
        """
        for endpoint_tag, handle in self.handles.items():
            # If the target_app_name matches with the endpoint or if
            # there is only one endpoint.
            if target_app_name == endpoint_tag.app_name or len(self.handles) == 1:
                endpoint_info = self.endpoints[endpoint_tag]
                return (
                    endpoint_info.route,
                    handle,
                    endpoint_info.app_is_cross_language,
                )

        return None
