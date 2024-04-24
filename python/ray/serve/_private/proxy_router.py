import logging
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Tuple

from ray.serve._private.common import (
    ApplicationName,
    DeploymentHandleSource,
    DeploymentID,
    EndpointInfo,
    RequestProtocol,
)
from ray.serve._private.constants import (
    RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING,
    SERVE_LOGGER_NAME,
)
from ray.serve.handle import DeploymentHandle

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ProxyRouter(ABC):
    """Router interface for the proxy to use."""

    @abstractmethod
    def update_routes(self, endpoints: Dict[DeploymentID, EndpointInfo]):
        raise NotImplementedError


class LongestPrefixRouter(ProxyRouter):
    """Router that performs longest prefix matches on incoming routes."""

    def __init__(
        self,
        get_handle: Callable[[str, str], DeploymentHandle],
        protocol: RequestProtocol,
    ):
        # Function to get a handle given a name. Used to mock for testing.
        self._get_handle = get_handle
        # Protocol to config handle
        self._protocol = protocol
        # Routes sorted in order of decreasing length.
        self.sorted_routes: List[str] = list()
        # Endpoints associated with the routes.
        self.route_info: Dict[str, DeploymentID] = dict()
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[DeploymentID, DeploymentHandle] = dict()
        # Map of application name to is_cross_language.
        self.app_to_is_cross_language: Dict[ApplicationName, bool] = dict()

    def update_routes(self, endpoints: Dict[DeploymentID, EndpointInfo]) -> None:
        logger.info(
            f"Got updated endpoints: {endpoints}.", extra={"log_to_stderr": False}
        )

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
                handle = self._get_handle(endpoint.name, endpoint.app_name).options(
                    # Streaming codepath isn't supported for Java.
                    stream=not info.app_is_cross_language,
                    _prefer_local_routing=RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING,
                    _source=DeploymentHandleSource.PROXY,
                )
                handle._set_request_protocol(self._protocol)
                self.handles[endpoint] = handle

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


class EndpointRouter(ProxyRouter):
    """Router that matches endpoint to return the handle."""

    def __init__(self, get_handle: Callable, protocol: RequestProtocol):
        # Function to get a handle given a name. Used to mock for testing.
        self._get_handle = get_handle
        # Protocol to config handle
        self._protocol = protocol
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[DeploymentID, DeploymentHandle] = dict()
        # Endpoints info associated with endpoints.
        self.endpoints: Dict[DeploymentID, EndpointInfo] = dict()

    def update_routes(self, endpoints: Dict[DeploymentID, EndpointInfo]):
        logger.info(
            f"Got updated endpoints: {endpoints}.", extra={"log_to_stderr": False}
        )
        self.endpoints = endpoints
        existing_handles = set(self.handles.keys())
        for endpoint, info in endpoints.items():
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                handle = self._get_handle(endpoint.name, endpoint.app_name).options(
                    # Streaming codepath isn't supported for Java.
                    stream=not info.app_is_cross_language,
                    _prefer_local_routing=RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING,
                    _source=DeploymentHandleSource.PROXY,
                )
                handle._set_request_protocol(self._protocol)
                self.handles[endpoint] = handle

        # Clean up any handles that are no longer used.
        if len(existing_handles) > 0:
            logger.info(
                f"Deleting {len(existing_handles)} unused handles.",
                extra={"log_to_stderr": False},
            )
        for endpoint in existing_handles:
            del self.handles[endpoint]

    def get_handle_for_endpoint(
        self, target_app_name: str
    ) -> Optional[Tuple[str, DeploymentHandle, bool]]:
        """Return the handle that matches with endpoint.

        Args:
            target_app_name: app_name to match against.
        Returns:
            (route, handle, app_name, is_cross_language) for the single app if there
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
