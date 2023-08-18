from abc import ABC, abstractmethod
import logging
from typing import Callable, Dict, List, Optional, Tuple

from ray.serve._private.common import (
    ApplicationName,
    EndpointInfo,
    EndpointTag,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
)
from ray.serve.handle import RayServeHandle

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ProxyRouter(ABC):
    """Router interface for the proxy to use."""

    @abstractmethod
    def update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]):
        raise NotImplementedError


class LongestPrefixRouter(ProxyRouter):
    """Router that performs longest prefix matches on incoming routes."""

    def __init__(self, get_handle: Callable):
        # Function to get a handle given a name. Used to mock for testing.
        self._get_handle = get_handle
        # Routes sorted in order of decreasing length.
        self.sorted_routes: List[str] = list()
        # Endpoints associated with the routes.
        self.route_info: Dict[str, EndpointTag] = dict()
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[EndpointTag, RayServeHandle] = dict()
        # Map of application name to is_cross_language.
        self.app_to_is_cross_language: Dict[ApplicationName, bool] = dict()

    def update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
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
            app_to_is_cross_language[endpoint.app] = info.app_is_cross_language
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                self.handles[endpoint] = self._get_handle(
                    endpoint.name, endpoint.app
                ).options(
                    # Streaming codepath isn't supported for Java.
                    stream=(
                        RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING
                        and not info.app_is_cross_language
                    ),
                )

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
    ) -> Optional[Tuple[str, RayServeHandle, str, bool]]:
        """Return the longest prefix match among existing routes for the route.
        Args:
            target_route: route to match against.
        Returns:
            (route, handle, app_name, is_cross_language) if found, else None.
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
                        endpoint.app,
                        self.app_to_is_cross_language[endpoint.app],
                    )

        return None


class EndpointRouter(ProxyRouter):
    """Router that matches endpoint to return the handle."""

    def __init__(self, get_handle: Callable):
        # Function to get a handle given a name. Used to mock for testing.
        self._get_handle = get_handle
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[EndpointTag, RayServeHandle] = dict()
        # Endpoints info associated with endpoints.
        self.endpoints: Dict[EndpointTag, EndpointInfo] = dict()

    def update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]):
        logger.info(
            f"Got updated endpoints: {endpoints}.", extra={"log_to_stderr": False}
        )
        self.endpoints = endpoints
        existing_handles = set(self.handles.keys())
        for endpoint, info in endpoints.items():
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                self.handles[endpoint] = self._get_handle(
                    endpoint.name, endpoint.app
                ).options(
                    # Streaming codepath isn't supported for Java.
                    stream=(
                        RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING
                        and not info.app_is_cross_language
                    ),
                )

        # Clean up any handles that are no longer used.
        if len(existing_handles) > 0:
            logger.info(
                f"Deleting {len(existing_handles)} unused handles.",
                extra={"log_to_stderr": False},
            )
        for endpoint in existing_handles:
            del self.handles[endpoint]

    def get_handle_for_endpoint(
        self, target_endpoint: str
    ) -> Optional[Tuple[str, RayServeHandle, str, bool]]:
        """Return the handle that matches with endpoint.

        Args:
            target_endpoint: endpoint to match against.
        Returns:
            (route, handle, app_name, is_cross_language) for the single app if there
            is only one, else find the app and handle for exact match. Else return None.
        """
        for endpoint_tag, handle in self.handles.items():
            # If the target_endpoint matches with the endpoint or if
            # there is only one endpoint.
            if target_endpoint == str(endpoint_tag) or len(self.handles) == 1:
                endpoint_info = self.endpoints[endpoint_tag]
                return (
                    endpoint_info.route,
                    handle,
                    endpoint_tag.app,
                    endpoint_info.app_is_cross_language,
                )

        return None
