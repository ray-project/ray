# This code is adapted from:
# https://github.com/elastic/apm-agent-python/blob/f570e8c2b68a8714628acac815aebcc3518b44c7/elasticapm/contrib/starlette/__init__.py
#
#  BSD 3-Clause License
#
#  Copyright (c) 2012, the Sentry Team, see AUTHORS for more details
#  Copyright (c) 2019, Elasticsearch BV
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#
#  * Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
#  * Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
#  * Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from starlette.routing import Match, Mount, Route
from starlette.types import ASGIApp, Scope


@dataclass(frozen=True)
class RoutePattern:
    """Represents a route pattern with optional HTTP method restrictions.

    Attributes:
        methods: List of HTTP methods (e.g., ["GET", "POST"]), or None if the route
                 accepts all methods (e.g., WebSocket routes, ASGI apps).
        path: The route path pattern (e.g., "/", "/users/{user_id}").
    """

    methods: Optional[List[str]]
    path: str


def _get_route_name(
    scope: Scope, routes: List[Route], *, route_name: Optional[str] = None
) -> Optional[str]:
    for route in routes:
        match, child_scope = route.matches(scope)
        if match == Match.FULL:
            route_name = route.path
            child_scope = {**scope, **child_scope}
            if isinstance(route, Mount) and route.routes:
                child_route_name = _get_route_name(
                    child_scope, route.routes, route_name=route_name
                )
                if child_route_name is None:
                    route_name = None
                else:
                    route_name += child_route_name
            return route_name
        elif match == Match.PARTIAL and route_name is None:
            route_name = route.path

    return None


def get_asgi_route_name(app: ASGIApp, scope: Scope) -> Optional[str]:
    """Gets route name for given request taking mounts into account."""

    routes = app.routes
    route_name = _get_route_name(scope, routes)

    # Starlette magically redirects requests if the path matches a route name
    # with a trailing slash appended or removed. To not spam the transaction
    # names list, we do the same here and put these redirects all in the
    # same "redirect trailing slashes" transaction name.
    if not route_name and app.router.redirect_slashes and scope["path"] != "/":
        redirect_scope = dict(scope)
        if scope["path"].endswith("/"):
            redirect_scope["path"] = scope["path"][:-1]
            trim = True
        else:
            redirect_scope["path"] = scope["path"] + "/"
            trim = False

        route_name = _get_route_name(redirect_scope, routes)
        if route_name is not None:
            route_name = route_name + "/" if trim else route_name[:-1]

    if route_name:
        root_path = scope.get("root_path", "")
        if root_path:
            route_name = root_path.rstrip("/") + "/" + route_name.lstrip("/")

    return route_name


def extract_route_patterns(app: ASGIApp) -> List[RoutePattern]:
    """Extracts all route patterns from an ASGI app.

    This function recursively traverses the app's routes (including mounted apps)
    and returns a list of all route patterns. This is used to communicate available
    routes from build time to proxies for accurate metrics tagging.

    Args:
        app: The ASGI application (typically FastAPI or Starlette)

    Returns:
        List of RoutePattern objects. Examples:
        - RoutePattern(methods=["GET", "POST"], path="/"): GET and POST to root
        - RoutePattern(methods=["GET"], path="/users/{id}"): GET to users endpoint
        - RoutePattern(methods=None, path="/websocket"): No method restrictions
    """
    # Use a dict to store path -> set of methods mapping
    # This allows us to track which methods apply to each path
    # Use None as a sentinel value to indicate "no method restrictions"
    path_methods: Dict[str, Optional[Set[str]]] = {}

    def _extract_from_routes(routes: List[Route], prefix: str = "") -> None:
        for route in routes:
            route_path = prefix + route.path

            if isinstance(route, Mount):
                # Recursively extract patterns from mounted apps
                if hasattr(route, "routes") and route.routes:
                    _extract_from_routes(route.routes, route_path)
                else:
                    # Mount without sub-routes - no method restrictions
                    if route_path not in path_methods:
                        path_methods[route_path] = None
            else:
                # Regular route - extract methods if available
                if hasattr(route, "methods") and route.methods:
                    # Route has specific methods
                    if route_path not in path_methods:
                        path_methods[route_path] = set()
                    # Only add methods if we haven't already marked this path as "all methods"
                    if path_methods[route_path] is not None:
                        path_methods[route_path].update(route.methods)
                else:
                    # Route has no method restrictions (accepts all methods)
                    # Mark this path as accepting all methods (None)
                    path_methods[route_path] = None

    try:
        if hasattr(app, "routes"):
            _extract_from_routes(app.routes)

            # Handle root_path if present
            if hasattr(app, "root_path") and app.root_path:
                root_path = app.root_path.rstrip("/")
                adjusted_path_methods = {}
                for path, methods in path_methods.items():
                    adjusted_path = (
                        root_path + "/" + path.lstrip("/")
                        if path != "/"
                        else root_path + path
                    )
                    adjusted_path_methods[adjusted_path] = methods
                path_methods = adjusted_path_methods
    except Exception:
        # If extraction fails for any reason, return empty list
        # This shouldn't break the system
        return []

    # Convert path_methods dict to list of RoutePattern objects
    patterns: List[RoutePattern] = []
    for path, methods in path_methods.items():
        if methods is None:
            # No method restrictions
            patterns.append(RoutePattern(methods=None, path=path))
        else:
            # Convert set to sorted list for consistent ordering
            methods_list = sorted(methods)
            patterns.append(RoutePattern(methods=methods_list, path=path))

    # Sort by path for consistent ordering
    return sorted(patterns, key=lambda x: x.path)
