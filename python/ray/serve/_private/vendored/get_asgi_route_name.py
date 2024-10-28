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

from typing import List, Optional

from starlette.routing import Match, Mount, Route
from starlette.types import ASGIApp, Scope


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
