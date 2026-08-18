"""Browser-request detection and blocking middleware for internal HTTP servers.

Internal Ray HTTP servers (dashboard head, dashboard agent, runtime env agent)
bind TCP sockets and are meant to be accessed only by other Ray processes.
Blocking browser-originated requests on them prevents DNS rebinding and CSRF
attacks against state-changing endpoints.

This module lives in ray._private so core components (e.g. the runtime env
agent) can use it without importing ray.dashboard, which is not import-safe
from minimal installations. ray.dashboard.optional_utils keeps its own
historical copy; converging the two is intentionally left out of scope to keep
changes minimal.
"""

from types import ModuleType
from typing import List, Optional, Set


def is_browser_request(req) -> bool:
    """Best-effort detection if the request was made by a browser.

    Uses three heuristics:
        1) If the `User-Agent` header starts with 'Mozilla'. This heuristic is weak,
        but hard for a browser to bypass e.g., fetch/xhr and friends cannot alter the
        user agent, but requests made with an HTTP library can stumble into this if
        they choose to user a browser-like user agent. At the time of writing, all
        common browsers' user agents start with 'Mozilla'.
        2) If any of the `Sec-Fetch-*` headers are present.
        3) If any of the various CORS headers are present
    """
    return req.headers.get("User-Agent", "").startswith("Mozilla") or any(
        h in req.headers
        for h in (
            # Origin and Referer are sent by browser user agents to give
            # information about the requesting origin
            "Referer",
            "Origin",
            # Sec-Fetch headers are sent with many but not all `fetch`
            # requests, and will eventually be sent on all requests.
            "Sec-Fetch-Mode",
            "Sec-Fetch-Dest",
            "Sec-Fetch-Site",
            "Sec-Fetch-User",
            # CORS headers specifying which other headers are modified
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
        )
    )


def get_browser_request_middleware(
    aiohttp_module: ModuleType,
    allowed_methods: Optional[Set[str]] = None,
    allowed_paths: Optional[List[str]] = None,
):
    """Create middleware that restricts browser access to specified HTTP methods.

    This middleware blocks browser requests to prevent DNS rebinding and CSRF
    attacks. Only explicitly allowed methods are permitted from browsers.

    Args:
        aiohttp_module: The aiohttp module to use
        allowed_methods: Set of HTTP methods browsers are allowed to use.
        allowed_paths: List of paths that bypass the method check entirely,
            allowing any method from browsers.

    Returns:
        An aiohttp middleware function
    """
    allowed_methods = allowed_methods or set()

    @aiohttp_module.web.middleware
    async def browser_request_middleware(request, handler):
        if not is_browser_request(request):
            return await handler(request)

        # Allow whitelisted paths to bypass the check
        if allowed_paths and request.path in allowed_paths:
            return await handler(request)

        # No methods allowed for browsers, return `403` status.
        if not allowed_methods:
            return aiohttp_module.web.Response(
                status=403, text="Browser requests not allowed."
            )

        # This specific method is not allowed, return `405` status.
        if request.method not in allowed_methods:
            return aiohttp_module.web.Response(
                status=405,
                text=f"'{request.method}' method not allowed for browser traffic.",
            )

        return await handler(request)

    return browser_request_middleware
