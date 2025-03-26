# These imports determine whether or not a user has the required dependencies
# to launch the optional dashboard API server.
# If any of these imports fail, the dashboard API server will not be launched.
# Please add important dashboard-api dependencies to this list.

import aiohttp  # noqa: F401
import aiohttp.web  # noqa: F401
import aiohttp_cors  # noqa: F401
import grpc  # noqa: F401

# These checks have to come first because aiohttp looks
# for opencensus, too, and raises a different error otherwise.
import opencensus  # noqa: F401
import prometheus_client  # noqa: F401
import pydantic  # noqa: F401
from aiohttp import hdrs  # noqa: F401
from aiohttp.typedefs import PathLike  # noqa: F401
from aiohttp.web import Request  # noqa: F401
from aiohttp.web import RouteDef  # noqa: F401

# Adding new modules should also be reflected in the
# python/ray/tests/test_minimal_install.py
