import logging
from functools import wraps

from aiohttp.web import Response

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def validate_endpoint(log_deprecation_warning: bool):
    def decorator(func):
        @wraps(func)
        async def check(self, *args, **kwargs):
            try:
                from ray import serve  # noqa: F401

                if log_deprecation_warning:
                    logger.info(
                        "The Serve REST API on the dashboard agent is deprecated. "
                        "Send requests to the Serve REST API directly to the "
                        "dashboard instead. If you're using default ports, this "
                        "means you should send the request to the same route on "
                        "port 8265 instead of 52365."
                    )
            except ImportError:
                return Response(
                    status=501,
                    text=(
                        "Serve dependencies are not installed. Please run `pip "
                        'install "ray[serve]"`.'
                    ),
                )
            return await func(self, *args, **kwargs)

        return check

    return decorator
