import functools
import json
import os
from typing import Any, Dict, Optional, Union

import click


def bool_cast(string: str) -> Union[bool, str]:
    """Cast a string to a boolean if possible, otherwise return the string."""
    if string.lower() == "true" or string == "1":
        return True
    elif string.lower() == "false" or string == "0":
        return False
    else:
        return string


def parse_headers(headers: Optional[str], *, env_var: str) -> Optional[Dict[str, Any]]:
    """Parse HTTP headers from a JSON string or environment variable."""
    if headers is None:
        headers = os.environ.get(env_var)
    if headers is None:
        return None

    try:
        parsed_headers = json.loads(headers)
    except Exception as exc:
        raise ValueError(
            "Failed to parse headers into JSON. " 'Expected format: {"KEY": "VALUE"}.'
        ) from exc

    if not isinstance(parsed_headers, dict):
        raise ValueError("Expected headers to be a JSON object/dictionary.")

    return dict(parsed_headers)


class BoolOrStringParam(click.ParamType):
    """A click parameter that can be either a boolean or a string."""

    name = "BOOL | TEXT"

    def convert(self, value, param, ctx):
        if isinstance(value, bool):
            return value
        else:
            return bool_cast(value)


def add_common_job_options(func):
    """Decorator for adding CLI flags shared by all `ray job` commands."""

    @click.option(
        "--verify",
        default=True,
        show_default=True,
        type=BoolOrStringParam(),
        help=(
            "Boolean indication to verify the server's TLS certificate or a path to"
            " a file or directory of trusted certificates."
        ),
    )
    @click.option(
        "--headers",
        required=False,
        type=str,
        default=None,
        help=(
            "Used to pass headers through http/s to the Ray Cluster."
            'please follow JSON formatting formatting {"key": "value"}'
        ),
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
