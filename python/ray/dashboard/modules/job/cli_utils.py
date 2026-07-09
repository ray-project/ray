import functools
import re
from typing import Union

import click

# Matches an unindented "ExceptionType: message" summary line, the last line
# of any standard Python traceback. The exception-type part is restricted to
# (optionally dotted/qualified) identifiers whose final component starts with
# an uppercase letter, per PEP 8 exception naming, so prose or trailing
# metadata lines like "Request failed with status code 500: ..." or
# "submission_id: my_job" aren't mistaken for the actual exception summary.
_EXCEPTION_SUMMARY_RE = re.compile(
    r"^((?:[a-zA-Z_][a-zA-Z0-9_]*\.)*[A-Z][a-zA-Z0-9_]*): (.+)$", re.MULTILINE
)


def extract_concise_error_message(message: str) -> str:
    """Extract the innermost exception summary from a chain of nested tracebacks.

    Ray's job submission errors are RuntimeErrors whose message embeds the
    traceback text of the next hop in the CLI -> dashboard SDK -> job head ->
    job agent -> job manager forwarding chain, so the real, actionable error is
    the last "ExceptionType: message" line, buried under internal plumbing
    frames. Returns the original message unchanged if no such line is found.
    """
    matches = _EXCEPTION_SUMMARY_RE.findall(message)
    if not matches:
        return message
    exc_type, exc_message = matches[-1]
    return f"{exc_type}: {exc_message}"


def bool_cast(string: str) -> Union[bool, str]:
    """Cast a string to a boolean if possible, otherwise return the string."""
    if string.lower() == "true" or string == "1":
        return True
    elif string.lower() == "false" or string == "0":
        return False
    else:
        return string


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
