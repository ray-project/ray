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
    """Collapse a chain of nested job-submission tracebacks to the last stack.

    Ray's job submission errors are RuntimeErrors whose message embeds the
    traceback text of the next hop in the CLI -> dashboard SDK -> job head ->
    job agent -> job manager forwarding chain. Each outer hop only wraps the
    next hop's traceback in an HTTP status message (e.g. "Request failed with
    status code 500"), so the actionable error is the last (deepest) stack.

    Drops the outer forwarding hops but keeps that last stack trace in full --
    its frames and root-cause exception line -- so it stays debuggable. Returns
    the message unchanged when it has no exception summary line, and just the
    single summary line when there is no nesting to collapse.
    """
    matches = list(_EXCEPTION_SUMMARY_RE.finditer(message))
    if not matches:
        return message
    last = matches[-1]
    innermost = f"{last.group(1)}: {last.group(2)}"
    if len(matches) < 2:
        return innermost
    penultimate = matches[-2]
    # Only collapse when the penultimate summary line actually introduces
    # another (outer) traceback, i.e. this really is a nested forwarding chain.
    if "Traceback (most recent call last)" not in penultimate.group(2):
        return innermost
    # Keep the last stack in full, dropping only the outer forwarding hop(s).
    return message[penultimate.start() :].strip()


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
