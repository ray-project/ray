import os
from typing import Optional, Callable


def buildkite_echo(message: str, print_fn: Callable[[str], None] = print):
    if "BUILDKITE" in os.environ:
        print_fn(message)


def buildkite_group(
    name: str, open: Optional[bool] = None, print_fn: Callable[[str], None] = print
):
    if open is True:
        buildkite_echo(f"+++ {name}", print_fn=print_fn)
    elif open is False:
        buildkite_echo(f"~~~ {name}", print_fn=print_fn)
    else:  # None
        buildkite_echo(f"--- {name}", print_fn=print_fn)


def buildkite_open_last(print_fn: Callable[[str], None] = print):
    buildkite_echo("^^^ +++", print_fn=print_fn)
