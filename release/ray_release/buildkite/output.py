from typing import Optional, Callable

from ray_release.buildkite.utils import is_in_buildkite


def buildkite_echo(message: str, print_fn: Callable[[str], None] = print):
    if is_in_buildkite():
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
