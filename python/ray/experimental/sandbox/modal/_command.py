"""Validation shared by every place that sends an argv into a Sandbox.

Lives apart from :mod:`~ray.experimental.sandbox.modal.sandbox` only to keep the
import graph acyclic: ``sandbox`` imports ``probe`` for the ``readiness_probe``
type check, and ``probe`` validates its own command with the same rules
``Sandbox.exec`` applies, so the rules cannot live in either one.
"""

from typing import Sequence

from ray.experimental.sandbox.modal.exception import InvalidError

# Total size of the arguments to a single exec, matching Modal's limit.
ARG_MAX_BYTES = 2**16


def validate_exec_args(args: Sequence[str]) -> None:
    """Check that ``args`` is a usable argv.

    Args:
        args: The command and its arguments.

    Raises:
        InvalidError: The argv is empty, holds a non-string, or exceeds
            ``ARG_MAX_BYTES`` in total.
    """
    if not args:
        raise InvalidError("No command was provided to exec.")
    total = 0
    for arg in args:
        if not isinstance(arg, str):
            raise InvalidError(
                f"Command arguments must be strings, got {type(arg).__name__}."
            )
        # Code points, not encoded bytes, because that is what Modal counts
        # against the same limit. Measuring UTF-8 here would reject an argv of
        # non-ASCII text that Modal accepts, at up to four times too small.
        total += len(arg)
    if total > ARG_MAX_BYTES:
        raise InvalidError(f"Total command length exceeds {ARG_MAX_BYTES} bytes.")
