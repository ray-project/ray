"""Stream disposition for Sandbox and container-process I/O."""

import subprocess
from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class StreamType(Enum):
    """Where the output of a stream should go.

    Values alias the ``subprocess`` sentinels of the same name.
    """

    # Discard all output from the stream.
    DEVNULL = subprocess.DEVNULL
    # Buffer output in a pipe to be read by the client.
    PIPE = subprocess.PIPE
    # Print output to the local stdout as it arrives.
    STDOUT = subprocess.STDOUT

    def __repr__(self):
        return f"{self.__module__}.{self.__class__.__name__}.{self.name}"


__all__ = ["StreamType"]
