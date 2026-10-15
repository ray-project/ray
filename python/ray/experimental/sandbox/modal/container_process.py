"""A handle on a command running inside a Sandbox."""

import time
from typing import Optional

from ray.experimental.sandbox.modal._actor import STDERR_FD, STDOUT_FD
from ray.experimental.sandbox.modal._sync import synchronize_api
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    _sandbox_gone_as_not_found,
)
from ray.experimental.sandbox.modal.io_streams import (
    EXEC_MAX_BUFFER_SIZE,
    _StreamReader,
    _StreamWriter,
)
from ray.experimental.sandbox.modal.stream_type import StreamType


class _ContainerProcess:
    """Represents a process running in a Sandbox.

    Returned by :meth:`Sandbox.exec`. The interface mirrors
    :class:`subprocess.Popen`: read ``stdout``/``stderr``, write ``stdin``,
    then ``wait()`` for the exit code.
    """

    def __init__(
        self,
        actor,
        exec_id: str,
        *,
        stdout: StreamType = StreamType.PIPE,
        stderr: StreamType = StreamType.PIPE,
        text: bool = True,
        by_line: bool = False,
        exec_deadline: Optional[float] = None,
    ):
        self._actor = actor
        self._exec_id = exec_id
        self._exec_deadline = exec_deadline
        self._returncode: Optional[int] = None
        # The deadline travels with the streams too: on Modal an exec's output
        # is readable only until its timeout runs out.
        self._stdout = _StreamReader(
            actor,
            exec_id,
            STDOUT_FD,
            stream_type=stdout,
            text=text,
            by_line=by_line,
            deadline=exec_deadline,
        )
        self._stderr = _StreamReader(
            actor,
            exec_id,
            STDERR_FD,
            stream_type=stderr,
            text=text,
            by_line=by_line,
            deadline=exec_deadline,
        )
        self._stdin = _StreamWriter(actor, exec_id, EXEC_MAX_BUFFER_SIZE)

    def __repr__(self) -> str:
        return f"ContainerProcess(exec_id={self._exec_id!r})"

    @property
    def stdout(self) -> _StreamReader:
        """Reader for the process's stdout stream."""
        return self._stdout

    @property
    def stderr(self) -> _StreamReader:
        """Reader for the process's stderr stream."""
        return self._stderr

    @property
    def stdin(self) -> _StreamWriter:
        """Writer for the process's stdin stream."""
        return self._stdin

    @property
    def returncode(self) -> int:
        """The exit code of the process.

        Raises:
            InvalidError: ``wait()`` has not been called yet. To check on a
                still-running process without blocking, use ``poll()``.
        """
        if self._returncode is None:
            raise InvalidError(
                "You must call wait() before accessing the returncode. "
                "To poll for the status of a running process, use poll() instead."
            )
        return self._returncode

    async def poll(self) -> Optional[int]:
        """Check whether the process has finished.

        Once the ``timeout`` passed to ``exec`` has elapsed, the exit code is
        ``-1`` -- even for a command that had already exited, unless an
        earlier ``poll()`` or ``wait()`` saw its real code first. That is
        Modal's contract: its client reports a passed deadline without asking
        the worker at all.

        Returns:
            None while the process is still running, otherwise its exit code.
        """
        if self._returncode is not None:
            return self._returncode
        if self._deadline_passed():
            return self._timed_out()
        with _sandbox_gone_as_not_found():
            self._returncode = await self._actor.exec_poll.remote(self._exec_id)
        return self._returncode

    async def wait(self) -> int:
        """Wait for the process to finish and return its exit code.

        If the ``timeout`` passed to ``exec`` elapses first -- or had already
        elapsed when this is first called -- the exit code is ``-1``, and no
        exception is raised, as on Modal.
        """
        if self._returncode is not None:
            return self._returncode
        if self._deadline_passed():
            return self._timed_out()

        remaining = None
        if self._exec_deadline is not None:
            remaining = max(self._exec_deadline - time.monotonic(), 0)
        with _sandbox_gone_as_not_found():
            returncode = await self._actor.exec_wait.remote(self._exec_id, remaining)
        if returncode is None:
            # The deadline passed while waiting.
            return self._timed_out()
        self._returncode = returncode
        return self._returncode

    def _deadline_passed(self) -> bool:
        return (
            self._exec_deadline is not None and time.monotonic() >= self._exec_deadline
        )

    def _timed_out(self) -> int:
        """Report a passed deadline the way Modal does: -1, remembered.

        Nothing is killed from here. The actor enforces the same timeout on
        its own clock -- which started before this handle's, at exec_start --
        so the command is already stopped, as Modal's worker stops it there.
        """
        self._returncode = -1
        return self._returncode


ContainerProcess = synchronize_api(_ContainerProcess)
