"""Readiness probes for Sandboxes.

A probe is a check the backend runs inside a Sandbox on an interval until it
passes, so that a caller can start a Sandbox whose setup takes a while and then
block until that setup has finished::

    sandbox = modal.Sandbox.create(
        "bash", "-c", "sleep 5 && touch /tmp/ready && sleep 3600",
        readiness_probe=modal.Probe.with_exec(
            "sh", "-c", "test -f /tmp/ready", interval_ms=250
        ),
    )
    sandbox.wait_until_ready()

The class mirrors ``modal.Probe``: a plain value object built through one of the
two classmethods, never constructed directly. Unlike most types in this package
it is not run through ``synchronize_api`` -- it has no async methods, and
upstream's is a plain dataclass too, so a caller can build one without a running
event loop.
"""

import dataclasses
from typing import Optional, Tuple

from ray.experimental.sandbox.modal._command import validate_exec_args
from ray.experimental.sandbox.modal.exception import InvalidError, NotSupportedError
from ray.util.annotations import PublicAPI

# Modal's default, in milliseconds.
DEFAULT_INTERVAL_MS = 100

# Shared by :meth:`Probe.with_tcp` and the direct-construction check in
# ``__post_init__``, so a caller reaches the same explanation whichever way they
# tried to build a TCP probe.
_NO_TCP_PROBE_MESSAGE = (
    "TCP readiness probes are not supported by the Ray sandbox backend. "
    "The backend publishes no ports and has no tunnels, so there is "
    "nothing for a port check to observe. Use Probe.with_exec() to "
    "check readiness from inside the Sandbox instead -- for a server, "
    "a command that connects to the port locally."
)


@PublicAPI(stability="alpha")
@dataclasses.dataclass(frozen=True)
class Probe:
    """A readiness check for a Sandbox.

    Build one with :meth:`with_exec`. Instances are immutable and picklable, so
    the probe travels to the actor that owns the Sandbox.

    Attributes:
        tcp_port: Always None. The field exists so the attribute surface matches
            Modal's, but any other value is refused; see :meth:`with_tcp`.
        exec_argv: The command run inside the Sandbox. It is ready when the
            command exits 0.
        interval_ms: How long to wait between attempts.
    """

    tcp_port: Optional[int] = None
    exec_argv: Optional[Tuple[str, ...]] = None
    interval_ms: int = DEFAULT_INTERVAL_MS

    def __post_init__(self) -> None:
        """Reject a probe the actor could not run, as Modal does.

        Direct construction is part of Modal's surface, so none of this can be
        left to the ``with_*`` builders. Every check here exists because the
        actor's probe loop has no way to report a malformed probe: whatever it
        raises is swallowed by :meth:`_run_probe`'s catch-all and recorded as
        the probe having *ended*, so ``wait_until_ready()`` reports a perfectly
        healthy Sandbox as terminated.

        ``tcp_port`` is the case that bites: naming it alone satisfies the
        "exactly one" rule below, then fails on ``list(None)`` inside the actor.
        It is refused here with the same explanation :meth:`with_tcp` gives, at
        the line that built the probe.
        """
        if (self.tcp_port is None) == (self.exec_argv is None):
            raise InvalidError(
                "A Probe must specify exactly one of tcp_port or exec_argv."
            )
        if self.tcp_port is not None:
            raise NotSupportedError(_NO_TCP_PROBE_MESSAGE)
        # The same rules Sandbox.exec applies, for the same reason: an empty or
        # non-string argv reaches runsc as a command it cannot run.
        validate_exec_args(self.exec_argv)
        _validate_interval(self.interval_ms)

    @classmethod
    def with_exec(cls, *argv: str, interval_ms: int = DEFAULT_INTERVAL_MS) -> "Probe":
        """Build a probe that runs a command until it exits 0.

        Args:
            *argv: Command and arguments to run inside the Sandbox. Held to the
                same rules as :meth:`Sandbox.exec`.
            interval_ms: Milliseconds between attempts. Defaults to 100.

        Returns:
            A :class:`Probe` to pass as ``Sandbox.create(readiness_probe=...)``.

        Raises:
            InvalidError: The command or the interval is not usable.
        """
        # Both arguments are checked by __post_init__, which direct
        # construction has to go through anyway.
        return cls(exec_argv=tuple(argv), interval_ms=interval_ms)

    @classmethod
    def with_tcp(cls, port: int, *, interval_ms: int = DEFAULT_INTERVAL_MS) -> "Probe":
        """Unsupported. TCP readiness probes need networking this backend lacks.

        Defined rather than left off so that ported code gets a directed error
        at the line that built the probe, instead of an ``AttributeError`` here
        or a confusing failure later from ``Sandbox.create``.

        Args:
            port: Unsupported. The port that would be checked.
            interval_ms: Unsupported. Milliseconds between attempts.

        Returns:
            Never returns.

        Raises:
            NotImplementedError: Always.
        """
        raise NotSupportedError(_NO_TCP_PROBE_MESSAGE)


def _validate_interval(interval_ms: int) -> None:
    if not isinstance(interval_ms, int) or isinstance(interval_ms, bool):
        raise InvalidError(
            f"interval_ms must be an int, got {type(interval_ms).__name__}."
        )
    if interval_ms <= 0:
        raise InvalidError(f"interval_ms must be positive, got {interval_ms}.")
