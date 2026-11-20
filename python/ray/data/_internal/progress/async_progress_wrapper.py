"""Non-blocking progress rendering for Ray Data.

SKELETON ONLY — every method body is a TODO. See async_progress_wrapper_GUIDELINE.md.

The scheduling loop and the consumer's iteration thread call a progress manager
synchronously today, so a slow or wedged terminal write blocks the caller and
stalls the pipeline. This module moves all terminal I/O onto a single
process-wide daemon render thread.

Two objects, deliberately separated:

  * ``AsyncProgressManagerWrapper`` is per-execution. It wraps the real manager,
    accumulates update state under a lock, and returns immediately. It never
    touches the terminal.
  * ``_AsyncProgressRenderer`` is a process-wide singleton (one daemon thread,
    modeled on ``ray.data._internal.usage.poller``). Once per ``RENDER_INTERVAL_S``
    it snapshots each registered wrapper's accumulated state and calls the real
    manager. This is the only place terminal I/O happens.

Design invariants:
  I1. Callers never block on terminal I/O. Setters assign under a lock held for
      O(1) and return; all wrapped-manager calls happen on the render thread.
  I2. State is accumulated, not queued: pending size is bounded by #operators,
      never by #updates.
  I3. Exactly one render thread process-wide, regardless of #executions, and no
      wrapper may be kept alive by the registry alone.
  I4. A wedged terminal is detectable, and the condition clears by itself once
      the terminal recovers.
  I5. ``close()`` must stay bounded against a dead terminal without relying on a
      timeout. Bounded process exit comes from the render thread being a daemon.
"""

import logging
import typing
from typing import List, Optional, Tuple

from ray.data._internal.progress.base_progress import BaseExecutionProgressManager

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)

# How often the render thread wakes to paint accumulated state.
# TODO(tune): pick a value that keeps repaint cadence within 2x of master on a
# healthy TTY (requirement R5). Validate with the repaint-rate test.
RENDER_INTERVAL_S = 0.5

# How long a single frame may stay in flight before we treat the terminal as
# wedged. This is a health signal, NOT a close-path timeout (see I5).
# TODO(tune): choose a default and cover it in the wedge test.
WEDGE_TIMEOUT_S = 30.0


# The contract between _take_pending() and _flush(): everything one interval
# accumulated, in the order the fields are documented on the wrapper.
_PendingSnapshot = Tuple[
    int,
    Optional[int],
    bool,
    Optional[str],
    List[Tuple["OpState", "ResourceManager"]],
    bool,
]


class AsyncProgressManagerWrapper(BaseExecutionProgressManager):
    """Per-execution wrapper. Records update state and returns; the singleton
    renderer paints it. Implements BaseExecutionProgressManager so it is a
    drop-in for the real manager in get_progress_manager().
    """

    def __init__(self, wrapped_manager: BaseExecutionProgressManager):
        # TODO(0): hold the wrapped manager, plus the pending state the renderer
        # will drain. You need, at minimum:
        #   - a lock guarding every pending field (written by caller threads,
        #     drained by the render thread)
        #   - a running row count, the latest known row total, and a way to tell
        #     "a row update arrived this interval" apart from "zero rows arrived"
        #   - the latest resource-status string
        #   - the latest per-operator entry, keyed so repeats collapse (I2)
        #   - a refresh-requested flag
        #   - a signal that close() has begun, so the renderer can skip us
        # Every one of these must be fixed-size: nothing may grow with the
        # number of updates received.
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Caller side (scheduling loop + consumer threads). Assign and return. #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Register with the singleton renderer so it begins painting this
        execution. Must be non-blocking.

        NOTE: do NOT call self._wrapped.start() here — rich's Live.start()
        writes to the terminal, which would block the caller. The render thread
        owns all wrapped-manager I/O.
        """
        # TODO(1): make this execution known to the process-wide renderer, and
        # arrange for the wrapped manager's own start() to happen exactly once,
        # on the render thread rather than here.
        raise NotImplementedError

    def update_total_progress(self, new_rows: int, total_rows: Optional[int]) -> None:
        # TODO(2): record this update and return. Two different kinds of value
        # arrive here, and they must be treated differently:
        #   - `new_rows` is a per-step DELTA. Nothing else in the system records
        #     it, so if you keep only the most recent one you permanently lose
        #     every row counted in between and the final count comes out wrong
        #     (I2 / requirement R3).
        #   - `total_rows` is a running total, i.e. a snapshot of a known value.
        # Also leave a way for the renderer to distinguish "an update arrived
        # this interval" from "the delta happened to be zero".
        raise NotImplementedError

    def update_total_resource_status(self, resource_status: str) -> None:
        # TODO(3): record and return. This is a display string with no history —
        # only the most recent one is ever painted.
        raise NotImplementedError

    def update_operator_progress(
        self, op_state: "OpState", resource_manager: "ResourceManager"
    ) -> None:
        # TODO(4): record and return. Keep one entry per operator, not one per
        # call, or a stall would let this grow without bound (I2). Think about
        # what identifies "the same operator" across calls.
        # Note `op_state` is a live, mutating object — consider what that means
        # for whether you need to copy anything here.
        raise NotImplementedError

    def refresh(self) -> None:
        # TODO(5): record that a repaint was asked for, and return.
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Render side. Called ONLY by the singleton renderer thread.          #
    # ------------------------------------------------------------------ #

    def _take_pending(self) -> _PendingSnapshot:
        """Atomically snapshot the pending state and reset it. Brief lock."""
        # TODO(6): hand back everything accumulated since the last call and clear
        # it, so the next interval starts empty.
        # Careful: not every field should be cleared. Ask of each one, "is this a
        # delta that I am consuming, or the latest known value of something that
        # is still true?" Clearing the second kind loses information the renderer
        # still needs on a later interval.
        raise NotImplementedError

    def _flush(self) -> None:
        """Snapshot, then call the real manager OUTSIDE the lock."""
        # TODO(7): drain via _take_pending(), then forward to the wrapped manager
        # as at most one call per method.
        #   - The lock must NOT be held while calling the wrapped manager: those
        #     calls are the terminal I/O that may block for minutes, and holding
        #     the lock there would block the caller threads too (I1).
        #   - Only forward what actually arrived; skip the rest.
        #   - Repainting is itself the expensive write, so decide when it is
        #     worth doing at all — an idle pipeline should not be repainting
        #     every interval.
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Close. Current thread, health-gated, no timeout (I5).               #
    # ------------------------------------------------------------------ #

    def close_with_finishing_description(self, desc: str, success: bool) -> None:
        # TODO(8): execution is over by now, so there is no scheduling loop left
        # to protect and this may render on the calling thread (agreed with
        # Daniel: "for closing only, for simplicity sake, we can just render on
        # the current thread").
        # Work out the ordering for yourself, but it has to achieve:
        #   - the renderer stops touching this wrapper, and stops being able to
        #     repaint over the final line afterwards
        #   - anything still accumulated gets painted, so the last interval is
        #     not silently dropped
        #   - the final description is written
        #   - the caller is NOT left blocked forever if the terminal is dead.
        #     Do this WITHOUT a timeout — there is already a signal available
        #     that tells you whether the terminal is currently wedged (I4/I5).
        #   - a failure in any of this can never take the job down. A progress
        #     bar must not be fatal.
        raise NotImplementedError

    def __getattr__(self, name):
        # TODO(9): anything this wrapper does not define should fall through to
        # the wrapped manager. Watch out: this hook also fires for attributes
        # that do not exist yet (e.g. during __init__), so a naive forward can
        # recurse forever. Guard against that.
        raise NotImplementedError

    def __repr__(self):
        # TODO(10): make it obvious in logs that this is a wrapper, and what it
        # is wrapping.
        raise NotImplementedError


class _AsyncProgressRenderer:
    """Process-wide singleton. One daemon thread paints every registered
    wrapper once per RENDER_INTERVAL_S. Modeled on
    ray.data._internal.usage.poller.ClusterMetricsPoller.
    """

    def __init__(self, render_interval: float = RENDER_INTERVAL_S):
        # TODO(11): set up the renderer's own state:
        #   - the interval
        #   - a lock guarding the registry and the frame bookkeeping below
        #   - the set of wrappers currently being painted. Executions can be
        #     abandoned without close() on error paths, and each wrapper reaches
        #     a whole topology — so the registry must not be what keeps a dead
        #     wrapper alive (I3). Pick a container accordingly.
        #   - whatever bookkeeping is_healthy() needs to tell "a frame is taking
        #     too long" from "nothing is happening right now" (I4)
        #   - the thread handle
        raise NotImplementedError

    def _start_thread_if_not_running(self) -> None:
        # TODO(12): start the render thread if it is not already running, and be
        # safe to call more than once.
        # It MUST be a daemon: a render blocked on a dead terminal must never
        # stop the interpreter from exiting (I5). Give it a recognisable name —
        # the tests assert on how many such threads exist (R6).
        raise NotImplementedError

    def register(self, wrapper: "AsyncProgressManagerWrapper") -> None:
        # TODO(13): start painting this wrapper.
        # Related decision: the wrapped manager's start() has to run once
        # somewhere. Doing it on the caller's thread would reintroduce exactly
        # the blocking write this whole module exists to avoid (see the note on
        # the wrapper's start()).
        raise NotImplementedError

    def deregister(self, wrapper: "AsyncProgressManagerWrapper") -> None:
        # TODO(14): stop painting this wrapper. Must be safe if it was never
        # registered, or is already gone.
        raise NotImplementedError

    def is_healthy(self) -> bool:
        """False when the terminal appears wedged."""
        # TODO(15): report whether the renderer is currently stuck.
        # Two states must NOT be confused: a frame that has been in flight too
        # long (wedged) versus no frame running at all (idle) — treating idle as
        # wedged would fire constantly on a healthy but quiet pipeline.
        # This must clear by itself once a frame completes again (I4).
        # Read by close() (I5) and, optionally, by get_progress_manager().
        raise NotImplementedError

    def _run(self) -> None:
        # TODO(16): the render loop. It runs for the life of the process — like
        # the usage poller, there is no shutdown path; being a daemon is what
        # bounds exit.
        # Each pass: paint every registered wrapper once, then wait out the
        # interval. Things to get right:
        #   - the registry is mutated by other threads while you iterate it
        #   - a wrapper's paint may block for a very long time, so think about
        #     what you must not still be holding while it does
        #   - wrappers that have begun closing should be left alone
        #   - record enough around each paint for is_healthy() to work (I4)
        #   - one wrapper raising must not kill the loop or the job; after this
        #     thread dies nothing repaints ever again
        raise NotImplementedError


# TODO(17): expose the process-wide renderer through a module-level accessor,
# created and started on first use. `ray/data/_internal/usage/poller.py` is the
# in-package precedent for this exact shape — read it and follow it, since
# reviewers will recognise the pattern.
# Note the accessor can be called concurrently from multiple executions, and
# exactly one renderer must ever exist (I3).
