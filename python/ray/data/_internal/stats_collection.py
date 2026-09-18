import logging
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, List, Optional

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.stats import DatasetStatsSummary

logger = logging.getLogger(__name__)

# The list that executions report into, or ``None`` when no block is active. At most
# one block is active per process, so a plain global is enough: `collect_stats_summaries`
# rejects a second block rather than trying to decide which one an execution belongs to.
_lock = threading.Lock()
_active_summaries: Optional[List["DatasetStatsSummary"]] = None


@DeveloperAPI
@contextmanager
def collect_stats_summaries() -> Iterator[List["DatasetStatsSummary"]]:
    """Collect the stats of every execution that finishes inside the block.

    ``Dataset.stats()`` is only reachable through a ``Dataset`` object, so a chained
    expression that ends in a write has no handle to its own stats. This collects them
    out-of-band instead, leaving the expression unchanged.

    Both successful and failed executions are recorded. The list is appended to as
    executions finish and stays readable after the block exits; executions still
    running when the block exits aren't recorded, so read the list only once the work
    inside the block has completed.

    At most one block can be active per process. Entering a second one -- nested, or
    concurrently from another thread -- raises rather than silently mixing or dropping
    the two blocks' executions.

    Examples:

        >>> import ray
        >>> from ray.data import collect_stats_summaries
        >>> with collect_stats_summaries() as summaries: # doctest: +SKIP
        ...     ray.data.read_parquet(src).map_batches(fn).write_parquet(dst)
        >>> summaries[0].time_total_s # doctest: +SKIP
        12.3

    Yields:
        List[DatasetStatsSummary]: One
        :class:`~ray.data._internal.stats.DatasetStatsSummary` per execution, in
        completion order.

    Raises:
        RuntimeError: If another block is already active in this process.
    """
    global _active_summaries

    summaries: List["DatasetStatsSummary"] = []
    with _lock:
        if _active_summaries is not None:
            raise RuntimeError(
                "collect_stats_summaries() is already active in this process. Blocks "
                "can't be nested or used concurrently from multiple threads."
            )
        _active_summaries = summaries
    try:
        yield summaries
    finally:
        with _lock:
            _active_summaries = None


def report_stats_summary(summary: "DatasetStatsSummary") -> None:
    """Record a finished execution, if a block is active.

    Never raises: stats collection must not be able to fail a user's execution.
    """
    try:
        with _lock:
            if _active_summaries is not None:
                _active_summaries.append(summary)
    except Exception:
        logger.exception("Failed to record execution stats summary.")
