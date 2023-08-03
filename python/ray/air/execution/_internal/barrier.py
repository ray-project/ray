from typing import Any, Callable, List, Optional, Tuple


class Barrier:
    """Barrier to collect results and process them in bulk.

    A barrier can be used to collect multiple results and process them in bulk once
    a certain count or a timeout is reached.

    For instance, if ``max_results=N``, the ``on_completion`` callback will be
    invoked once :meth:`arrive` has been called ``N`` times.

    The completion callback will only be invoked once, even if more results
    arrive after completion. The collected results can be resetted
    with :meth:`reset`, after which the callback may be invoked again.

    The completion callback should expect one argument, which is the barrier
    object that completed.

    Args:
        max_results: Maximum number of results to collect before a call to
            :meth:`wait` resolves or the :meth:`on_completion` callback is invoked.
        on_completion: Callback to invoke when ``max_results`` results
            arrived at the barrier.

    """

    def __init__(
        self,
        max_results: int,
        *,
        on_completion: Optional[Callable[["Barrier"], None]] = None,
    ):
        self._max_results = max_results

        # on_completion callback
        self._completed = False
        self._on_completion = on_completion

        # Collect received results
        self._results: List[Tuple[Any]] = []

    def arrive(self, *data):
        """Notify barrier that a result successfully arrived.

        This will count against the ``max_results`` limit. The received result
        will be included in a call to :meth:`get_results`.

        Args:
            *data: Result data to be cached. Can be obtained via :meth:`get_results`.

        """
        if len(data) == 1:
            data = data[0]

        self._results.append(data)
        self._check_completion()

    def _check_completion(self):
        if self._completed:
            # Already fired completion callback
            return

        if self.num_results >= self._max_results:
            # Barrier is complete
            self._completed = True

            if self._on_completion:
                self._on_completion(self)

    @property
    def completed(self) -> bool:
        """Returns True if the barrier is completed."""
        return self._completed

    @property
    def num_results(self) -> int:
        """Number of received (successful) results."""
        return len(self._results)

    def get_results(self) -> List[Tuple[Any]]:
        """Return list of received results."""
        return self._results

    def reset(self) -> None:
        """Reset barrier, removing all received results.

        Resetting the barrier will reset the completion status. When ``max_results``
        is set and enough new events arrive after resetting, the
        :meth:`on_completion` callback will be invoked again.
        """
        self._completed = False
        self._results = []
