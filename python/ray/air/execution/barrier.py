from typing import Any, Callable, List, Optional


class Barrier:
    """Barrier to collect results and process them in bulk.

    A barrier can be used to collect multiple results and process them in bulk once
    a certain count or a timeout is reached.

    For instance, if ``max_results=N``, the :meth:`on_completion` callback will be
    invoked once :meth:`arrive` has been called ``N`` times.

    ``max_results`` can be ``None``, in which case an infinite amount of results
    will be collected. In this case, the :meth:`on_completion` callback
    will not be invoked on successful task resolution.

    Args:
        max_results: Maximum number of results to collect before a call to
            :meth:`wait` resolves or the :meth:`on_completion` callback is invoked.
            If ``None``, will collect an infinite number of results.
        complete_on_first_error: If True, will trigger completion once a single error
            is received.

    """

    def __init__(
        self, max_results: Optional[int] = None, complete_on_first_error: bool = False
    ):
        raise NotImplementedError

    def arrive(self, *data):
        """Notify barrier that a result successfully arrived.

        This will count against the ``max_results`` limit. The received result
        will be included in a call to :meth:`get_results`.

        Args:
            *data: Result data to be cached. Can be obtained via :meth:`get_results`.

        """
        raise NotImplementedError

    def error(self, *data):
        """Notify barrier that an error arrived.

        This will count against the ``max_results`` limit. The received arguments
        will be included in a call to :meth:`get_errors`.

        If ``Barrier.complete_on_first_error`` is set, this will immediately trigger
        the completion callback.

        Args:
            *data: Error data to be cached. Can be obtained via :meth:`get_errors`.
        """
        raise NotImplementedError

    def on_completion(self, callback: Callable[["Barrier"], None]) -> "Barrier":
        """Define callback to be invoked when the barrier is full.

        Whenever ``max_results`` results and errors arrived at the barrier,
        the completion callback is invoked.

        The completion callback should expect one argument, which is the barrier
        object that completed.

        The completion callback will only be invoked once, even if more results
        arrive after completion. The collected results and errors can be flushed
        with :meth:`flush`, after which the callback may be invoked again.

        If ``max_results=None``, an infinite number of events are collected. In this
        case, the ``on_completion`` callback will only be invoked if
        ``complete_on_first_error`` is set and an error arrives.

        Args:
            callback: Callback to invoke when ``max_results`` results and errors
            arrived at the barrier.

        """
        raise NotImplementedError

    @property
    def completed(self) -> bool:
        """Returns True if the barrier is completed."""
        raise NotImplementedError

    @property
    def num_results(self) -> int:
        """Number of received (successful) results."""
        raise NotImplementedError

    @property
    def num_errors(self) -> int:
        """Number of received errors."""
        raise NotImplementedError

    def get_results(self) -> List[Any]:
        """Return list of received results."""
        raise NotImplementedError

    def get_errors(self) -> List[Any]:
        """Return list of received errors."""
        raise NotImplementedError

    def flush(self) -> None:
        """Reset barrier, removing all received results and errors.

        This method can be used for a persistent barrier that can receive more
        results than ``max_results``. In that case, the received results can be
        flushed after processing so that new results can be received.

        Flushing the barrier will reset the completion status. When ``max_results``
        is set and enough new events arrive after flushing, the
        :meth:`on_completion` callback will be invoked again.
        """
        raise NotImplementedError
