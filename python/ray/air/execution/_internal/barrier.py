from typing import Any, Callable, List, Optional, Tuple


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

    """

    def __init__(self, max_results: Optional[int] = None):
        self._max_results = max_results

        self._completed = False
        self._on_completion = None

        self._first_error = False
        self._on_first_error = None

        self._results = []
        self._errors = []

    def arrive(self, *data):
        """Notify barrier that a result successfully arrived.

        This will count against the ``max_results`` limit. The received result
        will be included in a call to :meth:`get_results`.

        Args:
            *data: Result data to be cached. Can be obtained via :meth:`get_results`.

        """
        self._results.append(data)
        self._check_completion()

    def error(self, *data):
        """Notify barrier that an error arrived.

        This will count against the ``max_results`` limit. The received arguments
        will be included in a call to :meth:`get_errors`.

        If ``Barrier.complete_on_first_error`` is set, this will immediately trigger
        the completion callback.

        Args:
            *data: Error data to be cached. Can be obtained via :meth:`get_errors`.
        """
        self._errors.append(data)
        self._check_first_error()
        self._check_completion()

    def _check_first_error(self):
        if self._first_error:
            # Already fired callback
            return

        num_errors = self.num_errors
        if num_errors:
            # First error arrived
            self._first_error = True

            # Invoke callback
            if self._on_first_error:
                self._on_first_error(self)

    def _check_completion(self):
        if self._completed:
            # Already fired completion callback
            return

        num_results = self.num_results
        num_errors = self.num_errors

        if (
            # max_results is set and number of received results exceeds it
            self._max_results is not None
            and num_results + num_errors >= self._max_results
        ):
            # Barrier is complete
            self._completed = True

            if self._on_completion:
                self._on_completion(self)

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
        case, the ``on_completion`` callback will never be invoked.

        Args:
            callback: Callback to invoke when ``max_results`` results and errors
            arrived at the barrier.

        """
        self._on_completion = callback
        return self

    def on_first_error(self, callback: Callable[["Barrier"], None]) -> "Barrier":
        """Define callback to be invoked when the first error arrived.

        When :meth:`error` is called the first time, the first error callback is
        invoked.

        The first error callback should expect one argument, which is the barrier
        object that received the error.

        The first error callback will only be invoked once, even if more errors
        arrive afterwards. The collected results and errors can be flushed
        with :meth:`flush`, after which the callback may be invoked again.

        Args:
            callback: Callback to invoke when meth:`error` was invoked for the
                first time.

        """
        self._on_first_error = callback
        return self

    @property
    def completed(self) -> bool:
        """Returns True if the barrier is completed."""
        return self._completed

    @property
    def has_error(self) -> bool:
        """Returns True if the barrier had an error arrive."""
        return self._first_error

    @property
    def num_results(self) -> int:
        """Number of received (successful) results."""
        return len(self._results)

    @property
    def num_errors(self) -> int:
        """Number of received errors."""
        return len(self._errors)

    def get_results(self) -> List[Tuple[Any]]:
        """Return list of received results."""
        return self._results

    def get_errors(self) -> List[Tuple[Any]]:
        """Return list of received errors."""
        return self._errors

    def flush(self) -> None:
        """Reset barrier, removing all received results and errors.

        This method can be used for a persistent barrier that can receive more
        results than ``max_results``. In that case, the received results can be
        flushed after processing so that new results can be received.

        Flushing the barrier will reset the completion status. When ``max_results``
        is set and enough new events arrive after flushing, the
        :meth:`on_completion` callback will be invoked again.
        """
        self._completed = False
        self._first_error = False
        self._results = []
        self._errors = []
