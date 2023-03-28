import random

import ray
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple, Union

_ResultCallback = Callable[[Any], None]
_ErrorCallback = Callable[[Exception], None]


class RayEventManager:
    """Event manager for Ray futures.

    The event manager can be used to track futures and invoke callbacks when
    they resolve.

    Futures are tracked with :meth:`track_future`. Future can then be awaited with
    :meth:`wait`. When futures successfully resolve, they trigger an optional
    ``on_result`` callback that can be passed to :meth:`track_future`. If they
    fail, they trigger an optional ``on_error`` callback.

    Args:
        shuffle_futures: If True, futures will be shuffled before awaited. This
            will avoid implicit prioritization of futures within Ray.
    """

    def __init__(self, shuffle_futures: bool = True):
        self._shuffle_futures = shuffle_futures

        # Map of futures to callbacks (result, error)
        self._tracked_futures: Dict[
            ray.ObjectRef, Tuple[Optional[_ResultCallback], Optional[_ErrorCallback]]
        ] = {}

    def track_future(
        self,
        future: ray.ObjectRef,
        on_result: Optional[_ResultCallback] = None,
        on_error: Optional[_ErrorCallback] = None,
    ):
        """Track a single future and invoke callbacks on resolution.

        Control has to be yielded to the event manager for the callbacks to
        be invoked, either via :meth:`wait` or via :meth:`resolve_future`.

        Args:
            future: Ray future to await.
            on_result: Callback to invoke when the future resolves successfully.
            on_error: Callback to invoke when the future fails.

        """
        self._tracked_futures[future] = (on_result, on_error)

    def track_futures(
        self,
        futures: Iterable[ray.ObjectRef],
        on_result: Optional[_ResultCallback] = None,
        on_error: Optional[_ErrorCallback] = None,
    ):
        """Track multiple futures and invoke callbacks on resolution.

        Control has to be yielded to the event manager for the callbacks to
        be invoked, either via :meth:`wait` or via :meth:`resolve_future`.

        Args:
            futures: Ray futures to await.
            on_result: Callback to invoke when the future resolves successfully.
            on_error: Callback to invoke when the future fails.

        """
        for future in futures:
            self.track_future(future, on_result=on_result, on_error=on_error)

    def discard_future(self, future: ray.ObjectRef):
        """Remove future from tracking.

        The future will not be awaited anymore, and it will not trigger any callbacks.

        Args:
            future: Ray futures to discard.
        """
        self._tracked_futures.pop(future, None)

    def get_futures(self) -> Set[ray.ObjectRef]:
        """Get futures tracked by the event manager."""
        return set(self._tracked_futures)

    @property
    def num_futures(self) -> int:
        return len(self._tracked_futures)

    def resolve_future(self, future: ray.ObjectRef):
        """Resolve a single future.

        This method will block until the future is available. It will then
        trigger the callback associated to the future and the event (success
        or error), if specified.

        Args:
            future: Ray future to resolve.

        """
        try:
            on_result, on_error = self._tracked_futures.pop(future)
        except KeyError as e:
            raise ValueError(
                f"Future {future} is not tracked by this RayEventManager"
            ) from e

        try:
            result = ray.get(future)
        except Exception as e:
            if on_error:
                on_error(e)
            else:
                raise e
        else:
            if on_result:
                on_result(result)

    def wait(
        self,
        timeout: Optional[Union[float, int]] = None,
        num_results: Optional[int] = 1,
    ):
        """Wait up to ``timeout`` seconds for ``num_results`` futures to resolve.

        If ``timeout=None``, this method will block until all `num_results`` futures
        resolve. If ``num_results=None``, this method will await all tracked futures.

        For every future that resolves, the respective associated callbacks will be
        invoked.

        Args:
            timeout: Timeout in second to wait for futures to resolve.
            num_results: Number of futures to await. If ``None``, will wait for
                all tracked futures to resolve.

        """
        futures = list(self.get_futures())

        if self._shuffle_futures:
            random.shuffle(futures)

        num_results = num_results or len(futures)

        ready, _ = ray.wait(list(futures), timeout=timeout, num_returns=num_results)
        for future in ready:
            self.resolve_future(future)
