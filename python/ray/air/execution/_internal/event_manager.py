import random

import ray
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple, Union

_ResultCallback = Callable[[Any], None]
_ErrorCallback = Callable[[Exception], None]


class RayEventManager:
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
        self._tracked_futures[future] = (on_result, on_error)

    def track_futures(
        self,
        futures: Iterable[ray.ObjectRef],
        on_result: Optional[_ResultCallback],
        on_error: Optional[_ErrorCallback],
    ):
        for future in futures:
            self.track_future(future, on_result=on_result, on_error=on_error)

    def discard_future(self, future: ray.ObjectRef):
        self._tracked_futures.pop(future, None)

    def get_futures(self) -> Set[ray.ObjectRef]:
        return set(self._tracked_futures)

    def resolve_future(self, future: ray.ObjectRef):
        try:
            on_result, on_error = self._tracked_futures.pop(future)
        except KeyError as e:
            raise ValueError(
                f"Future {future} is not tracked by this RayEventManager"
            ) from e

        try:
            result = ray.get(future)
        except Exception as e:
            on_error(e)
        else:
            on_result(result)

    def wait(
        self,
        timeout: Optional[Union[float, int]] = None,
        num_results: Optional[int] = None,
    ):
        futures = list(self.get_futures())

        if self._shuffle_futures:
            random.shuffle(futures)

        num_results = num_results or len(futures)

        ready, _ = ray.wait(list(futures), timeout=timeout, num_returns=num_results)
        for future in ready:
            self.resolve_future(future)
