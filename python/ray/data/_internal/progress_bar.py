import threading
from typing import Any, List, Optional

import ray
from ray.experimental import tqdm_ray
from ray.types import ObjectRef
from ray.util.annotations import Deprecated

try:
    import tqdm

    needs_warning = False
except ImportError:
    tqdm = None
    needs_warning = True

# Used a signal to cancel execution.
_canceled_threads = set()
_canceled_threads_lock = threading.Lock()


@Deprecated
def set_progress_bars(enabled: bool) -> bool:
    """Set whether progress bars are enabled.

    The default behavior is controlled by the
    ``RAY_DATA_DISABLE_PROGRESS_BARS`` environment variable. By default,
    it is set to "0". Setting it to "1" will disable progress bars, unless
    they are reenabled by this method.

    Returns:
        Whether progress bars were previously enabled.
    """
    raise DeprecationWarning(
        "`set_progress_bars` is deprecated. Set "
        "`ray.data.DataContext.get_current().enable_progress_bars` instead.",
    )


class ProgressBar:
    """Thin wrapper around tqdm to handle soft imports.

    If `total` is `None` known (for example, it is unknown
    because no tasks have finished yet), doesn't display the full
    progress bar. Still displays basic progress stats from tqdm."""

    def __init__(
        self,
        name: str,
        total: Optional[int],
        unit: str,
        position: int = 0,
        enabled: Optional[bool] = None,
    ):
        self._desc = name
        self._progress = 0
        # Prepend a space to the unit for better formatting.
        if unit[0] != " ":
            unit = " " + unit

        if enabled is None:
            from ray.data import DataContext

            enabled = DataContext.get_current().enable_progress_bars
        if not enabled:
            self._bar = None
        elif tqdm:
            ctx = ray.data.context.DataContext.get_current()
            if ctx.use_ray_tqdm:
                self._bar = tqdm_ray.tqdm(total=total, unit=unit, position=position)
            else:
                self._bar = tqdm.tqdm(
                    total=total,
                    position=position,
                    dynamic_ncols=True,
                    unit=unit,
                )
            self._bar.set_description(self._desc)
        else:
            global needs_warning
            if needs_warning:
                print("[dataset]: Run `pip install tqdm` to enable progress reporting.")
                needs_warning = False
            self._bar = None

    def block_until_complete(self, remaining: List[ObjectRef]) -> None:
        t = threading.current_thread()
        while remaining:
            done, remaining = ray.wait(remaining, fetch_local=False, timeout=0.1)
            self.update(len(done))

            with _canceled_threads_lock:
                if t in _canceled_threads:
                    break

    def fetch_until_complete(self, refs: List[ObjectRef]) -> List[Any]:
        ref_to_result = {}
        remaining = refs
        t = threading.current_thread()
        # Triggering fetch_local redundantly for the same object is slower.
        # We only need to trigger the fetch_local once for each object,
        # raylet will persist these fetch requests even after ray.wait returns.
        # See https://github.com/ray-project/ray/issues/30375.
        fetch_local = True
        while remaining:
            done, remaining = ray.wait(remaining, fetch_local=fetch_local, timeout=0.1)
            if fetch_local:
                fetch_local = False
            for ref, result in zip(done, ray.get(done)):
                ref_to_result[ref] = result
            self.update(len(done))

            with _canceled_threads_lock:
                if t in _canceled_threads:
                    break

        return [ref_to_result[ref] for ref in refs]

    def set_description(self, name: str) -> None:
        if self._bar and name != self._desc:
            self._desc = name
            self._bar.set_description(self._desc)

    def update(self, i: int = 0, total: Optional[int] = None) -> None:
        if self._bar and (i != 0 or self._bar.total != total):
            self._progress += i
            if total is not None:
                self._bar.total = total
            if self._bar.total is not None and self._progress > self._bar.total:
                # If the progress goes over 100%, update the total.
                self._bar.total = self._progress
            self._bar.update(i)

    def close(self):
        if self._bar:
            if self._bar.total is not None and self._progress != self._bar.total:
                # If the progress is not complete, update the total.
                self._bar.total = self._progress
                self._bar.refresh()
            self._bar.close()
            self._bar = None

    def __del__(self):
        self.close()

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self._bar = None  # Progress bar is disabled on remote nodes.
