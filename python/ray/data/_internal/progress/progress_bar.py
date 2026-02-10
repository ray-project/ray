from typing import Optional

from ray.data._internal.progress.base_progress import BaseProgressBar
from ray.data._internal.progress.utils import truncate_operator_name
from ray.experimental import tqdm_ray

try:
    import tqdm

    needs_warning = False
except ImportError:
    tqdm = None
    needs_warning = True


class ProgressBar(BaseProgressBar):
    """Thin wrapper around tqdm to handle soft imports.

    If `total` is `None` known (for example, it is unknown
    because no tasks have finished yet), doesn't display the full
    progress bar. Still displays basic progress stats from tqdm."""

    # If the name/description of the progress bar exceeds this length,
    # it will be truncated.
    MAX_NAME_LENGTH = 100

    def __init__(
        self,
        name: str,
        total: Optional[int],
        unit: str,
        position: int = 0,
        enabled: Optional[bool] = None,
    ):
        from ray.data.context import DataContext

        self._desc = truncate_operator_name(name, self.MAX_NAME_LENGTH)
        self._progress = 0
        # Prepend a space to the unit for better formatting.
        if unit[0] != " ":
            unit = " " + unit

        if enabled is None:
            # When enabled is None (not explicitly set by the user),
            # check DataContext setting
            enabled = DataContext.get_current().enable_progress_bars

        use_ray_tqdm = DataContext.get_current().use_ray_tqdm

        if not enabled:
            self._bar = None
        elif use_ray_tqdm:
            self._bar = tqdm_ray.tqdm(total=total, unit=unit, position=position)
            self._bar.set_description(self._desc)
        elif tqdm:
            self._bar = tqdm.tqdm(
                total=total or 0,
                position=position,
                dynamic_ncols=True,
                unit=unit,
                unit_scale=True,
            )
            self._bar.set_description(self._desc)
        else:
            global needs_warning
            if needs_warning:
                print("[dataset]: Run `pip install tqdm` to enable progress reporting.")
                needs_warning = False
            self._bar = None

    def set_description(self, name: str) -> None:
        name = truncate_operator_name(name, self.MAX_NAME_LENGTH)
        if self._bar and name != self._desc:
            self._desc = name
            self._bar.set_description(self._desc)

    def get_description(self) -> str:
        return self._desc

    def refresh(self):
        if self._bar:
            self._bar.refresh()

    def update(self, increment: int = 0, total: Optional[int] = None) -> None:
        if self._bar and (increment != 0 or self._bar.total != total):
            self._progress += increment
            if total is not None:
                self._bar.total = total
            if self._bar.total is not None and self._progress > self._bar.total:
                # If the progress goes over 100%, update the total.
                self._bar.total = self._progress
            self._bar.update(increment)

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
