import logging
import sys
import time
from typing import Optional

import ray._private.worker
from ray.data._internal.progress.base_progress import BaseProgressBar
from ray.data._internal.progress.utils import truncate_operator_name
from ray.experimental import tqdm_ray
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

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
        self._desc = truncate_operator_name(name, self.MAX_NAME_LENGTH)
        self._progress = 0
        self._total = total
        # Prepend a space to the unit for better formatting.
        if unit[0] != " ":
            unit = " " + unit

        self._use_logging = False

        from ray.data.context import DataContext

        if enabled is None:
            # When enabled is None (not explicitly set by the user),
            # check DataContext setting
            enabled = DataContext.get_current().enable_progress_bars

        use_ray_tqdm = DataContext.get_current().use_ray_tqdm

        # If enabled and in non-interactive terminal, use logging instead of tqdm.
        # Exception: tqdm_ray works in Ray workers by sending JSON to driver.
        worker = ray._private.worker
        in_ray_worker = worker.global_worker.mode == worker.WORKER_MODE
        if enabled and not sys.stdout.isatty() and not (use_ray_tqdm and in_ray_worker):
            self._use_logging = True
            enabled = False
            if log_once("progress_bar_disabled"):
                logger.info(
                    "Progress bar disabled because stdout is a non-interactive terminal."
                )

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

        # For logging progress in non-interactive terminals
        self._last_logged_time = 0
        # Log interval in seconds
        from ray.data.context import DataContext

        self._log_interval = DataContext.get_current().progress_bar_log_interval
        self._logged_once = False

    def set_description(self, name: str) -> None:
        name = truncate_operator_name(name, self.MAX_NAME_LENGTH)
        if self._use_logging:
            self._desc = name
        if self._bar and name != self._desc:
            self._desc = name
            self._bar.set_description(self._desc)

    def get_description(self) -> str:
        return self._desc

    def _log_progress_if_needed(self):
        """Log progress if the required time interval has passed."""
        current_time = time.time()
        time_diff = current_time - self._last_logged_time
        should_log = (self._last_logged_time == 0) or (time_diff >= self._log_interval)

        if should_log:
            # Remove leading hyphens from the description
            clean_desc = self._desc.lstrip("- ").strip()
            if not self._logged_once:
                operation_name = clean_desc.split(":")[0]
                logger.info(f"=== Ray Data Progress {{{operation_name}}} ===")
                self._logged_once = True
            logger.info(
                f"{clean_desc}: Progress Completed {self._progress} / {self._total or '?'}"
            )
            self._last_logged_time = current_time

    def refresh(self):
        if self._bar:
            self._bar.refresh()
        elif self._use_logging:
            # Log progress periodically
            self._log_progress_if_needed()

    def update(self, increment: int = 0, total: Optional[int] = None) -> None:
        if self._bar and (increment != 0 or self._bar.total != total):
            self._progress += increment
            if total is not None:
                self._bar.total = total
            if self._bar.total is not None and self._progress > self._bar.total:
                # If the progress goes over 100%, update the total.
                self._bar.total = self._progress
            self._bar.update(increment)
        elif self._use_logging and (increment is not None and increment != 0):
            self._progress += increment
            if total is not None:
                self._total = total

            # Log progress periodically
            self._log_progress_if_needed()

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
