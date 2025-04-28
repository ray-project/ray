import logging
import threading
from typing import Any, List, Optional

import ray
from ray.experimental import tqdm_ray
from ray.types import ObjectRef
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

try:
    import tqdm

    needs_warning = False
except ImportError:
    tqdm = None
    needs_warning = True

# Used a signal to cancel execution.
_canceled_threads = set()
_canceled_threads_lock = threading.Lock()


def extract_num_rows(result: Any) -> int:
    """Extract the number of rows from a result object.

    Args:
        result: The result object from which to extract the number of rows.

    Returns:
        The number of rows, defaulting to 1 if it cannot be determined.
    """
    if hasattr(result, "num_rows"):
        return result.num_rows
    elif hasattr(result, "__len__"):
        # For output is DataFrame,i.e. sort_sample
        return len(result)
    else:
        return 1


class ProgressBar:
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
        self._desc = self._truncate_name(name)
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

    def _truncate_name(self, name: str) -> str:
        ctx = ray.data.context.DataContext.get_current()
        if (
            not ctx.enable_progress_bar_name_truncation
            or len(name) <= self.MAX_NAME_LENGTH
        ):
            return name

        op_names = name.split("->")
        if len(op_names) == 1:
            return op_names[0]

        # Include as many operators as possible without approximately
        # exceeding `MAX_NAME_LENGTH`. Always include the first and
        # last operator names so it is easy to identify the DAG.
        truncated_op_names = [op_names[0]]
        for op_name in op_names[1:-1]:
            if (
                len("->".join(truncated_op_names))
                + len("->")
                + len(op_name)
                + len("->")
                + len(op_names[-1])
            ) > self.MAX_NAME_LENGTH:
                truncated_op_names.append("...")
                if log_once("ray_data_truncate_operator_name"):
                    logger.warning(
                        f"Truncating long operator name to {self.MAX_NAME_LENGTH} "
                        "characters. To disable this behavior, set "
                        "`ray.data.DataContext.get_current()."
                        "DEFAULT_ENABLE_PROGRESS_BAR_NAME_TRUNCATION = False`."
                    )
                break
            truncated_op_names.append(op_name)
        truncated_op_names.append(op_names[-1])
        return "->".join(truncated_op_names)

    def block_until_complete(self, remaining: List[ObjectRef]) -> None:
        t = threading.current_thread()
        while remaining:
            done, remaining = ray.wait(
                remaining, num_returns=len(remaining), fetch_local=False, timeout=0.1
            )
            total_rows_processed = 0
            for _, result in zip(done, ray.get(done)):
                num_rows = extract_num_rows(result)
                total_rows_processed += num_rows
            self.update(total_rows_processed)

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
            done, remaining = ray.wait(
                remaining,
                num_returns=len(remaining),
                fetch_local=fetch_local,
                timeout=0.1,
            )
            if fetch_local:
                fetch_local = False
            total_rows_processed = 0
            for ref, result in zip(done, ray.get(done)):
                ref_to_result[ref] = result
                num_rows = extract_num_rows(result)
                total_rows_processed += num_rows
            self.update(total_rows_processed)

            with _canceled_threads_lock:
                if t in _canceled_threads:
                    break

        return [ref_to_result[ref] for ref in refs]

    def set_description(self, name: str) -> None:
        name = self._truncate_name(name)
        if self._bar and name != self._desc:
            self._desc = name
            self._bar.set_description(self._desc)

    def get_description(self) -> str:
        return self._desc

    def refresh(self):
        if self._bar:
            self._bar.refresh()

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
