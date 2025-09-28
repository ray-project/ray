import logging
import math
import time
from enum import Enum
from typing import List, Optional, Tuple

from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import Topology, OpState
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.util.debug import log_once

try:
    import rich
    from rich.console import Console
    from rich.live import Live
    from rich.progress import (
        Progress,
        TaskID,
        BarColumn,
        TextColumn,
        TimeRemainingColumn,
        FractionColumn,
        SpinnerColumn,
    )
    from rich.table import Table, Column
    from rich.text import Text

    needs_rich_warning = False
except ImportError:
    rich = None
    needs_rich_warning = True

logger = logging.getLogger(__name__)

_TREE_BRANCH = "  ├─ "
_TREE_VERTICAL = "  │  "
_TOTAL_PROGRESS_TOTAL = 1.0
_RESOURCE_REPORT_HEADER = f"{_TREE_VERTICAL} Active/total resources: "


class _ManagerMode(Enum):
    NONE = 1  # no-op
    GLOBAL_ONLY = 2  # global progress
    ALL = 3  # show everything

    def show_op(self) -> bool:
        return self == self.ALL

    def is_enabled(self) -> bool:
        return self != self.NONE

    @classmethod
    def get_mode(cls) -> "_ManagerMode":
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        if not ctx.enable_progress_bars:
            if log_once("ray_data_progress_manager_disabled"):
                logger.warning(
                    "Progress bars disabled. To enable, set "
                    "`ray.data.DataContext.get_current()."
                    "enable_progress_bars = True`."
                )
            return cls.NONE
        elif rich is None:
            global needs_rich_warning
            if needs_rich_warning:
                print(
                    "[dataset]: Run `pip install rich` to enable "
                    "execution progress reporting."
                )
                needs_rich_warning = False
            return cls.NONE
        elif not ctx.enable_operator_progress_bars:
            if log_once("ray_data_progress_manager_global"):
                logger.warning(
                    "Progress bars for operators disabled. To enable, "
                    "set `ray.data.DataContext.get_current()."
                    "enable_operator_progress_bars = True`."
                )
            return cls.GLOBAL_ONLY
        else:
            return cls.ALL


def _format_row_count(completed: int, total: Optional[int]) -> str:
    """Formats row counts with k units."""

    def format_k(val: int) -> str:
        if val >= 1000:
            fval = val / 1000.0
            fval_str = f"{int(fval)}" if fval.is_integer() else f"{fval:.2f}"
            return fval_str + "k"
        return str(val)

    cstr = format_k(completed)
    if total is None or math.isinf(total):
        tstr = "?k" if cstr.endswith("k") else "?"
    else:
        tstr = format_k(total)
    return f"{cstr}/{tstr}"


class RichExecutionProgressManager:
    """Execution progress display using rich."""

    def __init__(self, dataset_id: str, topology: Topology):
        self._mode = _ManagerMode.get_mode()
        self._dataset_id = dataset_id

        if self._mode.is_enabled():
            self._start_time: Optional[float] = None

            # rich
            self._console = Console()
            self._total = Progress(
                SpinnerColumn(finished_text=""),
                TextColumn(
                    "{task.description} {task.percentage:>3.0f}%",
                    table_column=Column(no_wrap=True),
                ),
                BarColumn(),
                TextColumn(
                    "{task.fields[count_str]}", table_column=Column(no_wrap=True)
                ),
                TextColumn("["),
                TimeRemainingColumn(),
                TextColumn("]"),
                TextColumn("{task.fields[rate]}", table_column=Column(no_wrap=True)),
                console=self._console,
                transient=False,
                expand=True,
            )
            self._total_resources = Text(
                f"{_RESOURCE_REPORT_HEADER}Initializing...", no_wrap=True
            )
            # TODO (kyuds): op rows

            self._layout_table = Table.grid(padding=(0, 1, 0, 0), expand=True)
            self._layout_table.add_row(self._total)
            self._layout_table.add_row(self._total_resources)
            self._live = Live(
                self._layout_table,
                console=self._console,
                refresh_per_second=2,
                vertical_overflow="visible",
            )

            self._total_task_id = self._total.add_task(
                f"Dataset {self._dataset_id} running:",
                total=_TOTAL_PROGRESS_TOTAL,
                rate="? rows/s",
                count_str="0/?",
            )
        else:
            self._live = None

    # Management
    def start(self):
        if self._mode.is_enabled():
            self._live.start()

    def refresh(self):
        if self._mode.is_enabled():
            self._live.refresh()

    def close(self):
        if self._mode.is_enabled():
            self.refresh()
            time.sleep(0.1)
            self._live.stop()

    # Total Progress
    def _can_update_total(self) -> bool:
        return (
            self._mode.is_enabled()
            and self._total_task_id is not None
            and self._total_task_id in self._total.task_ids
        )

    def update_total_progress(self, total_rows: Optional[int], current_rows: int):
        if not self._can_update_total():
            return

        # Progress Report
        if self._start_time is None:
            self._start_time = time.time()

        elapsed = time.time() - self._start_time
        rate_val = current_rows / elapsed if elapsed > 1 else 0
        rate_unit = "row/s"
        if rate_val >= 1000:
            rate_val /= 1000
            rate_unit = "k row/s"
        rate_str = f"{rate_val:.2f} {rate_unit}"

        completed = 0.0
        if total_rows is not None and total_rows > 0:
            completed = min(1.0, current_rows / total_rows)

        count_str = _format_row_count(current_rows, total_rows)
        self._total.update(
            self._total_task_id,
            completed=completed,
            total=_TOTAL_PROGRESS_TOTAL,
            fields={"rate": rate_str, "count_str": count_str},
        )

    def update_resource_status(self, resource_manager: ResourceManager):
        # running_usage is the amount of resources that have been requested but
        # not necessarily available
        # TODO(sofian) https://github.com/ray-project/ray/issues/47520
        # We need to split the reported resources into running, pending-scheduling,
        # pending-node-assignment.
        if not self._can_update_total():
            return

        running_usage = resource_manager.get_global_running_usage()
        pending_usage = resource_manager.get_global_pending_usage()
        limits = resource_manager.get_global_limits()

        resource_usage = _RESOURCE_REPORT_HEADER
        resource_usage += f"{running_usage.cpu:.4g}/{limits.cpu:.4g} CPU, "
        if running_usage.gpu > 0:
            resources_status += f"{running_usage.gpu:.4g}/{limits.gpu:.4g} GPU, "
        resources_status += (
            f"{running_usage.object_store_memory_str()}/"
            f"{limits.object_store_memory_str()} object store"
        )

        # Only include pending section when there are pending resources.
        if pending_usage.cpu or pending_usage.gpu:
            pending = []
            if pending_usage.cpu:
                pending.append(f"{pending_usage.cpu:.4g} CPU")
            if pending_usage.gpu:
                pending.append(f"{pending_usage.gpu:.4g} GPU")
            pending_str = ", ".join(pending)
            resources_status += f" (pending: {pending_str})"

        self._total_resources.plain = resources_status

    def set_finishing_message(self, desc: str):
        if not self._can_update_total():
            return

        self._total.update(self._total_task_id, description=desc)
        self._total.stop_task(self._total_task_id)

    # Op Progress
