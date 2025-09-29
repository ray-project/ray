import logging
import math
import threading
import time
import uuid
from enum import Enum
from typing import Optional, Tuple

from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import OpState, Topology
from ray.util.debug import log_once

try:
    import rich
    from rich.console import Console
    from rich.live import Live
    from rich.progress import (
        BarColumn,
        Progress,
        ProgressColumn,
        SpinnerColumn,
        TextColumn,
    )
    from rich.table import Column, Table
    from rich.text import Text

    needs_rich_warning = False
except ImportError:
    rich = None
    needs_rich_warning = True

logger = logging.getLogger(__name__)

_TREE_BRANCH = "  ├─"
_TREE_VERTICAL = "│"
_TREE_VERTICAL_INDENT = f"  {_TREE_VERTICAL}    "
_TOTAL_PROGRESS_TOTAL = 1.0
_RESOURCE_REPORT_HEADER = f"  {_TREE_VERTICAL} Active/total resources: "


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


if rich:

    class CustomTimeColumn(ProgressColumn):
        """A column that shows elapsed<remaining time like tqdm."""

        def render(self, task):
            """Show time in format: elapsed<remaining"""
            elapsed = task.elapsed
            if elapsed is None:
                return Text("--:--<--:--", style="progress.remaining")

            # Format elapsed time
            elapsed_str = self._format_time(elapsed)

            # Calculate remaining time
            if task.speed and task.remaining:
                remaining = task.remaining / task.speed if task.speed > 0 else 0
                remaining_str = self._format_time(remaining)
            else:
                remaining_str = "--:--"

            return Text(f"{elapsed_str}<{remaining_str}", style="progress.remaining")

        def _format_time(self, seconds):
            """Format seconds into MM:SS or HH:MM:SS format."""
            if seconds is None or seconds < 0:
                return "--:--"

            hours, remainder = divmod(int(seconds), 3600)
            minutes, seconds = divmod(remainder, 60)

            if hours > 0:
                return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            else:
                return f"{minutes:02d}:{seconds:02d}"


class RichExecutionProgressManager:
    """Execution progress display using rich."""

    def __init__(self, dataset_id: str, topology: Topology):
        self._mode = _ManagerMode.get_mode()
        self._dataset_id = dataset_id
        self._lock = None

        if not self._mode.is_enabled():
            self._live = None
            return

        self._start_time: Optional[float] = None
        self._lock = threading.RLock()

        # rich
        self._console = Console()
        self._total = Progress(
            TextColumn(" ", table_column=Column(no_wrap=True)),
            SpinnerColumn(finished_text="•"),
            TextColumn(
                "{task.description} {task.percentage:>3.0f}%",
                table_column=Column(no_wrap=True),
            ),
            BarColumn(bar_width=15),
            TextColumn("{task.fields[count_str]}", table_column=Column(no_wrap=True)),
            TextColumn("["),
            CustomTimeColumn(),
            TextColumn(","),
            TextColumn("{task.fields[rate]}", table_column=Column(no_wrap=True)),
            TextColumn("]"),
            console=self._console,
            transient=False,
            expand=False,
        )
        self._current_rows = 0
        self._total_resources = Text(
            f"{_RESOURCE_REPORT_HEADER}Initializing...", no_wrap=True
        )

        self._op_display = {}

        self._layout_table = Table.grid(padding=(0, 1, 0, 0), expand=True)
        self._layout_table.add_row(self._total)
        self._layout_table.add_row(self._total_resources)
        self._layout_table.add_row(Text(f"  {_TREE_VERTICAL}", no_wrap=True))

        for state in topology.values():
            if isinstance(state.op, InputDataBuffer):
                continue
            uid = uuid.uuid4()
            progress = Progress(
                TextColumn(_TREE_BRANCH, table_column=Column(no_wrap=True)),
                SpinnerColumn(),
                TextColumn("{task.description}", table_column=Column(no_wrap=True)),
                BarColumn(bar_width=10),
                TextColumn(
                    "{task.fields[count_str]}", table_column=Column(no_wrap=True)
                ),
                TextColumn("["),
                CustomTimeColumn(),
                TextColumn(","),
                TextColumn("{task.fields[rate]}", table_column=Column(no_wrap=True)),
                TextColumn("]"),
                console=self._console,
                transient=False,
                expand=False,
            )
            stats = Text(f"{_TREE_VERTICAL_INDENT}Initializing...", no_wrap=True)
            total = state.op.num_output_rows_total()
            tid = progress.add_task(
                state.op.name,
                total=float(total) if total is not None else float("inf"),
                start=True,
                rate="? rows/s",
                count_str="?/?",
            )
            self._layout_table.add_row(progress)
            self._layout_table.add_row(stats)
            state.progress_manager_uuid = uid
            self._op_display[uid] = (tid, progress, stats)
        # empty new line to prevent "packed" feeling
        self._layout_table.add_row(Text())
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

    # Management
    def start(self):
        if self._mode.is_enabled():
            with self._lock:
                if not self._live.is_started:
                    self._live.start()

    def refresh(self):
        if self._mode.is_enabled():
            with self._lock:
                if self._live.is_started:
                    self._live.refresh()

    def _close_no_lock(self):
        self.refresh()
        time.sleep(0.02)
        self._live.stop()

    def close(self):
        if self._mode.is_enabled():
            with self._lock:
                if self._live.is_started:
                    self._close_no_lock()

    def close_with_finishing_description(self, desc: str, success: bool):
        if self._mode.is_enabled():
            with self._lock:
                if self._live.is_started:
                    kwargs = {}
                    if success:
                        kwargs["completed"] = 1.0
                        kwargs["total"] = 1.0
                    self._total.update(self._total_task_id, description=desc, **kwargs)
                    self._close_no_lock()

    # Total Progress
    def _can_update_total(self) -> bool:
        return (
            self._mode.is_enabled()
            and self._total_task_id is not None
            and self._total_task_id in self._total.task_ids
        )

    def update_total_progress(self, new_rows: int, total_rows: Optional[int]):
        if not self._can_update_total():
            return
        with self._lock:
            if self._live.is_started:
                self._update_total_progress_no_lock(new_rows, total_rows)

    def _update_total_progress_no_lock(self, new_rows: int, total_rows: Optional[int]):
        if self._start_time is None:
            self._start_time = time.time()
        if new_rows is not None:
            self._current_rows += new_rows
        c, t, rs, cs = _get_progress_metrics(
            self._start_time, self._current_rows, total_rows
        )
        self._total.update(
            self._total_task_id,
            completed=c,
            total=t,
            rate=rs,
            count_str=cs,
        )

    def update_resource_status(self, resource_manager: ResourceManager):
        if not self._can_update_total():
            return
        with self._lock:
            if self._live.is_started:
                self._update_resource_status_no_lock(resource_manager)

    def _update_resource_status_no_lock(self, resource_manager: ResourceManager):
        # running_usage is the amount of resources that have been requested but
        # not necessarily available
        # TODO(sofian) https://github.com/ray-project/ray/issues/47520
        # We need to split the reported resources into running, pending-scheduling,
        # pending-node-assignment.
        running_usage = resource_manager.get_global_running_usage()
        pending_usage = resource_manager.get_global_pending_usage()
        limits = resource_manager.get_global_limits()

        resource_usage = _RESOURCE_REPORT_HEADER
        resource_usage += f"{running_usage.cpu:.4g}/{limits.cpu:.4g} CPU, "
        if running_usage.gpu > 0:
            resource_usage += f"{running_usage.gpu:.4g}/{limits.gpu:.4g} GPU, "
        resource_usage += (
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
            resource_usage += f" (pending: {pending_str})"

        self._total_resources.plain = resource_usage

    def _can_update_operator(self, op_state: OpState) -> bool:
        if not self._mode.show_op():
            return False
        uid = op_state.progress_manager_uuid
        if uid is None or uid not in self._op_display:
            return False
        tid, progress, stats = self._op_display[uid]
        if tid is None or not progress or not stats or tid not in progress.task_ids:
            return False
        return True

    def update_operator_progress(self, op_state: OpState):
        if not self._can_update_operator(op_state):
            return
        with self._lock:
            self._update_operator_progress_no_lock(op_state)

    def _update_operator_progress_no_lock(self, op_state: OpState):
        if self._start_time is None:
            self._start_time = time.time()
        uid = op_state.progress_manager_uuid
        tid, progress, stats = self._op_display[uid]

        # progress
        current_rows = op_state.output_row_count
        total_rows = op_state.op.num_output_rows_total()
        c, t, rs, cs = _get_progress_metrics(self._start_time, current_rows, total_rows)
        progress.update(
            tid,
            completed=c,
            total=t,
            rate=rs,
            count_str=cs,
        )
        # stats
        stats_str = op_state.op_display_metrics.display_str()
        stats.plain = f"{_TREE_VERTICAL_INDENT}{stats_str}"


# utilities
def _format_k(val: int) -> str:
    if val >= 1000:
        fval = val / 1000.0
        fval_str = f"{int(fval)}" if fval.is_integer() else f"{fval:.2f}"
        return fval_str + "k"
    return str(val)


def _format_row_count(completed: int, total: Optional[int]) -> str:
    """Formats row counts with k units."""
    cstr = _format_k(completed)
    if total is None or math.isinf(total):
        tstr = "?k" if cstr.endswith("k") else "?"
    else:
        tstr = _format_k(total)
    return f"{cstr}/{tstr}"


def _get_progress_metrics(
    start_time: float, current_rows: int, total_rows: Optional[int]
) -> Tuple[int, int, str, str]:
    """
    Args:
        start_time: time when progress tracking started
        current_rows: current rows outputted (cumulative)
        total_rows: total rows expected (can be unknown)
    Returns:
        completed (int)
        total (int)
        rate (str)
        count (str)
    """
    total = 1 if total_rows is None or total_rows < 1 else total_rows

    if total_rows is None:
        rate_str = "? row/s"
        count_str = "?/?"
    else:
        elapsed = time.time() - start_time
        rate_val = current_rows / elapsed if elapsed > 1 else 0
        rate_unit = "row/s"
        if rate_val >= 1000:
            rate_val /= 1000
            rate_unit = "k row/s"
        rate_str = f"{rate_val:.2f} {rate_unit}"
        count_str = _format_row_count(current_rows, total_rows)

    return current_rows, total, rate_str, count_str
