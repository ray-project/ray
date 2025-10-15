import logging
import math
import sys
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional

from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.streaming_executor_state import OpState, Topology
from ray.data._internal.progress_bar import AbstractProgressBar, truncate_operator_name
from ray.util.debug import log_once

try:
    import rich
    from rich.console import Console
    from rich.live import Live
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
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
_TREE_VERTICAL_SUB_PROGRESS = "  │     -"
_TREE_VERTICAL_INDENT = f"  {_TREE_VERTICAL}    "
_TOTAL_PROGRESS_TOTAL = 1.0
_RESOURCE_REPORT_HEADER = f"  {_TREE_VERTICAL} Active/total resources: "


class _ManagerMode(str, Enum):
    NONE = "NONE"  # no-op
    GLOBAL_ONLY = "GLOBAL_ONLY"  # global progress
    ALL = "ALL"  # show everything

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


class SubProgressBar(AbstractProgressBar):
    """Thin wrapper to provide identical interface to the ProgressBar.

    Updates RichExecutionProgressManager internally.
    """

    # If the name/description of the progress bar exceeds this length,
    # it will be truncated.
    MAX_NAME_LENGTH = 100

    def __init__(
        self,
        name: str,
        total: Optional[int] = None,
        enabled: bool = True,
        progress: Optional[Any] = None,
        tid: Optional[Any] = None,
    ):
        """
        Initialize sub-progress bar

        Args:
            name: name of sub-progress bar
            total: total number of output rows. None for unknown.
            enabled: whether progress bar is enabled.
            progress: rich.Progress instance for the corresponding
                sub-progress bar.
            tid: rich.TaskId for the corresponding sub-progress bar task.
        """
        # progress, tid type Optional[Any] due to conditional rich import.
        if enabled:
            assert progress is not None and tid is not None
        else:
            progress = None
            tid = None
        self._total = total
        self._completed = 0
        self._start_time = None
        self._enabled = enabled
        self._progress = progress
        self._tid = tid
        self._desc = truncate_operator_name(name, self.MAX_NAME_LENGTH)

    def set_description(self, name: str) -> None:
        self._desc = truncate_operator_name(name, self.MAX_NAME_LENGTH)
        if self._enabled:
            self._progress.update(self._tid, description=self._desc)

    def get_description(self) -> str:
        return self._desc

    def _update(self, completed: int, total: Optional[int] = None) -> None:
        assert self._enabled
        if self._start_time is None:
            self._start_time = time.time()
        metrics = _get_progress_metrics(self._start_time, completed, total)
        self._progress.update(
            self._tid,
            completed=metrics.completed,
            total=metrics.total,
            rate_str=metrics.rate_str,
            count_str=metrics.count_str,
        )

    def update(self, increment: int = 0, total: Optional[int] = None) -> None:
        if self._enabled and increment != 0:
            if total is not None:
                self._total = total
            self._completed += increment
            self._update(self._completed, self._total)

    def complete(self) -> None:
        if self._enabled:
            self._update(self._completed, self._completed)

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self.enabled = False  # Progress bar is disabled on remote nodes.


class RichExecutionProgressManager:
    """Execution progress display using rich."""

    # If the name/description of the progress bar exceeds this length,
    # it will be truncated.
    MAX_NAME_LENGTH = 100

    def __init__(self, dataset_id: str, topology: Topology):
        self._mode = _ManagerMode.get_mode()
        self._dataset_id = dataset_id
        self._sub_progress_bars: List[SubProgressBar] = []

        if not self._mode.is_enabled():
            self._live = None
            # TODO (kyuds): for sub-progress, initialize no-op
            for state in topology.values():
                if _has_sub_progress_bars(state.op):
                    self._setup_operator_sub_progress(state)
            return

        self._start_time: Optional[float] = None

        # rich
        self._console = Console(file=sys.stderr)
        self._total = self._make_progress_bar(" ", "•", 15)
        self._current_rows = 0
        self._total_resources = Text(
            f"{_RESOURCE_REPORT_HEADER}Initializing...", no_wrap=True
        )

        self._op_display = {}

        self._layout_table = Table.grid(padding=(0, 1, 0, 0), expand=True)
        self._layout_table.add_row(self._total)
        self._layout_table.add_row(self._total_resources)

        self._setup_progress_grid(topology)

        # empty new line to prevent "packed" feeling
        self._layout_table.add_row(Text())

        # rich.Live is the auto-refreshing rich component display.
        # refreshing/closing is all done through rich.Live
        self._live = Live(
            self._layout_table,
            console=self._console,
            refresh_per_second=2,
            vertical_overflow="visible",
        )

        self._total_task_id = self._total.add_task(
            f"Dataset {self._dataset_id} running:",
            total=_TOTAL_PROGRESS_TOTAL,
            rate_str="? rows/s",
            count_str="0/?",
        )

    def _setup_progress_grid(self, topology: Topology):
        if self._mode.show_op():
            self._layout_table.add_row(Text(f"  {_TREE_VERTICAL}", no_wrap=True))
        for state in topology.values():
            if isinstance(state.op, InputDataBuffer):
                continue
            if self._mode.show_op():
                uid = uuid.uuid4()
                progress = self._make_progress_bar(_TREE_BRANCH, " ", 10)
                stats = Text(f"{_TREE_VERTICAL_INDENT}Initializing...", no_wrap=True)
                total = state.op.num_output_rows_total()
                name = truncate_operator_name(state.op.name, self.MAX_NAME_LENGTH)
                tid = progress.add_task(
                    name,
                    total=total if total is not None else 1,
                    start=True,
                    rate_str="? rows/s",
                    count_str="0/?",
                )
                self._layout_table.add_row(progress)
                self._layout_table.add_row(stats)
                state.progress_manager_uuid = uid
                self._op_display[uid] = (tid, progress, stats)

            if _has_sub_progress_bars(state.op):
                self._setup_operator_sub_progress(state)

    def _setup_operator_sub_progress(self, state: OpState):
        assert _has_sub_progress_bars(
            state.op
        ), f"Operator {state.op.name} doesn't support sub-progress bars."
        enabled = self._mode.show_op()

        sub_progress_bar_names = state.op.get_sub_progress_bar_names()
        if sub_progress_bar_names is not None:
            for name in sub_progress_bar_names:
                name = truncate_operator_name(name, SubProgressBar.MAX_NAME_LENGTH)
                progress = None
                tid = None
                total = None

                if enabled:
                    progress = self._make_progress_bar(
                        _TREE_VERTICAL_SUB_PROGRESS, "", 10
                    )
                    total = state.op.num_output_rows_total()
                    tid = progress.add_task(
                        name,
                        total=total if total is not None else 1,
                        start=True,
                        rate_str="? rows/s",
                        count_str="0/?",
                    )
                    self._layout_table.add_row(progress)

                pg = SubProgressBar(
                    name=name,
                    total=total,
                    enabled=enabled,
                    progress=progress,
                    tid=tid,
                )
                state.op.set_sub_progress_bar(name, pg)
                self._sub_progress_bars.append(pg)

    def _make_progress_bar(self, indent_str, spinner_finish, bar_width):
        # no type hints because rich import is conditional.
        assert self._mode.is_enabled()
        return Progress(
            TextColumn(indent_str, table_column=Column(no_wrap=True)),
            SpinnerColumn(finished_text=spinner_finish),
            TextColumn(
                "{task.description} {task.percentage:>3.0f}%",
                table_column=Column(no_wrap=True),
            ),
            BarColumn(bar_width=bar_width),
            TextColumn("{task.fields[count_str]}", table_column=Column(no_wrap=True)),
            TextColumn("["),
            TimeElapsedColumn(),
            TextColumn(","),
            TextColumn("{task.fields[rate_str]}", table_column=Column(no_wrap=True)),
            TextColumn("]"),
            console=self._console,
            transient=False,
            expand=False,
        )

    # Management
    def start(self):
        if self._mode.is_enabled():
            if not self._live.is_started:
                self._live.start()

    def refresh(self):
        if self._mode.is_enabled():
            if self._live.is_started:
                self._live.refresh()

    def close_with_finishing_description(self, desc: str, success: bool):
        if self._mode.is_enabled():
            if self._live.is_started:
                kwargs = {}
                if success:
                    # set everything to completed
                    kwargs["completed"] = 1.0
                    kwargs["total"] = 1.0
                    for pg in self._sub_progress_bars:
                        pg.complete()
                    for tid, progress, _ in self._op_display.values():
                        completed = progress.tasks[tid].completed or 0
                        metrics = _get_progress_metrics(
                            self._start_time, completed, completed
                        )
                        _update_with_conditional_rate(progress, tid, metrics)
                self._total.update(self._total_task_id, description=desc, **kwargs)
                self.refresh()
                time.sleep(0.02)
                self._live.stop()
                logger.info(desc)

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
        if self._live.is_started:
            if self._start_time is None:
                self._start_time = time.time()
            if new_rows is not None:
                self._current_rows += new_rows
            metrics = _get_progress_metrics(
                self._start_time, self._current_rows, total_rows
            )
            _update_with_conditional_rate(self._total, self._total_task_id, metrics)

    def update_resource_status(self, resource_status: str):
        if not self._can_update_total():
            return
        if self._live.is_started:
            self._total_resources.plain = _RESOURCE_REPORT_HEADER + resource_status

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
        if self._start_time is None:
            self._start_time = time.time()
        uid = op_state.progress_manager_uuid
        tid, progress, stats = self._op_display[uid]

        # progress
        current_rows = op_state.output_row_count
        total_rows = op_state.op.num_output_rows_total()
        metrics = _get_progress_metrics(self._start_time, current_rows, total_rows)
        _update_with_conditional_rate(progress, tid, metrics)
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


@dataclass
class _ProgressMetrics:
    completed: int
    total: int
    rate_str: str
    count_str: str


def _get_progress_metrics(
    start_time: float, completed_rows: int, total_rows: Optional[int]
) -> _ProgressMetrics:
    """
    Args:
        start_time: time when progress tracking started
        completed_rows: cumulative rows outputted
        total_rows: total rows expected (can be unknown)
    Returns:
        _ProgressMetrics instance containing the calculated data.
    """
    # Note, when total is unknown, we default the progress bar to 0.
    # Will properly have estimates for rate and count strings though.
    total = 1 if total_rows is None or total_rows < 1 else total_rows
    completed = 0 if total_rows is None else completed_rows

    if total_rows is None:
        rate_str = "? row/s"
    else:
        elapsed = time.time() - start_time
        rate_val = completed_rows / elapsed if elapsed > 1 else 0
        rate_unit = "row/s"
        if rate_val >= 1000:
            rate_val /= 1000
            rate_unit = "k row/s"
        rate_str = f"{rate_val:.2f} {rate_unit}"
    count_str = _format_row_count(completed_rows, total_rows)

    return _ProgressMetrics(
        completed=completed, total=total, rate_str=rate_str, count_str=count_str
    )


def _has_sub_progress_bars(op: PhysicalOperator) -> bool:
    """Determines if operator implements sub-progress bars

    Args:
        op: Operator
    Returns:
        whether operator implements sub-progress bars
    """
    # function primarily used to avoid circular imports
    from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin

    return isinstance(op, SubProgressBarMixin)


def _update_with_conditional_rate(progress, tid, metrics):
    # not doing type checking because rich is imported conditionally.
    # progress: rich.Progress
    # tid: rich.TaskId
    # metrics: _ProgressMetrics
    task = progress.tasks[tid]
    kwargs = {
        "completed": metrics.completed,
        "total": metrics.total,
        "count_str": metrics.count_str,
    }
    if task.completed != metrics.completed:
        # update rate string only if there are new rows.
        # this allows updates to other metric data while
        # preserving the right rate notation.
        kwargs["rate_str"] = metrics.rate_str
    progress.update(tid, **kwargs)
