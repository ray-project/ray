import dataclasses
import logging
import math
import sys
import time
import typing
import uuid
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Column, Table
from rich.text import Text

from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.progress.base_progress import (
    BaseExecutionProgressManager,
    BaseProgressBar,
    NoopSubProgressBar,
)
from ray.data._internal.progress.utils import truncate_operator_name

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology

logger = logging.getLogger(__name__)

_TREE_BRANCH = "  ├─"
_TREE_VERTICAL = "│"
_TREE_VERTICAL_SUB_PROGRESS = "  │     -"
_TREE_VERTICAL_INDENT = f"  {_TREE_VERTICAL}    "
_TOTAL_PROGRESS_TOTAL = 1.0
_RESOURCE_REPORT_HEADER = f"  {_TREE_VERTICAL} Active/total resources: "


class RichSubProgressBar(BaseProgressBar):
    """Thin wrapper to provide identical interface to the ProgressBar.

    Updates RichExecutionProgressManager internally.
    """

    def __init__(
        self,
        name: str,
        total: Optional[int],
        progress: Progress,
        tid: TaskID,
        max_name_length: int = 100,
    ):
        """
        Initialize sub-progress bar

        Args:
            name: name of sub-progress bar
            total: total number of output rows. None for unknown.
            progress: rich.Progress instance for the corresponding
                sub-progress bar.
            tid: rich.TaskId for the corresponding sub-progress bar task.
            max_name_length: maximum operator name length.
        """
        self._enabled = True
        self._total = total
        self._completed = 0
        self._start_time = None
        self._progress = progress
        self._tid = tid
        self._max_name_length = max_name_length
        self._desc = truncate_operator_name(name, self._max_name_length)

    def set_description(self, name: str) -> None:
        self._desc = truncate_operator_name(name, self._max_name_length)
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

    def update(self, new_rows: int = 0, total_rows: Optional[int] = None) -> None:
        if self._enabled and new_rows != 0:
            if total_rows is not None:
                self._total = total_rows
            self._completed += new_rows
            self._update(self._completed, self._total)

    def complete(self) -> None:
        if self._enabled:
            self._update(self._completed, self._completed)

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self._enabled = False  # Progress bar is disabled on remote nodes.


class RichExecutionProgressManager(BaseExecutionProgressManager):
    """Execution progress display using rich."""

    TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS = 20

    def __init__(self, dataset_id: str, topology: "Topology", show_op_progress: bool):
        self._dataset_id = dataset_id
        self._show_op_progress = show_op_progress
        self._sub_progress_bars: List[BaseProgressBar] = []

        self._start_time: Optional[float] = None

        # rich elements
        self._console = Console(file=sys.stderr)
        self._total = self._make_progress_bar(" ", "•", 15)
        self._current_rows = 0
        self._total_resources = Text(
            f"{_RESOURCE_REPORT_HEADER}Initializing...", no_wrap=True
        )

        self._op_display: Dict[uuid.UUID, Tuple[TaskID, Progress, Text]] = {}

        self._layout_table = Table.grid(padding=(0, 1, 0, 0), expand=True)
        self._layout_table.add_row(self._total)
        self._layout_table.add_row(self._total_resources)

        self._setup_operator_progress(topology)

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

    def _make_progress_bar(
        self, indent_str: str, spinner_finish: str, bar_width: int
    ) -> Progress:
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

    def _setup_operator_progress(self, topology: "Topology"):
        if self._show_op_progress:
            self._layout_table.add_row(Text(f"  {_TREE_VERTICAL}", no_wrap=True))
        for state in topology.values():
            op = state.op
            if isinstance(op, InputDataBuffer):
                continue
            if self._show_op_progress:
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
            if not isinstance(op, SubProgressBarMixin):
                continue
            op.initialize_sub_progress_related()
            sub_pg_names = op.get_sub_progress_bar_names()
            if sub_pg_names is None:
                continue
            for name in sub_pg_names:
                pg = None
                if self._show_op_progress:
                    progress = self._make_progress_bar(
                        _TREE_VERTICAL_SUB_PROGRESS, "", 10
                    )
                    total = state.op.num_output_rows_total()
                    tid = progress.add_task(
                        "",
                        total=total if total is not None else 1,
                        start=True,
                        rate_str="? rows/s",
                        count_str="0/?",
                    )
                    self._layout_table.add_row(progress)
                    pg = RichSubProgressBar(
                        name=name, total=total, progress=progress, tid=tid
                    )
                else:
                    pg = NoopSubProgressBar(
                        name=name, max_name_length=self.MAX_NAME_LENGTH
                    )
                op.set_sub_progress_bar(name, pg)
                self._sub_progress_bars.append(pg)

    # Management
    def start(self):
        if not self._live.is_started:
            self._live.start()

    def refresh(self):
        if self._live.is_started:
            self._live.refresh()

    def close_with_finishing_description(self, desc: str, success: bool):
        if self._live.is_started:
            kwargs = {}
            if success:
                # set everything to completed
                kwargs["completed"] = 1.0
                kwargs["total"] = 1.0
                for pg in self._sub_progress_bars:
                    if isinstance(pg, RichSubProgressBar):
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

    # Total Progress
    def _can_update_total(self) -> bool:
        return (
            self._total_task_id is not None
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

    def update_total_resource_status(self, resource_status: str):
        if not self._can_update_total():
            return
        if self._live.is_started:
            self._total_resources.plain = _RESOURCE_REPORT_HEADER + resource_status

    # Operator Progress
    def _can_update_operator(self, op_state: "OpState") -> bool:
        if not self._show_op_progress:
            return False
        uid = op_state.progress_manager_uuid
        if uid is None or uid not in self._op_display:
            return False
        tid, progress, stats = self._op_display[uid]
        if tid is None or not progress or not stats or tid not in progress.task_ids:
            return False
        return True

    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ):
        if not self._can_update_operator(opstate):
            return
        if self._start_time is None:
            self._start_time = time.time()
        uid = opstate.progress_manager_uuid
        tid, progress, stats = self._op_display[uid]

        # progress
        current_rows = opstate.output_row_count
        total_rows = opstate.op.num_output_rows_total()
        metrics = _get_progress_metrics(self._start_time, current_rows, total_rows)
        _update_with_conditional_rate(progress, tid, metrics)

        # stats
        summary_str = opstate.summary_str_raw(resource_manager)
        stats.plain = f"{_TREE_VERTICAL_INDENT}{summary_str}"


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


@dataclasses.dataclass
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


def _update_with_conditional_rate(
    progress: "Progress", tid: "TaskID", metrics: "_ProgressMetrics"
):
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
