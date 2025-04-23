"""ProgressBar implementation for Ray Data.

We use Rich for visualizing progress.
"""

import logging
import sys
import time
import math # Added math import
from typing import Dict, Optional, Tuple, List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.streaming_executor_state import (
    Topology,
    OpState,
    InputDataBuffer,
    format_memory_str, # Import helper
)
from ray.data.context import DataContext

# Try importing rich, handle gracefully if not installed
try:
    import rich
    import rich.progress
    from rich.console import Console
    from rich.live import Live
    from rich.progress import (
        Progress,
        TaskID,
        BarColumn,
        TextColumn,
        TimeRemainingColumn,
        FractionColumn,
        SpinnerColumn, # Added Spinner
    )
    from rich.table import Table
    from rich.text import Text
except ImportError:
    # Define fallbacks for type hinting and conditional logic
    rich = None
    TaskID = None
    Progress = object # Use object to allow isinstance checks
    Live = object
    Table = object
    Text = object
    Console = object
    BarColumn = object
    TextColumn = object
    TimeRemainingColumn = object
    FractionColumn = object
    SpinnerColumn = object

logger = logging.getLogger(__name__)

# --- Constants for formatting ---
TREE_BRANCH = "  ├─ "
TREE_END = "  └─ "
DETAILS_INDENT = "     "
# -----------------------------

# Fallback if rich is not installed
class NoopProgressBar:
    # Keep NoopProgressBar as is
    def __init__(self, *args, **kwargs):
        pass
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def start(self): pass
    def stop(self): pass
    def update(self, task_id: Optional[int], **kwargs): pass
    def add_task(self, description: str, **kwargs) -> Optional[int]: return None
    def update_total_progress(self, *args, **kwargs): pass
    def update_operator_progress(self, op_state: OpState): pass
    def refresh(self): pass

# Helper function for formatting row counts like 3.12k/9.47k
# Moved outside class for general use
def format_row_count(completed: int, total: Optional[int]) -> str:
    """Formats row counts with k units."""

    def format_k(val: int) -> Tuple[float, str]:
        if val >= 1000:
            return val / 1000.0, "k"
        return float(val), ""

    c_val, c_unit = format_k(completed)
    # Format without decimals if it's an integer
    c_disp = f"{int(c_val)}" if c_unit == "k" and c_val.is_integer() else f"{c_val:.2f}"
    if c_unit != "k" : c_disp = f"{int(c_val)}"

    if total is None or math.isinf(total):
        t_disp = "?"
        t_unit = "k" if c_unit == "k" else "" # Match unit if possible
    else:
        t_val, t_unit = format_k(int(total))
        t_disp = f"{int(t_val)}" if t_unit == "k" and t_val.is_integer() else f"{t_val:.2f}"
        if t_unit != "k" : t_disp = f"{int(t_val)}"


    return f"{c_disp}{c_unit}/{t_disp}{t_unit}"

# Custom Rich Columns
if rich:

    class OperatorDetailsColumn(rich.progress.ProgressColumn):
        """Renders the second line of operator details."""
        def render(self, task: "rich.progress.Task") -> Text:
            details = task.fields.get("details", "")
            return Text(f"{DETAILS_INDENT}{details}", style="dim", no_wrap=True)

    # OperatorNameTreeColumn removed - handled directly in TextColumn

    class KiloBytesColumn(rich.progress.ProgressColumn):
        """Renders the file size in kbytes."""
        def render(self, task: "rich.progress.Task") -> Text:
            completed = int(task.completed)
            total = int(task.total) if task.total is not None and not math.isinf(task.total) else 0
            unit = "k"
            total_str = f"{total/1000:.2f}{unit}" if total > 0 else "?k"
            return Text(
                f"{completed/1000:.2f}{unit}/{total_str}",
                style="progress.download",
            )


class RichProgressBarManager:
    """Manages the Rich progress bar display for Data execution."""

    def __init__(self, topology: Topology):
        if not rich or not sys.stdout.isatty():
            logger.info("Rich is not installed or not running in a TTY. Progress bars disabled.")
            self.enabled = False
            # Ensure necessary dummy attributes exist for type checking/attribute access
            self.global_progress = NoopProgressBar()
            self.operator_progress = NoopProgressBar()
            self.global_task_id = None
            self._live = NoopProgressBar()
            return

        self.enabled = True
        self.topology = topology
        self._display_ops: List[OpState] = [
            state for op, state in topology.items() if not isinstance(op, InputDataBuffer)
        ]
        self._num_display_ops = len(self._display_ops)
        self._start_time: Optional[float] = None
        self._console = Console()

        # --- Define Rich Progress Columns --- (Refined for Screenshot)
        self.global_progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description} {task.percentage:>3.0f}%", table_column=rich.table.Column(no_wrap=True)),
            BarColumn(),
            TextColumn("{task.fields[count_str]}", table_column=rich.table.Column(no_wrap=True)),
            TextColumn("["), TimeRemainingColumn(), TextColumn("]"),
            TextColumn("{task.fields[rate]}", table_column=rich.table.Column(no_wrap=True)),
            console=self._console, transient=False, expand=True,
        )
        self._resource_text = Text("| Active/total resources: Initializing...", style="dim", no_wrap=True)
        self.operator_progress = Progress(
            TextColumn("{task.description}", table_column=rich.table.Column(no_wrap=True)), # Tree prefix added here
            BarColumn(bar_width=10),
            TextColumn(" "),
            FractionColumn(unit_scale=True, unit_divisor=1000),
            TextColumn("["), TimeRemainingColumn(), TextColumn("]"),
            TextColumn("{task.fields[rate]}", table_column=rich.table.Column(no_wrap=True)),
            OperatorDetailsColumn(),
            console=self._console, transient=False, expand=True
        )

        # --- Layout --- 
        self._layout_table = Table.grid(padding=(0, 1, 0, 0), expand=True)
        self._layout_table.add_row(self.global_progress)
        self._layout_table.add_row(self._resource_text)
        self._layout_table.add_row(self.operator_progress)
        self._live = Live(self._layout_table, console=self._console, refresh_per_second=4, vertical_overflow="visible")

        # --- Add Tasks ---
        self.global_task_id = self.global_progress.add_task(
            ":Dataset ds running:", total=1.0, rate="? rows/s", count_str="0/?"
        )
        self._add_operator_tasks()

    def _add_operator_tasks(self):
        if not self.enabled: return
        for i, state in enumerate(self._display_ops):
            total = state.op.num_output_rows_total()
            is_last = (i == self._num_display_ops - 1)
            prefix = TREE_END if is_last else TREE_BRANCH
            description = f"{prefix}{state.op.name}"
            task_id = self.operator_progress.add_task(
                description,
                total=float(total) if total is not None else float('inf'),
                start=True, rate="? rows/s", details="Initializing..."
            )
            state.rich_task_id = task_id

    def start(self):
        if self.enabled: self._live.start()

    def stop(self):
        if self.enabled:
            self.refresh()
            time.sleep(0.1)
            self._live.stop()

    def refresh(self):
        if self.enabled: self._live.refresh()

    def update_total_progress(self, total_rows: Optional[int], current_rows: int, total_cpu: float, total_gpu: float, total_obj_store_mem: int, cluster_cpus: Optional[float], cluster_gpus: Optional[float], cluster_obj_store_mem: Optional[int]):
        if not self.enabled or self.global_task_id is None or self.global_task_id not in self.global_progress.task_ids:
            return

        if self._start_time is None: self._start_time = time.time()
        elapsed = time.time() - self._start_time
        rate_val = current_rows / elapsed if elapsed > 1 else 0
        rate_unit = "row/s"
        if rate_val >= 1000: rate_val /= 1000; rate_unit = "k row/s"
        rate_str = f"{rate_val:.2f} {rate_unit}"

        desc = ":Dataset ds running:"
        global_completed = 0.0
        global_total = 1.0
        finished = False
        if total_rows is not None and total_rows > 0:
            global_completed = min(1.0, current_rows / total_rows)
            if current_rows >= total_rows: finished = True
        elif total_rows == 0:
            global_completed = 1.0
            finished = True
        if finished:
            desc = f"✔️ Dataset execution finished"
            rate_str = "-"

        count_str = format_row_count(current_rows, total_rows)
        self.global_progress.update(
            self.global_task_id, description=desc, completed=global_completed,
            total=global_total, fields={"rate": rate_str, "count_str": count_str}
        )

        active_cpu_str = f"{total_cpu:.1f}".replace(".0", "")
        cluster_cpu_str = f"{cluster_cpus:.1f}".replace(".0", "") if cluster_cpus is not None else "?"
        active_mem_str = format_memory_str(total_obj_store_mem)
        cluster_mem_str = format_memory_str(cluster_obj_store_mem) if cluster_obj_store_mem is not None else "?"
        gpu_str = ""
        if total_gpu > 0 or (cluster_gpus is not None and cluster_gpus > 0):
            active_gpu_str = f"{total_gpu:.1f}".replace(".0", "")
            cluster_gpu_str = f"{cluster_gpus:.1f}".replace(".0", "") if cluster_gpus is not None else "?"
            gpu_str = f", {active_gpu_str}/{cluster_gpu_str} GPU"
        resource_line = (
            f"| Active/total resources: {active_cpu_str}/{cluster_cpu_str} CPU"
            f"{gpu_str}, {active_mem_str}/{cluster_mem_str} object store"
        )
        self._resource_text.plain = resource_line

    def update_operator_progress(self, op_state: OpState):
        if not self.enabled or op_state.rich_task_id is None or op_state.rich_task_id not in self.operator_progress.task_ids:
            return

        task_id = op_state.rich_task_id
        completed = float(op_state.last_num_output_rows)
        total = float(op_state.op.num_output_rows_total() or float('inf'))
        details_str = op_state.display_info.format_display_line()

        rate_str = "? rows/s"
        try:
            task = self.operator_progress._tasks[task_id]
            if task.elapsed is not None and task.elapsed > 0:
                rate_val = completed / task.elapsed
                rate_unit = "row/s"
                if rate_val >= 1000: rate_val /= 1000; rate_unit = "k row/s"
                elif rate_val < 1 and rate_val > 0: rate_val = 1 / rate_val; rate_unit = "s/row"
                rate_str = f"{rate_val:.2f} {rate_unit}"
            elif task.finished_time is not None: rate_str = "-"
        except Exception as e:
             logger.warning(f"Error calculating rate for task {task_id}: {e}")

        display_total = total
        if math.isinf(total):
             display_total = None
        elif total == 0: display_total = 1.0

        self.operator_progress.update(
            task_id, completed=completed, total=display_total,
            fields={"rate": rate_str, "details": details_str}
        )
