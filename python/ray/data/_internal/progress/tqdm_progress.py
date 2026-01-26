import logging
import typing
from typing import Dict, List, Optional

from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.execution.streaming_executor_state import (
    format_op_state_summary,
)
from ray.data._internal.progress.base_progress import (
    BaseExecutionProgressManager,
    BaseProgressBar,
    NoopSubProgressBar,
)
from ray.data._internal.progress.progress_bar import ProgressBar

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology

logger = logging.getLogger(__name__)


class TqdmSubProgressBar(ProgressBar):
    """Thin wrapper to provide helper interface for TqdmExecutionProgressManager"""

    def __init__(
        self,
        name: str,
        total: Optional[int],
        unit: str,
        position: int = 0,
        enabled: Optional[bool] = None,
        max_name_length: int = 100,
    ):
        # patch to make max_name_length configurable from ProgressManager.
        self.MAX_NAME_LENGTH = max_name_length
        super().__init__(name, total, unit, position, enabled)

    def update_absolute(self, completed: int, total_rows: Optional[int] = None) -> None:
        if self._bar:
            self._progress = completed
            if total_rows is not None:
                self._bar.total = total_rows
            if self._bar.total is not None and self._progress > self._bar.total:
                # If the progress goes over 100%, update the total.
                self._bar.total = self._progress
            self._bar.n = self._progress


class TqdmExecutionProgressManager(BaseExecutionProgressManager):
    """Execution progress display using tqdm."""

    def __init__(
        self,
        dataset_id: str,
        topology: "Topology",
        show_op_progress: bool,
        verbose_progress: bool,
    ):
        self._dataset_id = dataset_id

        self._sub_progress_bars: List[BaseProgressBar] = []
        self._op_display: Dict["OpState", TqdmSubProgressBar] = {}

        num_progress_bars = 0

        self._total = TqdmSubProgressBar(
            name=f"Running Dataset {self._dataset_id}.",
            total=None,
            unit="row",
            position=num_progress_bars,
            max_name_length=self.MAX_NAME_LENGTH,
            enabled=True,
        )
        num_progress_bars += 1

        for state in topology.values():
            op = state.op
            if isinstance(op, InputDataBuffer):
                continue
            total = op.num_output_rows_total() or 1

            contains_sub_progress_bars = isinstance(op, SubProgressBarMixin)
            sub_progress_bar_enabled = show_op_progress and (
                contains_sub_progress_bars or verbose_progress
            )

            # create operator progress bar
            if sub_progress_bar_enabled:
                pg = TqdmSubProgressBar(
                    name=f"- {op.name}",
                    total=total,
                    unit="row",
                    position=num_progress_bars,
                    max_name_length=self.MAX_NAME_LENGTH,
                )
                num_progress_bars += 1
                self._op_display[state] = pg
                self._sub_progress_bars.append(pg)

            if not contains_sub_progress_bars:
                continue

            sub_pg_names = op.get_sub_progress_bar_names()
            if sub_pg_names is None:
                continue
            for name in sub_pg_names:
                if sub_progress_bar_enabled:
                    pg = TqdmSubProgressBar(
                        name=f"  *- {name}",
                        total=total,
                        unit="row",
                        position=num_progress_bars,
                        max_name_length=self.MAX_NAME_LENGTH,
                        enabled=True,
                    )
                    num_progress_bars += 1
                else:
                    pg = NoopSubProgressBar(
                        name=f"  *- {name}",
                        max_name_length=self.MAX_NAME_LENGTH,
                    )
                op.set_sub_progress_bar(name, pg)
                self._sub_progress_bars.append(pg)

    # Management
    def start(self):
        # tqdm is automatically started
        pass

    def refresh(self):
        self._total.refresh()
        for pg in self._sub_progress_bars:
            pg.refresh()

    def close_with_finishing_description(self, desc: str, success: bool):
        del success  # unused
        self._total.set_description(desc)
        self._total.close()
        for pg in self._sub_progress_bars:
            pg.close()

    # Total Progress
    def update_total_progress(self, new_rows: int, total_rows: Optional[int]):
        self._total.update(new_rows, total_rows)

    def update_total_resource_status(self, resource_status: str):
        desc = f"Running Dataset: {self._dataset_id}. {resource_status}"
        self._total.set_description(desc)

    # Operator Progress
    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ):
        pg = self._op_display.get(opstate)
        if pg is not None:
            pg.update_absolute(
                opstate.op.metrics.row_outputs_taken, opstate.op.num_output_rows_total()
            )
            summary_str = format_op_state_summary(opstate, resource_manager)
            pg.set_description(f"- {opstate.op.name}: {summary_str}")
