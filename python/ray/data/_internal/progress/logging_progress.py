import dataclasses
import logging
import time
import typing
import uuid
from typing import Dict, List, Optional, Union

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

# Progress manager to be used for non-tty terminals, preventing spamming
# of progress reporting. Refer to following issues:
# https://github.com/ray-project/ray/issues/60083
# https://github.com/ray-project/ray/issues/57734

LOG_REPORT_INTERVAL_SEC = 10

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class _LoggingMetrics:
    name: str
    desc: Optional[str]
    completed: int
    total: Optional[int]


class LoggingSubProgressBar(BaseProgressBar):
    """Thin wrapper to provide identical interface to the ProgressBar.

    Logs progress to terminal internally.
    """

    def __init__(
        self,
        name: str,
        total: Optional[int] = None,
        max_name_length: int = 100,
    ):
        """
        Initialize sub-progress bar

        Args:
            name: name of sub-progress bar
            total: total number of output rows. None for unknown.
            max_name_length: maximum operator name length (unused).
        """
        del max_name_length  # unused
        self._total = total
        self._completed = 0
        self._name = name

    def set_description(self, name: str) -> None:
        pass  # unused

    def get_description(self) -> str:
        return ""  # unused

    def update(self, increment: int = 0, total: Optional[int] = None):
        if total is not None:
            self._total = total
        self._completed += increment

    def get_data(self) -> _LoggingMetrics:
        return _LoggingMetrics(
            name=f"    - {self._name}",
            desc=None,
            completed=self._completed,
            total=self._total,
        )


class LoggingExecutionProgressManager(BaseExecutionProgressManager):
    """Execution progress display for non-tty situations."""

    # If the name/description of the progress bar exceeds this length,
    # it will be truncated.
    MAX_NAME_LENGTH = 100

    # This progress manager needs to refresh (log) based on elapsed time
    # not scheduling steps. This elapsed time handling is done within
    # this class.
    TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS = 1

    def __init__(
        self,
        dataset_id: str,
        topology: "Topology",
        show_op_progress: bool,
        verbose_progress: bool,
    ):
        self._dataset_id = dataset_id
        self._last_log_time = time.time() - LOG_REPORT_INTERVAL_SEC
        self._log_order: List[uuid.UUID] = []
        self._metric_dict: Dict[
            uuid.UUID, Union[LoggingSubProgressBar, _LoggingMetrics]
        ] = {}

        self._global_progress_uuid = uuid.uuid4()
        self._metric_dict[self._global_progress_uuid] = _LoggingMetrics(
            name="Total Progress", desc=None, completed=0, total=None
        )

        for state in topology.values():
            op = state.op
            if isinstance(op, InputDataBuffer):
                continue
            total = op.num_output_rows_total() or 1

            contains_sub_progress_bars = isinstance(op, SubProgressBarMixin)
            sub_progress_bar_enabled = show_op_progress and (
                contains_sub_progress_bars or verbose_progress
            )

            if sub_progress_bar_enabled:
                op_uuid = uuid.uuid4()
                self._log_order.append(op_uuid)
                self._metric_dict[op_uuid] = _LoggingMetrics(
                    name=truncate_operator_name(op.name, self.MAX_NAME_LENGTH),
                    desc=None,
                    completed=0,
                    total=total,
                )
                state.progress_manager_uuid = op_uuid

            if not contains_sub_progress_bars:
                continue

            sub_pg_names = op.get_sub_progress_bar_names()
            if sub_pg_names is None:
                continue
            for name in sub_pg_names:
                if sub_progress_bar_enabled:
                    sub_uuid = uuid.uuid4()
                    pg = LoggingSubProgressBar(
                        name=name, total=total, max_name_length=self.MAX_NAME_LENGTH
                    )
                    self._log_order.append(sub_uuid)
                    self._metric_dict[sub_uuid] = pg
                else:
                    pg = NoopSubProgressBar(
                        name=name, max_name_length=self.MAX_NAME_LENGTH
                    )
                op.set_sub_progress_bar(name, pg)

    # Management
    def start(self):
        # logging progress manager doesn't need separate start
        pass

    def refresh(self):
        current_time = time.time()
        if current_time - self._last_log_time < LOG_REPORT_INTERVAL_SEC:
            return
        self._last_log_time = current_time

        # starting delimiter
        msg = f"======= Running Dataset: {self._dataset_id} ======="
        lastline = "=" * len(msg)
        logger.info(msg)
        m = self._get_as_logging_metrics(self._global_progress_uuid)
        logger.info(f"{m.name}: {m.completed}/{m.total or '?'}")
        if m.desc is not None:
            logger.info(m.desc)
        if len(self._log_order) > 0:
            logger.info("")

        for uid in self._log_order:
            m = self._metric_dict[uid]
            if isinstance(m, LoggingSubProgressBar):
                m = m.get_data()
            logger.info(f"{m.name}: {m.completed}/{m.total or '?'}")
            if m.desc is not None:
                logger.info(f"  {m.desc}")

        # finish logging
        logger.info(lastline)

    def close_with_finishing_description(self, desc: str, success: bool):
        # We log in StreamingExecutor. No need for duplicate logging.
        pass

    # Total Progress
    def update_total_progress(self, new_rows: int, total_rows: Optional[int]):
        total_metrics = self._get_as_logging_metrics(self._global_progress_uuid)
        if total_rows is not None:
            total_metrics.total = total_rows
        total_metrics.completed += new_rows

    def update_total_resource_status(self, resource_status: str):
        total_metrics = self._get_as_logging_metrics(self._global_progress_uuid)
        total_metrics.desc = resource_status

    # Operator Progress
    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ):
        op_metrics = self._get_as_logging_metrics(opstate.progress_manager_uuid)
        op_metrics.completed = opstate.output_row_count
        total = opstate.op.num_output_rows_total()
        if total is not None:
            op_metrics.total = total
        op_metrics.desc = opstate.summary_str_raw(resource_manager)

    # Utilities
    def _get_as_logging_metrics(self, uid: uuid.UUID) -> _LoggingMetrics:
        metrics = self._metric_dict[uid]
        assert isinstance(metrics, _LoggingMetrics)
        return metrics
