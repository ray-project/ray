import dataclasses
import logging
import time
import typing
from collections import defaultdict
from typing import Callable, Dict, List, Optional

from ray._private.ray_constants import env_integer
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
from ray.data._internal.progress.utils import truncate_operator_name

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class _LoggingMetrics:
    name: str
    desc: Optional[str]
    completed: int
    total: Optional[int]


class LoggingSubProgressBar(BaseProgressBar):
    """Thin wrapper to provide identical interface to the ProgressBar.

    Internally passes relevant logging metrics to `LoggingExecutionProgressManager`.
    Sub-progress is actually handled by Ray through operators, while operator-level
    and total progress is handled by the `StreamingExecutor`. To ensure log-order,
    this class helps to pass metric data to the progress manager so progress metrics
    are logged centrally.
    """

    def __init__(
        self,
        name: str,
        total: Optional[int] = None,
        max_name_length: int = 100,
    ):
        """Initialize sub-progress bar

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

    def get_logging_metrics(self) -> _LoggingMetrics:
        return _LoggingMetrics(
            name=f"    - {self._name}",
            desc=None,
            completed=self._completed,
            total=self._total,
        )


class LoggingExecutionProgressManager(BaseExecutionProgressManager):
    """Execution progress display for non-tty situations, preventing
    spamming of progress reporting."""

    # Refer to following issues for more context about this feature:
    # https://github.com/ray-project/ray/issues/60083
    # https://github.com/ray-project/ray/issues/57734

    # This progress manager needs to refresh (log) based on elapsed time
    # not scheduling steps. This elapsed time handling is done within
    # this class.
    TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS = 1

    # Time interval (seconds) in which progress is logged to console again.
    LOG_REPORT_INTERVAL_SEC = env_integer("RAY_DATA_NON_TTY_PROGRESS_LOG_INTERVAL", 10)

    def __init__(
        self,
        dataset_id: str,
        topology: "Topology",
        show_op_progress: bool,
        verbose_progress: bool,
        *,
        _get_time: Callable[[], float] = time.time,
    ):
        self._dataset_id = dataset_id
        self._topology = topology
        self._get_time = _get_time
        self._last_log_time = self._get_time() - self.LOG_REPORT_INTERVAL_SEC

        self._global_progress_metric = _LoggingMetrics(
            name="Total Progress", desc=None, completed=0, total=None
        )
        self._op_progress_metrics: Dict["OpState", _LoggingMetrics] = {}
        self._sub_progress_metrics: Dict[
            "OpState", List[LoggingSubProgressBar]
        ] = defaultdict(list)

        for state in self._topology.values():
            op = state.op
            if isinstance(op, InputDataBuffer):
                continue
            total = op.num_output_rows_total() or 1

            contains_sub_progress_bars = isinstance(op, SubProgressBarMixin)
            sub_progress_bar_enabled = show_op_progress and (
                contains_sub_progress_bars or verbose_progress
            )

            if sub_progress_bar_enabled:
                self._op_progress_metrics[state] = _LoggingMetrics(
                    name=truncate_operator_name(op.name, self.MAX_NAME_LENGTH),
                    desc=None,
                    completed=0,
                    total=total,
                )

            if not contains_sub_progress_bars:
                continue

            sub_pg_names = op.get_sub_progress_bar_names()
            if sub_pg_names is None:
                continue
            for name in sub_pg_names:
                if sub_progress_bar_enabled:
                    pg = LoggingSubProgressBar(
                        name=name, total=total, max_name_length=self.MAX_NAME_LENGTH
                    )
                    self._sub_progress_metrics[state].append(pg)
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
        current_time = self._get_time()
        if current_time - self._last_log_time < self.LOG_REPORT_INTERVAL_SEC:
            return
        self._last_log_time = current_time

        # starting delimiter
        firstline = f"======= Running Dataset: {self._dataset_id} ======="
        lastline = "=" * len(firstline)
        logger.info(firstline)

        # log global progress
        _log_global_progress(self._global_progress_metric)

        # log operator-level progress
        if len(self._op_progress_metrics.keys()) > 0:
            logger.info("")

        for opstate in self._topology.values():
            metrics = self._op_progress_metrics.get(opstate)
            if metrics is None:
                continue
            _log_op_or_sub_progress(metrics)
            for pg in self._sub_progress_metrics[opstate]:
                _log_op_or_sub_progress(pg.get_logging_metrics())

        # finish logging
        logger.info(lastline)

    def close_with_finishing_description(self, desc: str, success: bool):
        # We log in StreamingExecutor. No need for duplicate logging.
        pass

    # Total Progress
    def update_total_progress(self, new_rows: int, total_rows: Optional[int]):
        if total_rows is not None:
            self._global_progress_metric.total = total_rows
        self._global_progress_metric.completed += new_rows

    def update_total_resource_status(self, resource_status: str):
        self._global_progress_metric.desc = resource_status

    # Operator Progress
    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ):
        op_metrics = self._op_progress_metrics.get(opstate)
        if op_metrics is not None:
            op_metrics.completed = opstate.op.metrics.row_outputs_taken
            total = opstate.op.num_output_rows_total()
            if total is not None:
                op_metrics.total = total
            op_metrics.desc = format_op_state_summary(opstate, resource_manager)


def _format_progress(m: _LoggingMetrics) -> str:
    return f"{m.name}: {m.completed}/{m.total or '?'}"


def _log_global_progress(m: _LoggingMetrics):
    logger.info(_format_progress(m))
    if m.desc is not None:
        logger.info(m.desc)


def _log_op_or_sub_progress(m: _LoggingMetrics):
    logger.info(_format_progress(m))
    if m.desc is not None:
        logger.info(f"  {m.desc}")
