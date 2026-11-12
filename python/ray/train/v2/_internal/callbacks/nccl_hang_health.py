"""Runs the NCCL hang detector as a ``ray.train.health`` check.

With ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1``, the trainer registers
``NCCLHangHealthCallback``. The ``TrainController`` adds its ``health_check()``
to the run's in-flight checks, and the callback gives the check's evaluators
the run's workers, its storage for the diagnostics, and the detector's Grafana
metrics. The detection itself is in ``ray.train.health.evaluators.nccl_ras``.
"""
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

from ray._private.ray_constants import env_float
from ray.train.health.check import PeriodicHealthCheck
from ray.train.health.evaluators.nccl_ras import (
    NCCLHangDetectorState,
    NCCLHangRun,
    WorkerInfo,
    nccl_hang_check,
)
from ray.train.v2._internal.constants import (
    DEFAULT_NCCL_RAS_ACTION,
    DEFAULT_NCCL_RAS_CONFIRM_DURATION_S,
    DEFAULT_NCCL_RAS_MIN_POLL_INTERVAL_S,
    HANG_DETECTOR_DIRNAME,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.train.v2._internal.execution.worker_group import WorkerGroup
from ray.train.v2._internal.metrics.base import (
    RUN_ID_TAG_KEY,
    RUN_NAME_TAG_KEY,
    EnumMetric,
    Metric,
    ValueMetric,
)

# Grafana dashboard metric names, the same as NCCLRASCallback's.
GRAFANA_STATE = "train_nccl_hang_detector_state"
GRAFANA_STALL_DURATION_S = "train_nccl_hang_stall_duration_s"
GRAFANA_STATE_TAG_KEY = "ray_train_nccl_hang_detector_state"


class NCCLHangHealthCallback(WorkerGroupCallback, ControllerCallback, NCCLHangRun):
    """Detects NCCL hangs with a ``ray.train.health`` check, and gives the
    check the run's workers, storage and metrics.

    Default-off: the trainer only registers this callback when
    ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1`` is set on the driver.
    ``RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S`` sets how often NCCL RAS is
    queried, ``RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S`` how long a communicator
    stays frozen before its hang is confirmed, and
    ``RAY_TRAIN_NCCL_RAS_ACTION`` whether a confirmed hang reattempts the run
    (``fail``) or is only logged (``observe``).

    Every poll records the detector's state and the longest stall as Grafana
    metrics, so a run's whole detection history can be read against its other
    metrics.

    Raises:
        ValueError: If one of the env vars is invalid.
    """

    def __init__(self):
        self._poll_interval_s = env_float(
            NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, DEFAULT_NCCL_RAS_MIN_POLL_INTERVAL_S
        )
        if self._poll_interval_s <= 0:
            raise ValueError(
                f"{NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR} must be a positive number "
                f"of seconds, got {self._poll_interval_s}."
            )
        self._confirm_duration_s = env_float(
            NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, DEFAULT_NCCL_RAS_CONFIRM_DURATION_S
        )
        if self._confirm_duration_s <= 0:
            raise ValueError(
                f"{NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR} must be a positive number "
                f"of seconds, got {self._confirm_duration_s}."
            )
        self._action = os.environ.get(
            NCCL_RAS_ACTION_ENV_VAR, DEFAULT_NCCL_RAS_ACTION
        ).lower()
        if self._action not in (NCCL_RAS_ACTION_FAIL, NCCL_RAS_ACTION_OBSERVE):
            raise ValueError(
                f"{NCCL_RAS_ACTION_ENV_VAR} must be one of "
                f"{NCCL_RAS_ACTION_FAIL!r} or {NCCL_RAS_ACTION_OBSERVE!r}, "
                f"got {self._action!r}."
            )

        self._worker_group: Optional[WorkerGroup] = None
        # Created once the controller knows the run's name and id.
        self._metrics: Optional[Dict[str, Metric]] = None

    def health_check(self) -> PeriodicHealthCheck:
        """The in-flight check that detects the hangs, which the controller
        adds to the run."""
        return nccl_hang_check(
            self, self._poll_interval_s, self._confirm_duration_s, self._action
        )

    def after_controller_start(self, train_run_context: TrainRunContext):
        base_tags = {
            RUN_NAME_TAG_KEY: train_run_context.get_run_config().name,
            RUN_ID_TAG_KEY: train_run_context.run_id,
        }
        self._metrics = {
            GRAFANA_STATE: EnumMetric[NCCLHangDetectorState](
                name=GRAFANA_STATE,
                description="Current state of the NCCL hang detector.",
                base_tags=base_tags,
                enum_tag_key=GRAFANA_STATE_TAG_KEY,
            ),
            GRAFANA_STALL_DURATION_S: ValueMetric(
                name=GRAFANA_STALL_DURATION_S,
                description=(
                    "Seconds the most stalled NCCL communicator has made no "
                    "progress with a collective mismatch."
                ),
                base_tags=base_tags,
            ),
        }

    async def before_controller_shutdown(self):
        # Clear the gauges so a finished run doesn't keep showing its last state.
        if self._metrics is not None:
            for metric in self._metrics.values():
                metric.reset()

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._worker_group = worker_group

    def before_worker_group_shutdown(self, worker_group: WorkerGroup):
        self._worker_group = None

    def workers(self) -> List[WorkerInfo]:
        if self._worker_group is None:
            return []
        return [
            WorkerInfo(
                world_rank=worker.distributed_context.world_rank,
                node_id=worker.metadata.node_id,
                node_ip=worker.metadata.node_ip,
                pid=worker.metadata.pid,
            )
            for worker in self._worker_group.get_workers()
            if worker.distributed_context is not None
        ]

    def upload(self, tool: str, files: Dict[str, str]) -> str:
        """Upload one diagnostic's files to
        ``<experiment_fs_path>/hang_detector/<tool>/``."""
        if self._worker_group is None:
            raise RuntimeError("No worker group to upload the diagnostics for.")
        storage_context = self._worker_group._storage_context
        fs_path = os.path.join(
            storage_context.experiment_fs_path, HANG_DETECTOR_DIRNAME, tool
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for name, contents in files.items():
                (Path(temp_dir) / name).write_text(contents)
            _upload_to_fs_path(temp_dir, storage_context.storage_filesystem, fs_path)
        return fs_path

    def record_state(self, state: NCCLHangDetectorState, stall_s: float) -> None:
        if self._metrics is None:
            return
        self._metrics[GRAFANA_STALL_DURATION_S].record(stall_s)
        self._metrics[GRAFANA_STATE].record(state)
