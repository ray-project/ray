import json
import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import xgboost
from packaging.version import Version
from xgboost import RabitTracker
from xgboost.collective import CommunicatorContext

import ray
from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train.backend import Backend, BackendConfig

logger = logging.getLogger(__name__)


@dataclass
class XGBoostConfig(BackendConfig):
    """Configuration for xgboost collective communication setup.

    Ray Train will set up the necessary coordinator processes and environment
    variables for your workers to communicate with each other.
    Additional configuration options can be passed into the
    `xgboost.collective.CommunicatorContext` that wraps your own `xgboost.train` code.

    See the `xgboost.collective` module for more information:
    https://github.com/dmlc/xgboost/blob/master/python-package/xgboost/collective.py

    Args:
        xgboost_communicator: The backend to use for collective communication for
            distributed xgboost training. For now, only "rabit" is supported.
    """

    xgboost_communicator: str = "rabit"

    @property
    def train_func_context(self):
        @contextmanager
        def collective_communication_context():
            with CommunicatorContext(**_get_xgboost_args()):
                yield

        return collective_communication_context

    @property
    def backend_cls(self):
        if self.xgboost_communicator == "rabit":
            return (
                _XGBoostRabitBackend
                if Version(xgboost.__version__) >= Version("2.1.0")
                else _XGBoostRabitBackend_pre_xgb210
            )

        raise NotImplementedError(f"Unsupported backend: {self.xgboost_communicator}")


class _XGBoostRabitBackend(Backend):
    def __init__(self):
        self._tracker: Optional[RabitTracker] = None
        self._wait_thread: Optional[threading.Thread] = None

    def _setup_xgboost_distributed_backend(self, worker_group: BaseWorkerGroup):
        # Set up the rabit tracker on the Train driver.
        num_workers = len(worker_group)
        rabit_args = {"n_workers": num_workers}
        train_driver_ip = ray.util.get_node_ip_address()

        # NOTE: sortby="task" is needed to ensure that the xgboost worker ranks
        # align with Ray Train worker ranks.
        # The worker ranks will be sorted by `dmlc_task_id`,
        # which is defined below.
        self._tracker = RabitTracker(
            n_workers=num_workers, host_ip=train_driver_ip, sortby="task"
        )
        self._tracker.start()

        # The RabitTracker is started in a separate thread, and the
        # `wait_for` method must be called for `worker_args` to return.
        self._wait_thread = threading.Thread(target=self._tracker.wait_for, daemon=True)
        self._wait_thread.start()

        rabit_args.update(self._tracker.worker_args())

        start_log = (
            "RabitTracker coordinator started with parameters:\n"
            f"{json.dumps(rabit_args, indent=2)}"
        )
        logger.debug(start_log)

        def set_xgboost_communicator_args(args):
            import ray.train

            args["dmlc_task_id"] = (
                f"[xgboost.ray-rank={ray.train.get_context().get_world_rank():08}]:"
                f"{ray.get_runtime_context().get_actor_id()}"
            )

            _set_xgboost_args(args)

        worker_group.execute(set_xgboost_communicator_args, rabit_args)

    def on_training_start(
        self, worker_group: BaseWorkerGroup, backend_config: XGBoostConfig
    ):
        assert backend_config.xgboost_communicator == "rabit"
        self._setup_xgboost_distributed_backend(worker_group)

    def on_shutdown(self, worker_group: BaseWorkerGroup, backend_config: XGBoostConfig):
        timeout = 5

        if self._wait_thread is not None:
            self._wait_thread.join(timeout=timeout)

            if self._wait_thread.is_alive():
                logger.warning(
                    "During shutdown, the RabitTracker thread failed to join "
                    f"within {timeout} seconds. "
                    "The process will still be terminated as part of Ray actor cleanup."
                )


class _XGBoostRabitBackend_pre_xgb210(Backend):
    def __init__(self):
        self._tracker: Optional[RabitTracker] = None

    def _setup_xgboost_distributed_backend(self, worker_group: BaseWorkerGroup):
        # Set up the rabit tracker on the Train driver.
        num_workers = len(worker_group)
        rabit_args = {"DMLC_NUM_WORKER": num_workers}
        train_driver_ip = ray.util.get_node_ip_address()

        # NOTE: sortby="task" is needed to ensure that the xgboost worker ranks
        # align with Ray Train worker ranks.
        # The worker ranks will be sorted by `DMLC_TASK_ID`,
        # which is defined below.
        self._tracker = RabitTracker(
            n_workers=num_workers, host_ip=train_driver_ip, sortby="task"
        )
        self._tracker.start(n_workers=num_workers)

        worker_args = self._tracker.worker_envs()
        rabit_args.update(worker_args)

        start_log = (
            "RabitTracker coordinator started with parameters:\n"
            f"{json.dumps(rabit_args, indent=2)}"
        )
        logger.debug(start_log)

        def set_xgboost_env_vars():
            import ray.train

            for k, v in rabit_args.items():
                os.environ[k] = str(v)

            # Ranks are assigned in increasing order of the worker's task id.
            # This task id will be sorted by increasing world rank.
            os.environ["DMLC_TASK_ID"] = (
                f"[xgboost.ray-rank={ray.train.get_context().get_world_rank():08}]:"
                f"{ray.get_runtime_context().get_actor_id()}"
            )

        worker_group.execute(set_xgboost_env_vars)

    def on_training_start(
        self, worker_group: BaseWorkerGroup, backend_config: XGBoostConfig
    ):
        assert backend_config.xgboost_communicator == "rabit"
        self._setup_xgboost_distributed_backend(worker_group)

    def on_shutdown(self, worker_group: BaseWorkerGroup, backend_config: XGBoostConfig):
        if not self._tracker:
            return

        timeout = 5
        self._tracker.thread.join(timeout=timeout)

        if self._tracker.thread.is_alive():
            logger.warning(
                "During shutdown, the RabitTracker thread failed to join "
                f"within {timeout} seconds. "
                "The process will still be terminated as part of Ray actor cleanup."
            )


_xgboost_args: dict = {}
_xgboost_args_lock = threading.Lock()


def _set_xgboost_args(args):
    with _xgboost_args_lock:
        global _xgboost_args
        _xgboost_args = args


def _get_xgboost_args() -> dict:
    with _xgboost_args_lock:
        return _xgboost_args
