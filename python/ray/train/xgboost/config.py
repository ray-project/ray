import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass

from xgboost import RabitTracker
from xgboost.collective import CommunicatorContext

import ray
from ray.train._internal.worker_group import WorkerGroup
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
            with CommunicatorContext():
                yield

        return collective_communication_context

    @property
    def backend_cls(self):
        if self.xgboost_communicator == "rabit":
            return _XGBoostRabitBackend

        raise NotImplementedError(f"Unsupported backend: {self.xgboost_communicator}")


class _XGBoostRabitBackend(Backend):
    def __init__(self):
        self._tracker = None

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: XGBoostConfig
    ):
        assert backend_config.xgboost_communicator == "rabit"

        # Set up the rabit tracker on the Train driver.
        num_workers = len(worker_group)
        rabit_args = {"DMLC_NUM_WORKER": num_workers}
        train_driver_ip = ray.util.get_node_ip_address()

        # NOTE: sortby="task" is needed to ensure that the xgboost worker ranks
        # align with Ray Train worker ranks.
        # The worker ranks will be sorted by `DMLC_TASK_ID`,
        # which is defined in `on_training_start`.
        self._tracker = RabitTracker(
            host_ip=train_driver_ip, n_workers=num_workers, sortby="task"
        )
        rabit_args.update(self._tracker.worker_envs())
        self._tracker.start(num_workers)

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

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: XGBoostConfig):
        timeout = 5
        self._tracker.thread.join(timeout=timeout)

        if self._tracker.thread.is_alive():
            logger.warning(
                "During shutdown, the RabitTracker thread failed to join "
                f"within {timeout} seconds. "
                "The process will still be terminated as part of Ray actor cleanup."
            )
