from dataclasses import dataclass
from typing import Any, Dict, Optional

from xgboost import RabitTracker

import ray
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig


@dataclass
class XGBoostConfig(BackendConfig):
    """Configuration for xgboost collective communication setup.

    Args:
        xgboost_communicator: The backend to use for collective communication for
            distributed xgboost training. One of ["rabit", "federated"].
        args: Additional arguments to pass to the backend.
            See ___. TODO: link to __init__ docstring
    """

    xgboost_communicator: Optional[str] = "rabit"
    args: Optional[Dict[str, Any]] = None

    @property
    def backend_cls(self):
        return _XGBoostBackend


class _XGBoostBackend(Backend):
    def on_start(self, worker_group: WorkerGroup, backend_config: XGBoostConfig):
        user_args = backend_config.args or {}
        xgboost_communicator = user_args.pop(
            "xgboost_communicator", backend_config.xgboost_communicator
        )
        if xgboost_communicator == "rabit":
            # Set up the rabit tracker on the Train driver.
            num_workers = len(worker_group)
            self.rabit_args = {"DMLC_NUM_WORKER": num_workers}
            train_driver_ip = ray.util.get_node_ip_address()
            self.tracker = RabitTracker(host_ip=train_driver_ip, n_workers=num_workers)
            self.rabit_args.update(self.tracker.worker_envs())

            self.tracker.start(num_workers)

            print("DMLC_TRACKER_ENV_START\n")
            for k, v in self.rabit_args.items():
                print(f"{k}={v}\n")
            print("DMLC_TRACKER_ENV_END\n")

        else:
            raise NotImplementedError(f"{xgboost_communicator=}")

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: XGBoostConfig
    ):
        print("setting up...")
        rabit_args = self.rabit_args

        def set_xgboost_env_vars():
            import os

            for k, v in rabit_args.items():
                os.environ[k] = str(v)
            os.environ["DMLC_TASK_ID"] = ray.get_runtime_context().get_actor_id()

        worker_group.execute(set_xgboost_env_vars)

        # def enter_communicator_context():
        #     from ray.train._internal.session import get_session

        #     rabit_args["DMLC_TASK_ID"] = ray.get_runtime_context().get_actor_id()
        #     context = CommunicatorContext(**rabit_args)

        #     session = get_session()
        #     session.state = {"communicator_context": context}

        #     context.__enter__()

        #     # option 2: set the environment variables,
        #     # and have the user add the context manager themselves.

        # worker_group.execute(enter_communicator_context)
        print("setting up... done!")

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: XGBoostConfig):
        print("shutting down...")

        # def exit_communicator_context():
        #     from ray.train._internal.session import get_session

        #     session = get_session()
        #     session.state["communicator_context"].__exit__()

        # worker_group.execute(exit_communicator_context)

        self.tracker.thread.join(timeout=5)

        print("shutting down... done!")
