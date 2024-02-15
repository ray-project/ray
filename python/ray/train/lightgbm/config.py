import json
import logging
import os
from dataclasses import dataclass


import ray
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig

logger = logging.getLogger(__name__)


@dataclass
class LightGBMConfig(BackendConfig):
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

    @property
    def backend_cls(self):
        return _LightGBMBackend


class _LightGBMBackend(Backend):
    def __init__(self):
        pass

    def on_start(self, worker_group: WorkerGroup, backend_config: LightGBMConfig):
        pass

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: LightGBMConfig
    ):
        pass

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: LightGBMConfig):
        pass
