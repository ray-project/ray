import logging
from contextlib import nullcontext
from typing import TypeVar

from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train._internal.utils import Singleton
from ray.util.annotations import DeveloperAPI
from ray.widgets import make_table_html_repr

EncodedData = TypeVar("EncodedData")

logger = logging.getLogger(__name__)


@DeveloperAPI
class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        return Backend

    @property
    def train_func_context(self):
        return nullcontext

    def _repr_html_(self) -> str:
        return make_table_html_repr(obj=self, title=type(self).__name__)


@DeveloperAPI
class Backend(metaclass=Singleton):
    """Singleton for distributed communication backend.

    Attributes:
        share_cuda_visible_devices: If True, each worker
            process will have CUDA_VISIBLE_DEVICES set as the visible device
            IDs of all workers on the same node for this training instance.
            If False, each worker will have CUDA_VISIBLE_DEVICES set to the
            device IDs allocated by Ray for that worker.
    """

    share_cuda_visible_devices: bool = False

    def on_start(self, worker_group: BaseWorkerGroup, backend_config: BackendConfig):
        """Logic for starting this backend."""
        pass

    def on_shutdown(self, worker_group: BaseWorkerGroup, backend_config: BackendConfig):
        """Logic for shutting down the backend."""
        pass

    def on_training_start(
        self, worker_group: BaseWorkerGroup, backend_config: BackendConfig
    ):
        """Logic ran right before training is started.

        Session API is available at this point."""
        pass
