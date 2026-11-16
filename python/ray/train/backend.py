import logging
from contextlib import nullcontext
from typing import Any, Dict, TypeVar

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
        """The :class:`Backend` class that sets up this framework on the workers."""
        return Backend

    @property
    def train_func_context(self):
        """The context manager class wrapping each worker's training function.

        The base class uses ``contextlib.nullcontext``, which does nothing.
        """
        return nullcontext

    @property
    def framework(self):
        """The training framework this config sets up. ``None`` on the base class."""
        return None

    def _repr_html_(self) -> str:
        return make_table_html_repr(obj=self, title=type(self).__name__)

    def to_dict(self) -> Dict[str, Any]:
        """Returns serializable dictionary representation of the backend config.
        Subclasses can override this to expose framework-specific configuration.

        The fields here are used for state export of the backend config.
        If a field is not serializable, it should be excluded.

        Returns:
            A dictionary of the config's serializable fields. The base class
            returns an empty dictionary.
        """
        return {}


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
    has_replica_groups: bool = False

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
