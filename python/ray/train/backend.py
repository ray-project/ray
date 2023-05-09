import logging
from typing import TypeVar

from ray.air.checkpoint import Checkpoint
from ray.train._internal.utils import Singleton
from ray.train._internal.worker_group import WorkerGroup
from ray.util.annotations import DeveloperAPI
from ray.widgets import make_table_html_repr

EncodedData = TypeVar("EncodedData")

logger = logging.getLogger(__name__)

# This is used in several places to print a warning.
_encode_decode_deprecation_message = (
    "``encode_data`` and ``decode_data`` are deprecated in favor of "
    "framework-specific ``ray.air.Checkpoint`` subclasses (reported "
    "using ``ray.air.session.report()``) which can implement "
    "encoding and decoding logic."
)


@DeveloperAPI
class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        return Backend

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

    def on_start(self, worker_group: WorkerGroup, backend_config: BackendConfig):
        """Logic for starting this backend."""
        pass

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: BackendConfig):
        """Logic for shutting down the backend."""
        pass

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: BackendConfig
    ):
        """Logic ran right before training is started.

        Session API is available at this point."""
        pass

    # TODO(ml-team): Remove in 2.6.
    @classmethod
    def _encode_data(cls, checkpoint: Checkpoint) -> Checkpoint:
        raise DeprecationWarning(_encode_decode_deprecation_message)

    # TODO(ml-team): Remove in 2.6.
    @classmethod
    def _decode_data(cls, checkpoint: Checkpoint) -> Checkpoint:
        raise DeprecationWarning(_encode_decode_deprecation_message)
