import logging
from typing import Type, TypeVar, Dict
from ray.air.checkpoint import Checkpoint

from ray.train._internal.utils import Singleton
from ray.train._internal.worker_group import WorkerGroup
from ray.util.annotations import Deprecated, DeveloperAPI
from ray.widgets import make_table_html_repr

EncodedData = TypeVar("EncodedData")

logger = logging.getLogger(__name__)

# This is used in several places to print a warning.
_encode_decode_deprecation_message = (
    "``encode_data`` and ``decode_data`` are deprecated in favor of "
    "framework-specific ``ray.air.Checkpoint`` subclasses (reported "
    "using ``ray.air.session.report()``) which can implement "
    "encoding and decoding logic. In the future, ``encode_data`` and "
    "``decode_data`` will throw an exception if overriden. For legacy "
    "``ray.train.save_checkpoint()`` compatibility, set "
    "``checkpoint_class`` in your ``Backend``."
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

    @staticmethod
    def _get_checkpoint_class(data_dict: Dict) -> Type[Checkpoint]:
        """Get Ray AIR Checkpoint class to use with the legacy Train API.

        This is temporary until ``ray.train.save_checkpoint`` is
        hard-deprecated."""
        return Checkpoint

    @Deprecated(message=_encode_decode_deprecation_message)
    @staticmethod
    def encode_data(data_dict: Dict) -> EncodedData:
        """Logic to encode a data dict before sending to the driver.

        This function will be called on the workers for any data that is
        sent to the driver via ``session.report()``.
        """

        return data_dict

    @Deprecated(message=_encode_decode_deprecation_message)
    @staticmethod
    def decode_data(encoded_data: EncodedData) -> Dict:
        """Logic to decode an encoded data dict.

        This function will be called on the driver after receiving the
        encoded data dict from the worker.
        """

        return encoded_data
