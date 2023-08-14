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
    "framework-specific ``ray.train.Checkpoint`` subclasses (reported "
    "using ``ray.train.report()``) which can implement "
    "encoding and decoding logic."
)


def _warn_about_bad_checkpoint_type(expected_checkpoint_cls: Type[Checkpoint]):
    return
    # Do not print warnings in 2.1 yet.
    # TODO(ml-team): Change this once we have full API parity with framework
    # checkpoints. Also turn on test_torch_trainer::test_torch_bad_checkpoint_warning
    # warnings.warn(
    #     f"You have reported a checkpoint with the `{Checkpoint}` "
    #     "type, but the intended checkpoint type for the Trainer "
    #     f"you are using is `{expected_checkpoint_cls}`. "
    #     "Not using the intended checkpoint type may cause "
    #     "exceptions or other issues, especially during "
    #     "serialization and deserialization. The checkpoint "
    #     "type will be changed automatically. "
    #     "This behavior may change in the future."
    # )


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

    @classmethod
    def _encode_data(cls, checkpoint: Checkpoint) -> Checkpoint:
        """Temporary method until ``encode_data`` is deprecated."""
        if cls.encode_data != Backend.encode_data:
            raise DeprecationWarning(_encode_decode_deprecation_message)
        return checkpoint

    @classmethod
    def _decode_data(cls, checkpoint: Checkpoint) -> Checkpoint:
        """Temporary method until ``decode_data`` is deprecated."""
        if cls.decode_data != Backend.decode_data:
            raise DeprecationWarning(_encode_decode_deprecation_message)
        return checkpoint

    @Deprecated(message=_encode_decode_deprecation_message)
    @staticmethod
    def encode_data(data_dict: Dict) -> EncodedData:
        """Logic to encode a data dict before sending to the driver.

        This function will be called on the workers for any data that is
        sent to the driver via ``ray.train.report()``.
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
