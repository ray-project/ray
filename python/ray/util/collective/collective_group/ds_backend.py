import pickle
import uuid
import warnings
import os
from typing import TYPE_CHECKING, List

import ray
from ray.util.collective.types import Backend

if TYPE_CHECKING:
    import torch

try:
    from datasystem import DsTensorClient
except ImportError as e:
    print(f"Import error: {e}")


class DSBackend:
    """Backend implementation for Data System(DS) tensor transport.

    This class provides functionality for transferring tensors using DS. It handles
    initialization of the DS client, receiving tensors, and managing DS metadata.
    """

    def __init__(self):
        """Initialize the DS backend.
        Creates a DS client with connection to DS worker.
        """
        host = "localhost"
        port = 2379
        npu_ids = os.environ["ASCEND_RT_VISIBLE_DEVICES"]
        if len(npu_ids) > 1:
            device_id = int(npu_ids[0])
            warnings.warn(
                f"Data system requires exactly 1 NPU, but detected {len(npu_ids)} NPUs. Will use the first NPU (ID: {npu_ids[0]}) to connect to the data system"
            )

        self._ds_client = DsTensorClient(
            host=host, port=port, device_id=device_id, connect_timeout_ms=60000
        )
        self._ds_client.init()

    @classmethod
    def backend(cls):
        """Get the backend type.

        Returns:
            Backend.DS: The backend type enum value for DS.
        """
        return Backend.DS

    def get_ds_metadata(self, tensors: List["torch.Tensor"]):
        """Get DS metadata for a set of tensors.

        Args:
            tensors: List of tensors to get metadata for.

        Returns:
            Serialized keys for the tensors in DS

        Raises:
            RuntimeError: If metadata registration fails
            TypeError: If input parameters contain illegal values.
        """
        keys = [f"tensor_{uuid.uuid4()}" for _ in tensors]
        self._ds_client.dev_mset(keys=keys, tensors=tensors)
        return pickle.dumps(keys)

    def recv(
        self,
        tensors: List["torch.Tensor"],
        ds_serialized_keys: bytes,
        sub_timeout_ms: int = 30000,
    ):
        """Receive tensors from a remote DS client.

        Args:
            tensors: List of tensors to receive into.
            ds_serialized_keys: Serialized keys for the remote tensors in DS.
            sub_timeout_ms: Timeout for the dev_mget operation in milliseconds.

        Raises:
            RuntimeError: If the DS transfer fails.
            TypeError: If input parameters contain illegal values.
        """
        keys = pickle.loads(ds_serialized_keys)
        self._ds_client.dev_mget(
            keys=keys, tensors=tensors, sub_timeout_ms=sub_timeout_ms
        )

    def deregister_memory(self, ds_serialized_keys: bytes):
        """Deregister previously registered tensors from DS.

        Args:
            ds_serialized_keys: Serialized keys for the remote tensors in DS.
        """
        keys = pickle.loads(ds_serialized_keys)
        self._ds_client.dev_delete(keys=keys)
