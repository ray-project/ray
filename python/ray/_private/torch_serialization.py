import pickle
import warnings

from ray._private.ray_constants import DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE

import torch

_torch_serializer_has_warned = False


def _register_torch_tensor_data_serializer(serialization_context):
    serialization_context._register_cloudpickle_reducer(
        torch.Tensor, _torch_tensor_reduce
    )


def _torch_tensor_reduce(obj: torch.Tensor):
    global _torch_serializer_has_warned
    if (
        not _torch_serializer_has_warned
        and obj.nbytes >= DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE
    ):
        warnings.warn(
            "Torch tensors are not zero-copy serialized and can lead to worse "
            "performance. Convert to a numpy array.",
            UserWarning,
        )
        _torch_serializer_has_warned = True
    return obj.__reduce_ex__(pickle.HIGHEST_PROTOCOL)
