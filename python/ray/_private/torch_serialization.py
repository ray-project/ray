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
            "Torch tensors cannot be zero-copy serialized and "
            "deserialized which may lead to worse performance. "
            "To avoid this, convert the tensor to a numpy array using "
            "mytensor.to_numpy() before passing or returning the tensor to enable "
            "zero-copy deserialization."
            f"\n\tTensor shape: {obj.shape}"
            f"\n\tTensor dtype: {obj.dtype}"
            f"\n\tArray first few values: {obj[:5]}",
            UserWarning,
        )
        _torch_serializer_has_warned = True
    return obj.__reduce_ex__(pickle.HIGHEST_PROTOCOL)
