"""
This module is intended for implementing internal serializers for some
site packages.
"""

import warnings

try:
    import torch


    class _TorchTensorReducingHelper:
        def __init__(self, tensor):
            self.tensor = tensor

        @classmethod
        def rebuild_tensor(cls, _rebuild_func, ndarray, params):
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning,
                                        message="The given NumPy array is not writeable")
                storage = torch.from_numpy(ndarray).storage()
            tensor = _rebuild_func(storage, *params)
            return cls(tensor)

        def __reduce_ex__(self, protocol):
            if self.tensor.is_sparse:
                # Torch will help us reduce the sparse tensor into
                # several continuous tensors.
                return _TorchTensorReducingHelper, (self.tensor,)
            # By only replacing the storage with a numpy array, we can reuse
            # zero-copy serialization while keeping all other params of the torch tensor.
            _rebuild_func, content = self.tensor.__reduce_ex__(protocol)
            return self.rebuild_tensor, (_rebuild_func, self.tensor.numpy(), content[1:])


    def _unwrap_tensor(s):
        return s.tensor


    def torch_tensor_reducer(tensor):
        return _unwrap_tensor, (_TorchTensorReducingHelper(tensor),)

except ImportError:
    pass


def apply(serialization_context):
    try:
        import torch
        serialization_context._register_cloudpickle_reducer(
            torch.Tensor, torch_tensor_reducer)
    except ImportError:
        pass
