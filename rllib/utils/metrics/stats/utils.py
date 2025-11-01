from typing import Any, List, Union
from collections import deque

from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
def single_value_to_cpu(value):
    """Convert a single value to CPU if it's a tensor.

    TensorFlow tensors are always converted to numpy/python values.
    PyTorch tensors are converted to python scalars.
    """
    if torch and isinstance(value, torch.Tensor):
        return value.detach().cpu().item()
    elif tf and tf.is_tensor(value):
        return value.numpy()
    return value


@DeveloperAPI
def batch_values_to_cpu(values: Union[List[Any], deque]) -> List[Any]:
    """Convert a list or deque of GPU tensors to CPU scalars in a single operation.

    This function efficiently processes multiple PyTorch GPU tensors together by
    stacking them and performing a single .cpu() call. Assumes all values are either
    PyTorch tensors (on same device) or already CPU values.

    Args:
        values: A list or deque of values that may be GPU tensors.

    Returns:
        A list of CPU scalar values.
    """
    if not values:
        return []

    # Check if first value is a torch tensor - assume all are the same type
    if torch and isinstance(values[0], torch.Tensor):
        # Stack all tensors and move to CPU in one operation
        stacked = torch.stack(list(values))
        cpu_tensor = stacked.detach().cpu()
        return cpu_tensor.tolist()

    # Already CPU values
    return list(values)
