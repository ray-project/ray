from typing import Any, Dict, List, Optional, Union

import torch

from ray.air._internal.device_manager import get_torch_device_manager_by_context


def get_devices() -> List[torch.device]:
    """Gets the correct torch device list configured for this process.

    Returns a list of torch accelerator (GPU, HPU, NPU...) devices allocated for
    the current worker.
    If no accelerators are assigned, then it returns a list with a single CPU device.
    """
    return get_torch_device_manager_by_context().get_devices()


def load_torch_model(
    saved_model: Union[torch.nn.Module, Dict],
    model_definition: Optional[torch.nn.Module] = None,
) -> torch.nn.Module:
    """Loads a PyTorch model from the provided ``saved_model``.

    ``model_definition`` is only used when ``saved_model`` is
    a torch state dict, which will be loaded into ``model_definition``.
    Otherwise, ``model_definition`` is discarded.
    """
    if isinstance(saved_model, torch.nn.Module):
        return saved_model
    elif isinstance(saved_model, dict):
        if not model_definition:
            raise ValueError(
                "Attempting to load torch model from a "
                "state_dict, but no `model_definition` was "
                "provided."
            )
        model_definition.load_state_dict(saved_model)
        return model_definition
    else:
        raise ValueError(
            f"Saved model is of type {type(saved_model)}. "
            f"The model saved in the checkpoint is expected "
            f"to be of type `torch.nn.Module`, or a model "
            f"state dict of type dict."
        )


def contains_tensor(obj):
    if isinstance(obj, torch.Tensor):
        return True
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if contains_tensor(k):
                return True
            if contains_tensor(v):
                return True
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            if contains_tensor(v):
                return True
    return False


# Not present in torch<=1.7.0
# Adapted from https://github.com/pytorch/pytorch/blob/\
# c18da597e0bb1c1aecc97c77a73fed1849057fa4/torch/nn/modules/utils.py
def consume_prefix_in_state_dict_if_present_not_in_place(
    state_dict: Dict[str, Any], prefix: str
) -> Dict[str, Any]:
    """Strip the prefix in state_dict, if any and return a new dict.

    Adapted from https://github.com/pytorch/pytorch/blob/\
c18da597e0bb1c1aecc97c77a73fed1849057fa4/torch/nn/modules/utils.py
    The original method modified the dict in-place.

    Args:
        state_dict: a state-dict to be loaded to the model.
        prefix: prefix.

    """
    copied = False

    for key in state_dict:
        if key.startswith(prefix):
            newkey = key[len(prefix) :]
            if not copied:
                # We are doing shallow copies here, so the performance
                # impact should be negligible anyway, but this is
                # a simple optimization.
                state_dict = state_dict.copy()
                copied = True
            state_dict[newkey] = state_dict.pop(key)

    if "_metadata" in state_dict:
        state_dict["_metadata"] = state_dict["_metadata"].copy()
        metadata = state_dict["_metadata"]
        for key in metadata:
            if len(key) == 0:
                continue
            newkey = key[len(prefix) :]
            metadata[newkey] = metadata.pop(key)

    return state_dict
