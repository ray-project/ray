import numpy as np
import logging

from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

try:
    import tree
except (ImportError, ModuleNotFoundError) as e:
    logger.warning("`dm-tree` is not installed! Run `pip install dm-tree`.")
    raise e


def huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return torch.where(
        torch.abs(x) < delta,
        torch.pow(x, 2.0) * 0.5, delta * (torch.abs(x) - 0.5 * delta))


def reduce_mean_ignore_inf(x, axis):
    """Same as torch.mean() but ignores -inf values."""
    mask = torch.ne(x, float("-inf"))
    x_zeroed = torch.where(mask, x, torch.zeros_like(x))
    return torch.sum(x_zeroed, axis) / torch.sum(mask.float(), axis)


def minimize_and_clip(optimizer, objective, var_list, clip_val=10):
    """Minimized `objective` using `optimizer` w.r.t. variables in
    `var_list` while ensure the norm of the gradients for each
    variable is clipped to `clip_val`
    """
    gradients = optimizer.compute_gradients(objective, var_list=var_list)
    for i, (grad, var) in enumerate(gradients):
        if grad is not None:
            gradients[i] = (torch.nn.utils.clip_grad_norm_(grad, clip_val),
                            var)
    return gradients


def sequence_mask(lengths, maxlen, dtype=None):
    """
    Exact same behavior as tf.sequence_mask.
    Thanks to Dimitris Papatheodorou
    (https://discuss.pytorch.org/t/pytorch-equivalent-for-tf-sequence-mask/
    39036).
    """
    if maxlen is None:
        maxlen = lengths.max()

    mask = ~(torch.ones((len(lengths), maxlen)).cumsum(dim=1).t() > lengths). \
        t()
    mask.type(dtype or torch.bool)

    return mask


def convert_to_non_torch_type(stats):
    """Converts values in `stats` to non-Tensor numpy or python types.

    Args:
        stats (any): Any (possibly nested) struct, the values in which will be
            converted and returned as a new struct with all torch tensors
            being converted to numpy types.

    Returns:
        Any: A new struct with the same structure as `stats`, but with all
            values converted to non-torch Tensor types.
    """

    # The mapping function used to numpyize torch Tensors.
    def mapping(item):
        if isinstance(item, torch.Tensor):
            return item.cpu().item() if len(item.size()) == 0 else \
                item.cpu().detach().numpy()
        else:
            return item

    return tree.map_structure(mapping, stats)


def convert_to_torch_tensor(stats, device=None):
    """Converts any struct to torch.Tensors.

    stats (any): Any (possibly nested) struct, the values in which will be
        converted and returned as a new struct with all leaves converted
        to torch tensors.

    Returns:
        Any: A new struct with the same structure as `stats`, but with all
            values converted to torch Tensor types.
    """
    def mapping(item):
        if torch.is_tensor(item):
            return item if device is None else item.to(device)
        tensor = torch.from_numpy(np.asarray(item))
        # Floatify all float64 tensors.
        if tensor.dtype == torch.double:
            tensor = tensor.float()
        return tensor if device is None else tensor.to(device)

    return tree.map_structure(mapping, stats)

