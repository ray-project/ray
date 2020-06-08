import numpy as np

from ray.rllib.utils import try_import_torch, try_import_tree

torch, _ = try_import_torch()
tree = try_import_tree()


def global_norm(tensors):
    """Returns the global L2 norm over a list of tensors.

    output = sqrt(SUM(t ** 2 for t in tensors)),
        where SUM reduces over all tensors and over all elements in tensors.

    Args:
        tensors (List[torch.Tensor]): The list of tensors to calculate the
            global norm over.
    """
    # List of single tensors' L2 norms: SQRT(SUM(xi^2)) over all xi in tensor.
    single_l2s = [
        torch.pow(torch.sum(torch.pow(t, 2.0)), 0.5) for t in tensors
    ]
    # Compute global norm from all single tensors' L2 norms.
    return torch.pow(sum(torch.pow(l2, 2.0) for l2 in single_l2s), 0.5)


def huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return torch.where(
        torch.abs(x) < delta,
        torch.pow(x, 2.0) * 0.5, delta * (torch.abs(x) - 0.5 * delta))


def l2_loss(x):
    """Computes half the L2 norm of a tensor without the sqrt.

    output = sum(x ** 2) / 2
    """
    return torch.sum(torch.pow(x, 2.0)) / 2.0


def reduce_mean_ignore_inf(x, axis):
    """Same as torch.mean() but ignores -inf values."""
    mask = torch.ne(x, float("-inf"))
    x_zeroed = torch.where(mask, x, torch.zeros_like(x))
    return torch.sum(x_zeroed, axis) / torch.sum(mask.float(), axis)


def minimize_and_clip(optimizer, clip_val=10):
    """Clips gradients found in `optimizer.param_groups` to given value.

    Ensures the norm of the gradients for each variable is clipped to
    `clip_val`
    """
    for param_group in optimizer.param_groups:
        for p in param_group["params"]:
            if p.grad is not None:
                torch.nn.utils.clip_grad_norm_(p.grad, clip_val)


def sequence_mask(lengths, maxlen=None, dtype=None):
    """Offers same behavior as tf.sequence_mask for torch.

    Thanks to Dimitris Papatheodorou
    (https://discuss.pytorch.org/t/pytorch-equivalent-for-tf-sequence-mask/
    39036).
    """
    if maxlen is None:
        maxlen = lengths.max()

    mask = ~(torch.ones((len(lengths), maxlen)).to(
        lengths.device).cumsum(dim=1).t() > lengths).t()
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


def atanh(x):
    return 0.5 * torch.log((1 + x) / (1 - x))
