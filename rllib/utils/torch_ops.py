from gym.spaces import Discrete, MultiDiscrete
import numpy as np
import tree
import warnings

from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

# Limit values suitable for use as close to a -inf logit. These are useful
# since -inf / inf cause NaNs during backprop.
FLOAT_MIN = -3.4e38
FLOAT_MAX = 3.4e38


def apply_grad_clipping(policy, optimizer, loss):
    """Applies gradient clipping to already computed grads inside `optimizer`.

    Args:
        policy (TorchPolicy): The TorchPolicy, which calculated `loss`.
        optimizer (torch.optim.Optimizer): A local torch optimizer object.
        loss (torch.Tensor): The torch loss tensor.
    """
    info = {}
    if policy.config["grad_clip"]:
        for param_group in optimizer.param_groups:
            # Make sure we only pass params with grad != None into torch
            # clip_grad_norm_. Would fail otherwise.
            params = list(
                filter(lambda p: p.grad is not None, param_group["params"]))
            if params:
                grad_gnorm = nn.utils.clip_grad_norm_(
                    params, policy.config["grad_clip"])
                if isinstance(grad_gnorm, torch.Tensor):
                    grad_gnorm = grad_gnorm.cpu().numpy()
                info["grad_gnorm"] = grad_gnorm
    return info


def atanh(x):
    return 0.5 * torch.log((1 + x) / (1 - x))


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
                item.detach().cpu().numpy()
        else:
            return item

    return tree.map_structure(mapping, stats)


def convert_to_torch_tensor(x, device=None):
    """Converts any struct to torch.Tensors.

    x (any): Any (possibly nested) struct, the values in which will be
        converted and returned as a new struct with all leaves converted
        to torch tensors.

    Returns:
        Any: A new struct with the same structure as `stats`, but with all
            values converted to torch Tensor types.
    """

    def mapping(item):
        # Already torch tensor -> make sure it's on right device.
        if torch.is_tensor(item):
            return item if device is None else item.to(device)
        # Special handling of "Repeated" values.
        elif isinstance(item, RepeatedValues):
            return RepeatedValues(
                tree.map_structure(mapping, item.values), item.lengths,
                item.max_len)
        # Numpy arrays.
        if isinstance(item, np.ndarray):
            # np.object_ type (e.g. info dicts in train batch): leave as-is.
            if item.dtype == np.object_:
                return item
            # Non-writable numpy-arrays will cause PyTorch warning.
            elif item.flags.writeable is False:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    tensor = torch.from_numpy(item)
            # Already numpy: Wrap as torch tensor.
            else:
                tensor = torch.from_numpy(item)
        # Everything else: Convert to numpy, then wrap as torch tensor.
        else:
            tensor = torch.from_numpy(np.asarray(item))
        # Floatify all float64 tensors.
        if tensor.dtype == torch.double:
            tensor = tensor.float()
        return tensor if device is None else tensor.to(device)

    return tree.map_structure(mapping, x)


def explained_variance(y, pred):
    y_var = torch.var(y, dim=[0])
    diff_var = torch.var(y - pred, dim=[0])
    min_ = torch.tensor([-1.0]).to(pred.device)
    return torch.max(min_, 1 - (diff_var / y_var))


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


def minimize_and_clip(optimizer, clip_val=10):
    """Clips gradients found in `optimizer.param_groups` to given value.

    Ensures the norm of the gradients for each variable is clipped to
    `clip_val`
    """
    for param_group in optimizer.param_groups:
        for p in param_group["params"]:
            if p.grad is not None:
                torch.nn.utils.clip_grad_norm_(p.grad, clip_val)


def one_hot(x, space):
    if isinstance(space, Discrete):
        return nn.functional.one_hot(x.long(), space.n)
    elif isinstance(space, MultiDiscrete):
        return torch.cat(
            [
                nn.functional.one_hot(x[:, i].long(), n)
                for i, n in enumerate(space.nvec)
            ],
            dim=-1)
    else:
        raise ValueError("Unsupported space for `one_hot`: {}".format(space))


def reduce_mean_ignore_inf(x, axis):
    """Same as torch.mean() but ignores -inf values."""
    mask = torch.ne(x, float("-inf"))
    x_zeroed = torch.where(mask, x, torch.zeros_like(x))
    return torch.sum(x_zeroed, axis) / torch.sum(mask.float(), axis)


def sequence_mask(lengths, maxlen=None, dtype=None, time_major=False):
    """Offers same behavior as tf.sequence_mask for torch.

    Thanks to Dimitris Papatheodorou
    (https://discuss.pytorch.org/t/pytorch-equivalent-for-tf-sequence-mask/
    39036).
    """
    if maxlen is None:
        maxlen = int(lengths.max())

    mask = ~(torch.ones(
        (len(lengths), maxlen)).to(lengths.device).cumsum(dim=1).t() > lengths)
    if not time_major:
        mask = mask.t()
    mask.type(dtype or torch.bool)

    return mask


def softmax_cross_entropy_with_logits(logits, labels):
    """Same behavior as tf.nn.softmax_cross_entropy_with_logits.

    Args:
        x (TensorType):

    Returns:

    """
    return torch.sum(-labels * nn.functional.log_softmax(logits, -1), -1)


class Swish(nn.Module):
    def __init__(self):
        super().__init__()
        self._beta = nn.Parameter(torch.tensor(1.0))

    def forward(self, input_tensor):
        return input_tensor * torch.sigmoid(self._beta * input_tensor)
