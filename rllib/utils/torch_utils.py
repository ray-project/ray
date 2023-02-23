import os
import logging
import warnings
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from gymnasium.spaces import Discrete, MultiDiscrete

import ray
from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.utils.annotations import Deprecated, PublicAPI, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (
    LocalOptimizer,
    SpaceStruct,
    TensorStructType,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.policy.torch_policy import TorchPolicy
    from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2

logger = logging.getLogger(__name__)
torch, nn = try_import_torch()

# Limit values suitable for use as close to a -inf logit. These are useful
# since -inf / inf cause NaNs during backprop.
FLOAT_MIN = -3.4e38
FLOAT_MAX = 3.4e38


@PublicAPI
def apply_grad_clipping(
    policy: "TorchPolicy", optimizer: LocalOptimizer, loss: TensorType
) -> Dict[str, TensorType]:
    """Applies gradient clipping to already computed grads inside `optimizer`.

    Args:
        policy: The TorchPolicy, which calculated `loss`.
        optimizer: A local torch optimizer object.
        loss: The torch loss tensor.

    Returns:
        An info dict containing the "grad_norm" key and the resulting clipped
        gradients.
    """
    grad_gnorm = 0
    if policy.config["grad_clip"] is not None:
        clip_value = policy.config["grad_clip"]
    else:
        clip_value = np.inf

    num_none_grads = 0
    for param_group in optimizer.param_groups:
        # Make sure we only pass params with grad != None into torch
        # clip_grad_norm_. Would fail otherwise.
        params = list(filter(lambda p: p.grad is not None, param_group["params"]))
        if params:
            # PyTorch clips gradients inplace and returns the norm before clipping
            # We therefore need to compute grad_gnorm further down (fixes #4965)
            global_norm = nn.utils.clip_grad_norm_(params, clip_value)

            if isinstance(global_norm, torch.Tensor):
                global_norm = global_norm.cpu().numpy()

            grad_gnorm += min(global_norm, clip_value)
        else:
            num_none_grads += 1

    # Note (Kourosh): grads could indeed be zero. This method should still return
    # grad_gnorm in that case.
    if num_none_grads == len(optimizer.param_groups):
        # No grads available
        return {}
    return {"grad_gnorm": grad_gnorm}


@Deprecated(old="ray.rllib.utils.torch_utils.atanh", new="torch.math.atanh", error=True)
def atanh(x: TensorType) -> TensorType:
    pass


@PublicAPI
def concat_multi_gpu_td_errors(
    policy: Union["TorchPolicy", "TorchPolicyV2"]
) -> Dict[str, TensorType]:
    """Concatenates multi-GPU (per-tower) TD error tensors given TorchPolicy.

    TD-errors are extracted from the TorchPolicy via its tower_stats property.

    Args:
        policy: The TorchPolicy to extract the TD-error values from.

    Returns:
        A dict mapping strings "td_error" and "mean_td_error" to the
        corresponding concatenated and mean-reduced values.
    """
    td_error = torch.cat(
        [
            t.tower_stats.get("td_error", torch.tensor([0.0])).to(policy.device)
            for t in policy.model_gpu_towers
        ],
        dim=0,
    )
    policy.td_error = td_error
    return {
        "td_error": td_error,
        "mean_td_error": torch.mean(td_error),
    }


@Deprecated(new="ray/rllib/utils/numpy.py::convert_to_numpy", error=True)
def convert_to_non_torch_type(stats: TensorStructType) -> TensorStructType:
    pass


@PublicAPI
def convert_to_torch_tensor(x: TensorStructType, device: Optional[str] = None):
    """Converts any struct to torch.Tensors.

    x: Any (possibly nested) struct, the values in which will be
        converted and returned as a new struct with all leaves converted
        to torch tensors.

    Returns:
        Any: A new struct with the same structure as `x`, but with all
            values converted to torch Tensor types. This does not convert possibly
            nested elements that are None because torch has no representation for that.
    """

    def mapping(item):
        if item is None:
            # Torch has no representation for `None`, so we return None
            return item

        # Special handling of "Repeated" values.
        if isinstance(item, RepeatedValues):
            return RepeatedValues(
                tree.map_structure(mapping, item.values), item.lengths, item.max_len
            )

        # Already torch tensor -> make sure it's on right device.
        if torch.is_tensor(item):
            tensor = item
        # Numpy arrays.
        elif isinstance(item, np.ndarray):
            # Object type (e.g. info dicts in train batch): leave as-is.
            # str type (e.g. agent_id in train batch): leave as-is.
            if item.dtype == object or item.dtype.type is np.str_:
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
        if tensor.is_floating_point():
            tensor = tensor.float()

        return tensor if device is None else tensor.to(device)

    return tree.map_structure(mapping, x)


@PublicAPI
def explained_variance(y: TensorType, pred: TensorType) -> TensorType:
    """Computes the explained variance for a pair of labels and predictions.

    The formula used is:
    max(-1.0, 1.0 - (std(y - pred)^2 / std(y)^2))

    Args:
        y: The labels.
        pred: The predictions.

    Returns:
        The explained variance given a pair of labels and predictions.
    """
    y_var = torch.var(y, dim=[0])
    if y_var == 0.0:
        # Model case in which y does not vary with explained variance of -1
        return torch.tensor(-1.0).to(pred.device)
    diff_var = torch.var(y - pred, dim=[0])
    min_ = torch.tensor([-1.0]).to(pred.device)
    return torch.max(min_, 1 - (diff_var / y_var))[0]


@PublicAPI
def flatten_inputs_to_1d_tensor(
    inputs: TensorStructType,
    spaces_struct: Optional[SpaceStruct] = None,
    time_axis: bool = False,
) -> TensorType:
    """Flattens arbitrary input structs according to the given spaces struct.

    Returns a single 1D tensor resulting from the different input
    components' values.

    Thereby:
    - Boxes (any shape) get flattened to (B, [T]?, -1). Note that image boxes
    are not treated differently from other types of Boxes and get
    flattened as well.
    - Discrete (int) values are one-hot'd, e.g. a batch of [1, 0, 3] (B=3 with
    Discrete(4) space) results in [[0, 1, 0, 0], [1, 0, 0, 0], [0, 0, 0, 1]].
    - MultiDiscrete values are multi-one-hot'd, e.g. a batch of
    [[0, 2], [1, 4]] (B=2 with MultiDiscrete([2, 5]) space) results in
    [[1, 0,  0, 0, 1, 0, 0], [0, 1,  0, 0, 0, 0, 1]].

    Args:
        inputs: The inputs to be flattened.
        spaces_struct: The structure of the spaces that behind the input
        time_axis: Whether all inputs have a time-axis (after the batch axis).
            If True, will keep not only the batch axis (0th), but the time axis
            (1st) as-is and flatten everything from the 2nd axis up.

    Returns:
        A single 1D tensor resulting from concatenating all
        flattened/one-hot'd input components. Depending on the time_axis flag,
        the shape is (B, n) or (B, T, n).

    Examples:
        >>> # B=2
        >>> from ray.rllib.utils.tf_utils import flatten_inputs_to_1d_tensor
        >>> from gymnasium.spaces import Discrete, Box
        >>> out = flatten_inputs_to_1d_tensor( # doctest: +SKIP
        ...     {"a": [1, 0], "b": [[[0.0], [0.1]], [1.0], [1.1]]},
        ...     spaces_struct=dict(a=Discrete(2), b=Box(shape=(2, 1))))
        ... ) # doctest: +SKIP
        >>> print(out) # doctest: +SKIP
        [[0.0, 1.0,  0.0, 0.1], [1.0, 0.0,  1.0, 1.1]]  # B=2 n=4

        >>> # B=2; T=2
        >>> out = flatten_inputs_to_1d_tensor( # doctest: +SKIP
        ...     ([[1, 0], [0, 1]],
        ...      [[[0.0, 0.1], [1.0, 1.1]], [[2.0, 2.1], [3.0, 3.1]]]),
        ...     spaces_struct=tuple([Discrete(2), Box(shape=(2, ))]),
        ...     time_axis=True
        ... ) # doctest: +SKIP
        >>> print(out) # doctest: +SKIP
        [[[0.0, 1.0, 0.0, 0.1], [1.0, 0.0, 1.0, 1.1]],\
        [[1.0, 0.0, 2.0, 2.1], [0.0, 1.0, 3.0, 3.1]]]  # B=2 T=2 n=4
    """

    flat_inputs = tree.flatten(inputs)
    flat_spaces = (
        tree.flatten(spaces_struct)
        if spaces_struct is not None
        else [None] * len(flat_inputs)
    )

    B = None
    T = None
    out = []
    for input_, space in zip(flat_inputs, flat_spaces):
        # Store batch and (if applicable) time dimension.
        if B is None:
            B = input_.shape[0]
            if time_axis:
                T = input_.shape[1]

        # One-hot encoding.
        if isinstance(space, Discrete):
            if time_axis:
                input_ = torch.reshape(input_, [B * T])
            out.append(one_hot(input_, space).float())
        # Multi one-hot encoding.
        elif isinstance(space, MultiDiscrete):
            if time_axis:
                input_ = torch.reshape(input_, [B * T, -1])
            out.append(one_hot(input_, space).float())
        # Box: Flatten.
        else:
            if time_axis:
                input_ = torch.reshape(input_, [B * T, -1])
            else:
                input_ = torch.reshape(input_, [B, -1])
            out.append(input_.float())

    merged = torch.cat(out, dim=-1)
    # Restore the time-dimension, if applicable.
    if time_axis:
        merged = torch.reshape(merged, [B, T, -1])

    return merged


@PublicAPI
def get_device(config):
    """Returns a torch device edepending on a config and current worker index."""

    # Figure out the number of GPUs to use on the local side (index=0) or on
    # the remote workers (index > 0).
    worker_idx = config.get("worker_index", 0)
    if (
        not config["_fake_gpus"]
        and ray._private.worker._mode() == ray._private.worker.LOCAL_MODE
    ):
        num_gpus = 0
    elif worker_idx == 0:
        num_gpus = config["num_gpus"]
    else:
        num_gpus = config["num_gpus_per_worker"]
    # All GPU IDs, if any.
    gpu_ids = list(range(torch.cuda.device_count()))

    # Place on one or more CPU(s) when either:
    # - Fake GPU mode.
    # - num_gpus=0 (either set by user or we are in local_mode=True).
    # - No GPUs available.
    if config["_fake_gpus"] or num_gpus == 0 or not gpu_ids:
        return torch.device("cpu")
    # Place on one or more actual GPU(s), when:
    # - num_gpus > 0 (set by user) AND
    # - local_mode=False AND
    # - actual GPUs available AND
    # - non-fake GPU mode.
    else:
        # We are a remote worker (WORKER_MODE=1):
        # GPUs should be assigned to us by ray.
        if ray._private.worker._mode() == ray._private.worker.WORKER_MODE:
            gpu_ids = ray.get_gpu_ids()

        if len(gpu_ids) < num_gpus:
            raise ValueError(
                "TorchPolicy was not able to find enough GPU IDs! Found "
                f"{gpu_ids}, but num_gpus={num_gpus}."
            )
        return torch.device("cuda")


@PublicAPI
def global_norm(tensors: List[TensorType]) -> TensorType:
    """Returns the global L2 norm over a list of tensors.

    output = sqrt(SUM(t ** 2 for t in tensors)),
        where SUM reduces over all tensors and over all elements in tensors.

    Args:
        tensors: The list of tensors to calculate the global norm over.

    Returns:
        The global L2 norm over the given tensor list.
    """
    # List of single tensors' L2 norms: SQRT(SUM(xi^2)) over all xi in tensor.
    single_l2s = [torch.pow(torch.sum(torch.pow(t, 2.0)), 0.5) for t in tensors]
    # Compute global norm from all single tensors' L2 norms.
    return torch.pow(sum(torch.pow(l2, 2.0) for l2 in single_l2s), 0.5)


@PublicAPI
def huber_loss(x: TensorType, delta: float = 1.0) -> TensorType:
    """Computes the huber loss for a given term and delta parameter.

    Reference: https://en.wikipedia.org/wiki/Huber_loss
    Note that the factor of 0.5 is implicitly included in the calculation.

    Formula:
        L = 0.5 * x^2  for small abs x (delta threshold)
        L = delta * (abs(x) - 0.5*delta)  for larger abs x (delta threshold)

    Args:
        x: The input term, e.g. a TD error.
        delta: The delta parmameter in the above formula.

    Returns:
        The Huber loss resulting from `x` and `delta`.
    """
    return torch.where(
        torch.abs(x) < delta,
        torch.pow(x, 2.0) * 0.5,
        delta * (torch.abs(x) - 0.5 * delta),
    )


@PublicAPI
def l2_loss(x: TensorType) -> TensorType:
    """Computes half the L2 norm over a tensor's values without the sqrt.

    output = 0.5 * sum(x ** 2)

    Args:
        x: The input tensor.

    Returns:
        0.5 times the L2 norm over the given tensor's values (w/o sqrt).
    """
    return 0.5 * torch.sum(torch.pow(x, 2.0))


@PublicAPI
def minimize_and_clip(
    optimizer: "torch.optim.Optimizer", clip_val: float = 10.0
) -> None:
    """Clips grads found in `optimizer.param_groups` to given value in place.

    Ensures the norm of the gradients for each variable is clipped to
    `clip_val`.

    Args:
        optimizer: The torch.optim.Optimizer to get the variables from.
        clip_val: The global norm clip value. Will clip around -clip_val and
            +clip_val.
    """
    # Loop through optimizer's variables and norm per variable.
    for param_group in optimizer.param_groups:
        for p in param_group["params"]:
            if p.grad is not None:
                torch.nn.utils.clip_grad_norm_(p.grad, clip_val)


@PublicAPI
def one_hot(x: TensorType, space: gym.Space) -> TensorType:
    """Returns a one-hot tensor, given and int tensor and a space.

    Handles the MultiDiscrete case as well.

    Args:
        x: The input tensor.
        space: The space to use for generating the one-hot tensor.

    Returns:
        The resulting one-hot tensor.

    Raises:
        ValueError: If the given space is not a discrete one.

    Examples:
        >>> import torch
        >>> import gymnasium as gym
        >>> from ray.rllib.utils.torch_utils import one_hot
        >>> x = torch.IntTensor([0, 3])  # batch-dim=2
        >>> # Discrete space with 4 (one-hot) slots per batch item.
        >>> s = gym.spaces.Discrete(4)
        >>> one_hot(x, s) # doctest: +SKIP
        tensor([[1, 0, 0, 0], [0, 0, 0, 1]])
        >>> x = torch.IntTensor([[0, 1, 2, 3]])  # batch-dim=1
        >>> # MultiDiscrete space with 5 + 4 + 4 + 7 = 20 (one-hot) slots
        >>> # per batch item.
        >>> s = gym.spaces.MultiDiscrete([5, 4, 4, 7])
        >>> one_hot(x, s) # doctest: +SKIP
        tensor([[1, 0, 0, 0, 0,
                 0, 1, 0, 0,
                 0, 0, 1, 0,
                 0, 0, 0, 1, 0, 0, 0]])
    """
    if isinstance(space, Discrete):
        return nn.functional.one_hot(x.long(), space.n)
    elif isinstance(space, MultiDiscrete):
        if isinstance(space.nvec[0], np.ndarray):
            nvec = np.ravel(space.nvec)
            x = x.reshape(x.shape[0], -1)
        else:
            nvec = space.nvec
        return torch.cat(
            [nn.functional.one_hot(x[:, i].long(), n) for i, n in enumerate(nvec)],
            dim=-1,
        )
    else:
        raise ValueError("Unsupported space for `one_hot`: {}".format(space))


@PublicAPI
def reduce_mean_ignore_inf(x: TensorType, axis: Optional[int] = None) -> TensorType:
    """Same as torch.mean() but ignores -inf values.

    Args:
        x: The input tensor to reduce mean over.
        axis: The axis over which to reduce. None for all axes.

    Returns:
        The mean reduced inputs, ignoring inf values.
    """
    mask = torch.ne(x, float("-inf"))
    x_zeroed = torch.where(mask, x, torch.zeros_like(x))
    return torch.sum(x_zeroed, axis) / torch.sum(mask.float(), axis)


@PublicAPI
def sequence_mask(
    lengths: TensorType,
    maxlen: Optional[int] = None,
    dtype=None,
    time_major: bool = False,
) -> TensorType:
    """Offers same behavior as tf.sequence_mask for torch.

    Thanks to Dimitris Papatheodorou
    (https://discuss.pytorch.org/t/pytorch-equivalent-for-tf-sequence-mask/
    39036).

    Args:
        lengths: The tensor of individual lengths to mask by.
        maxlen: The maximum length to use for the time axis. If None, use
            the max of `lengths`.
        dtype: The torch dtype to use for the resulting mask.
        time_major: Whether to return the mask as [B, T] (False; default) or
            as [T, B] (True).

    Returns:
         The sequence mask resulting from the given input and parameters.
    """
    # If maxlen not given, use the longest lengths in the `lengths` tensor.
    if maxlen is None:
        maxlen = int(lengths.max())

    mask = ~(
        torch.ones((len(lengths), maxlen)).to(lengths.device).cumsum(dim=1).t()
        > lengths
    )
    # Time major transformation.
    if not time_major:
        mask = mask.t()

    # By default, set the mask to be boolean.
    mask.type(dtype or torch.bool)

    return mask


@DeveloperAPI
def warn_if_infinite_kl_divergence(
    policy: "TorchPolicy",
    kl_divergence: TensorType,
) -> None:
    if policy.loss_initialized() and kl_divergence.isinf():
        logger.warning(
            "KL divergence is non-finite, this will likely destabilize your model and"
            " the training process. Action(s) in a specific state have near-zero"
            " probability. This can happen naturally in deterministic environments"
            " where the optimal policy has zero mass for a specific action. To fix this"
            " issue, consider setting the coefficient for the KL loss term to zero or"
            " increasing policy entropy."
        )


@PublicAPI
def set_torch_seed(seed: Optional[int] = None) -> None:
    """Sets the torch random seed to the given value.

    Args:
        seed: The seed to use or None for no seeding.
    """
    if seed is not None and torch:
        torch.manual_seed(seed)
        # See https://github.com/pytorch/pytorch/issues/47672.
        cuda_version = torch.version.cuda
        if cuda_version is not None and float(torch.version.cuda) >= 10.2:
            os.environ["CUBLAS_WORKSPACE_CONFIG"] = "4096:8"
        else:
            # Not all Operations support this.
            torch.use_deterministic_algorithms(True)
        # This is only for Convolution no problem.
        torch.backends.cudnn.deterministic = True


@PublicAPI
def softmax_cross_entropy_with_logits(
    logits: TensorType,
    labels: TensorType,
) -> TensorType:
    """Same behavior as tf.nn.softmax_cross_entropy_with_logits.

    Args:
        x: The input predictions.
        labels: The labels corresponding to `x`.

    Returns:
        The resulting softmax cross-entropy given predictions and labels.
    """
    return torch.sum(-labels * nn.functional.log_softmax(logits, -1), -1)


@PublicAPI
class Swish(nn.Module):
    def __init__(self):
        super().__init__()
        self._beta = nn.Parameter(torch.tensor(1.0))

    def forward(self, input_tensor):
        return input_tensor * torch.sigmoid(self._beta * input_tensor)
