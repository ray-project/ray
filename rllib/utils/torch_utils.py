import logging
import os
import warnings
from typing import Dict, List, Optional, TYPE_CHECKING, Union

import gymnasium as gym
from gymnasium.spaces import Discrete, MultiDiscrete
import numpy as np
from packaging import version
import tree  # pip install dm_tree

from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI, OldAPIStack
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import SMALL_NUMBER
from ray.rllib.utils.typing import (
    LocalOptimizer,
    NetworkType,
    SpaceStruct,
    TensorStructType,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner import ParamDict, ParamList
    from ray.rllib.policy.torch_policy import TorchPolicy
    from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2

logger = logging.getLogger(__name__)
torch, nn = try_import_torch()

# Limit values suitable for use as close to a -inf logit. These are useful
# since -inf / inf cause NaNs during backprop.
FLOAT_MIN = -3.4e38
FLOAT_MAX = 3.4e38

if torch:
    TORCH_COMPILE_REQUIRED_VERSION = version.parse("2.0.0")
else:
    TORCH_COMPILE_REQUIRED_VERSION = ValueError(
        "torch is not installed. TORCH_COMPILE_REQUIRED_VERSION is not defined."
    )


@OldAPIStack
def apply_grad_clipping(
    policy: "TorchPolicy", optimizer: LocalOptimizer, loss: TensorType
) -> Dict[str, TensorType]:
    """Applies gradient clipping to already computed grads inside `optimizer`.

    Note: This function does NOT perform an analogous operation as
    tf.clip_by_global_norm. It merely clips by norm (per gradient tensor) and
    then computes the global norm across all given tensors (but without clipping
    by that global norm).

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


@PublicAPI
def clip_gradients(
    gradients_dict: "ParamDict",
    *,
    grad_clip: Optional[float] = None,
    grad_clip_by: str = "value",
) -> TensorType:
    """Performs gradient clipping on a grad-dict based on a clip value and clip mode.

    Changes the provided gradient dict in place.

    Args:
        gradients_dict: The gradients dict, mapping str to gradient tensors.
        grad_clip: The value to clip with. The way gradients are clipped is defined
            by the `grad_clip_by` arg (see below).
        grad_clip_by: One of 'value', 'norm', or 'global_norm'.

    Returns:
        If `grad_clip_by`="global_norm" and `grad_clip` is not None, returns the global
        norm of all tensors, otherwise returns None.
    """
    # No clipping, return.
    if grad_clip is None:
        return

    if grad_clip_by not in ["value", "norm", "global_norm"]:
        raise ValueError(
            f"`grad_clip_by` ({grad_clip_by}) must be one of [value|norm|global_norm]!"
        )

    # Clip by value (each gradient individually).
    if grad_clip_by == "value":
        for k, v in gradients_dict.items():
            gradients_dict[k] = (
                None if v is None else torch.clip(v, -grad_clip, grad_clip)
            )

    # Clip by L2-norm (per gradient tensor).
    elif grad_clip_by == "norm":
        for k, v in gradients_dict.items():
            if v is not None:
                # Compute the L2-norm of the gradient tensor.
                norm = v.norm(2).nan_to_num(neginf=-10e8, posinf=10e8)
                # Clip all the gradients.
                if norm > grad_clip:
                    v.mul_(grad_clip / norm)

    # Clip by global L2-norm (across all gradient tensors).
    else:
        gradients_list = list(gradients_dict.values())
        total_norm = compute_global_norm(gradients_list)
        if len(gradients_list) == 0:
            return total_norm
        # We do want the coefficient to be in between 0.0 and 1.0, therefore
        # if the global_norm is smaller than the clip value, we use the clip value
        # as normalization constant.
        clip_coeff = grad_clip / torch.clamp(total_norm + 1e-6, min=grad_clip)
        # Note: multiplying by the clamped coefficient is redundant when the coefficient
        # is clamped to 1, but doing so avoids a `if clip_coeff < 1:` conditional which
        # can require a CPU <=> device synchronization when the gradients reside in GPU
        # memory.
        clip_coeff_clamped = torch.clamp(clip_coeff, max=1.0)
        for g in gradients_list:
            if g is not None:
                g.detach().mul_(clip_coeff_clamped.to(g.device))
        return total_norm


@PublicAPI
def compute_global_norm(gradients_list: "ParamList") -> TensorType:
    """Computes the global norm for a gradients dict.

    Args:
        gradients_list: The gradients list containing parameters.

    Returns:
        Returns the global norm of all tensors in `gradients_list`.
    """
    # Define the norm type to be L2.
    norm_type = 2.0
    # If we have no grads, return zero.
    if len(gradients_list) == 0:
        return torch.tensor(0.0)

    # Compute the global norm.
    total_norm = torch.norm(
        torch.stack(
            [
                torch.norm(g.detach(), norm_type)
                # Note, we want to avoid overflow in the norm computation, this does
                # not affect the gradients themselves as we clamp by multiplying and
                # not by overriding tensor values.
                .nan_to_num(neginf=-10e8, posinf=10e8)
                for g in gradients_list
                if g is not None
            ]
        ),
        norm_type,
    ).nan_to_num(neginf=-10e8, posinf=10e8)

    # Return the global norm.
    return total_norm


@OldAPIStack
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


@PublicAPI
def convert_to_torch_tensor(
    x,
    device: Optional[str] = None,
    pin_memory: bool = False,
    use_stream: bool = False,
    stream: Optional[Union["torch.cuda.Stream", "torch.cuda.classes.Stream"]] = None,
):
    """
    Converts any (possibly nested) structure to torch.Tensors.

    Args:
        x: The input structure whose leaves will be converted.
        device: The device to create the tensor on (e.g. "cuda:0" or "cpu").
        pin_memory: If True, calls `pin_memory()` on the created tensors.
        use_stream: If True, uses a separate CUDA stream for `Tensor.to()`.
        stream: An optional CUDA stream for the host-to-device copy in `Tensor.to()`.

    Returns:
        A new structure with the same layout as `x` but with all leaves converted
        to torch.Tensors. Leaves that are None are left unchanged.
    """

    # Convert the provided device (if any) to a torch.device; default to CPU.
    device = torch.device(device) if device is not None else torch.device("cpu")
    is_cuda = (device.type == "cuda") and torch.cuda.is_available()

    # Determine the appropriate stream.
    if is_cuda:
        if use_stream:
            if stream is not None:
                # Ensure the provided stream is of an acceptable type.
                assert isinstance(
                    stream, (torch.cuda.Stream, torch.cuda.classes.Stream)
                ), f"`stream` must be a torch.cuda.Stream but got {type(stream)}."
            else:
                stream = torch.cuda.Stream()
        else:
            stream = torch.cuda.default_stream(device=device)
    else:
        stream = None

    def mapping(item):
        # Pass through None values.
        if item is None:
            return item

        # Special handling for "RepeatedValues" types.
        if isinstance(item, RepeatedValues):
            return RepeatedValues(
                tree.map_structure(mapping, item.values),
                item.lengths,
                item.max_len,
            )

        # Convert to a tensor if not already one.
        if torch.is_tensor(item):
            tensor = item
        elif isinstance(item, np.ndarray):
            # Leave object or string arrays as is.
            if item.dtype == object or item.dtype.type is np.str_:
                return item
            # If the numpy array is not writable, suppress warnings.
            if not item.flags.writeable:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    tensor = torch.from_numpy(item)
            else:
                tensor = torch.from_numpy(item)
        else:
            tensor = torch.from_numpy(np.asarray(item))

        # Convert floating-point tensors from float64 to float32 (unless they are float16).
        if tensor.is_floating_point() and tensor.dtype != torch.float16:
            tensor = tensor.float()

        # Optionally pin memory for faster host-to-GPU copies.
        if pin_memory and is_cuda:
            tensor = tensor.pin_memory()

        # Move the tensor to the desired device.
        # For CUDA devices, use the provided stream context if available.
        if is_cuda:
            if stream is not None:
                with torch.cuda.stream(stream):
                    tensor = tensor.to(device, non_blocking=True)
            else:
                tensor = tensor.to(device, non_blocking=True)
        else:
            # For CPU (or non-CUDA), this is a no-op if already on the target device.
            tensor = tensor.to(device)

        return tensor

    return tree.map_structure(mapping, x)


@PublicAPI
def copy_torch_tensors(x: TensorStructType, device: Optional[str] = None):
    """Creates a copy of `x` and makes deep copies torch.Tensors in x.

    Also moves the copied tensors to the specified device (if not None).

    Note if an object in x is not a torch.Tensor, it will be shallow-copied.

    Args:
        x : Any (possibly nested) struct possibly containing torch.Tensors.
        device : The device to move the tensors to.

    Returns:
        Any: A new struct with the same structure as `x`, but with all
            torch.Tensors deep-copied and moved to the specified device.

    """

    def mapping(item):
        if isinstance(item, torch.Tensor):
            return (
                torch.clone(item.detach())
                if device is None
                else item.detach().to(device)
            )
        else:
            return item

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
    squeezed_y = y.squeeze()
    y_var = torch.var(squeezed_y, dim=0)
    diff_var = torch.var(squeezed_y - pred.squeeze(), dim=0)
    min_ = torch.tensor([-1.0]).to(pred.device)
    return torch.max(min_, 1 - (diff_var / (y_var + SMALL_NUMBER)))[0]


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

    .. testcode::

        from gymnasium.spaces import Discrete, Box
        from ray.rllib.utils.torch_utils import flatten_inputs_to_1d_tensor
        import torch
        struct = {
            "a": np.array([1, 3]),
            "b": (
                np.array([[1.0, 2.0], [4.0, 5.0]]),
                np.array(
                    [[[8.0], [7.0]], [[5.0], [4.0]]]
                ),
            ),
                "c": {
                    "cb": np.array([1.0, 2.0]),
                },
        }
        struct_torch = tree.map_structure(lambda s: torch.from_numpy(s), struct)
        spaces = dict(
            {
                "a": gym.spaces.Discrete(4),
                "b": (gym.spaces.Box(-1.0, 10.0, (2,)), gym.spaces.Box(-1.0, 1.0, (2,
                        1))),
                "c": dict(
                    {
                        "cb": gym.spaces.Box(-1.0, 1.0, ()),
                    }
                ),
            }
        )
        print(flatten_inputs_to_1d_tensor(struct_torch, spaces_struct=spaces))

    .. testoutput::

        tensor([[0., 1., 0., 0., 1., 2., 8., 7., 1.],
                [0., 0., 0., 1., 4., 5., 5., 4., 2.]])

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


@OldAPIStack
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


@OldAPIStack
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

    .. testcode::

        import torch
        import gymnasium as gym
        from ray.rllib.utils.torch_utils import one_hot
        x = torch.IntTensor([0, 3])  # batch-dim=2
        # Discrete space with 4 (one-hot) slots per batch item.
        s = gym.spaces.Discrete(4)
        print(one_hot(x, s))
        x = torch.IntTensor([[0, 1, 2, 3]])  # batch-dim=1
        # MultiDiscrete space with 5 + 4 + 4 + 7 = 20 (one-hot) slots
        # per batch item.
        s = gym.spaces.MultiDiscrete([5, 4, 4, 7])
        print(one_hot(x, s))

    .. testoutput::

        tensor([[1, 0, 0, 0],
                [0, 0, 0, 1]])
        tensor([[1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0]])
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
        maxlen = lengths.max()

    mask = torch.ones(tuple(lengths.shape) + (int(maxlen),))

    mask = ~(mask.to(lengths.device).cumsum(dim=1).t() > lengths)
    # Time major transformation.
    if not time_major:
        mask = mask.t()

    # By default, set the mask to be boolean.
    mask.type(dtype or torch.bool)

    return mask


@PublicAPI
def update_target_network(
    main_net: NetworkType,
    target_net: NetworkType,
    tau: float,
) -> None:
    """Updates a torch.nn.Module target network using Polyak averaging.

    .. code-block:: text

        new_target_net_weight = (
            tau * main_net_weight + (1.0 - tau) * current_target_net_weight
        )

    Args:
        main_net: The nn.Module to update from.
        target_net: The target network to update.
        tau: The tau value to use in the Polyak averaging formula.
    """
    # Get the current parameters from the Q network.
    state_dict = main_net.state_dict()
    # Use here Polyak averaging.
    new_state_dict = {
        k: tau * state_dict[k] + (1 - tau) * v
        for k, v in target_net.state_dict().items()
    }
    # Apply the new parameters to the target Q network.
    target_net.load_state_dict(new_state_dict)


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
            # See https://docs.nvidia.com/cuda/cublas/index.html#results-reproducibility.
            os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
            torch.cuda.manual_seed(seed)
            torch.cuda.manual_seed_all(seed)  # if using multi-GPU
        else:
            if version.Version(torch.__version__) >= version.Version("1.8.0"):
                # Not all Operations support this.
                torch.use_deterministic_algorithms(True)
            else:
                torch.set_deterministic(True)
        # This is only for Convolution no problem.
        torch.backends.cudnn.deterministic = True
        # For benchmark=True, CuDNN may choose different algorithms depending on runtime
        # conditions or slight differences in input sizes, even if the seed is fixed,
        # which breaks determinism.
        torch.backends.cudnn.benchmark = False


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
def symlog(x: "torch.Tensor") -> "torch.Tensor":
    """The symlog function as described in [1]:

    [1] Mastering Diverse Domains through World Models - 2023
    D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
    https://arxiv.org/pdf/2301.04104v1.pdf
    """
    return torch.sign(x) * torch.log(torch.abs(x) + 1)


@PublicAPI
def inverse_symlog(y: "torch.Tensor") -> "torch.Tensor":
    """Inverse of the `symlog` function as desribed in [1]:

    [1] Mastering Diverse Domains through World Models - 2023
    D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
    https://arxiv.org/pdf/2301.04104v1.pdf
    """
    # To get to symlog inverse, we solve the symlog equation for x:
    #     y = sign(x) * log(|x| + 1)
    # <=> y / sign(x) = log(|x| + 1)
    # <=> y =  log( x + 1) V x >= 0
    #    -y =  log(-x + 1) V x <  0
    # <=> exp(y)  =  x + 1  V x >= 0
    #     exp(-y) = -x + 1  V x <  0
    # <=> exp(y)  - 1 =  x   V x >= 0
    #     exp(-y) - 1 = -x   V x <  0
    # <=>  exp(y)  - 1 = x   V x >= 0 (if x >= 0, then y must also be >= 0)
    #     -exp(-y) - 1 = x   V x <  0 (if x < 0, then y must also be < 0)
    # <=> sign(y) * (exp(|y|) - 1) = x
    return torch.sign(y) * (torch.exp(torch.abs(y)) - 1)


@PublicAPI
def two_hot(
    value: "torch.Tensor",
    num_buckets: int = 255,
    lower_bound: float = -20.0,
    upper_bound: float = 20.0,
    device: Optional[str] = None,
):
    """Returns a two-hot vector of dim=num_buckets with two entries that are non-zero.

    See [1] for more details:
    [1] Mastering Diverse Domains through World Models - 2023
    D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
    https://arxiv.org/pdf/2301.04104v1.pdf

    Entries in the vector represent equally sized buckets within some fixed range
    (`lower_bound` to `upper_bound`).
    Those entries not 0.0 at positions k and k+1 encode the actual `value` and sum
    up to 1.0. They are the weights multiplied by the buckets values at k and k+1 for
    retrieving `value`.

    Example:
        num_buckets=11
        lower_bound=-5
        upper_bound=5
        value=2.5
        -> [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0]
        -> [-5   -4   -3   -2   -1   0    1    2    3    4    5] (0.5*2 + 0.5*3=2.5)

    Example:
        num_buckets=5
        lower_bound=-1
        upper_bound=1
        value=0.1
        -> [0.0, 0.0, 0.8, 0.2, 0.0]
        -> [-1  -0.5   0   0.5   1] (0.2*0.5 + 0.8*0=0.1)

    Args:
        value: The input tensor of shape (B,) to be two-hot encoded.
        num_buckets: The number of buckets to two-hot encode into.
        lower_bound: The lower bound value used for the encoding. If input values are
            lower than this boundary, they will be encoded as `lower_bound`.
        upper_bound: The upper bound value used for the encoding. If input values are
            higher than this boundary, they will be encoded as `upper_bound`.

    Returns:
        The two-hot encoded tensor of shape (B, num_buckets).
    """
    # First make sure, values are clipped.
    value = torch.clamp(value, lower_bound, upper_bound)
    # Tensor of batch indices: [0, B=batch size).
    batch_indices = torch.arange(0, value.shape[0], device=device).float()
    # Calculate the step deltas (how much space between each bucket's central value?).
    bucket_delta = (upper_bound - lower_bound) / (num_buckets - 1)
    # Compute the float indices (might be non-int numbers: sitting between two buckets).
    idx = (-lower_bound + value) / bucket_delta
    # k
    k = torch.floor(idx)
    # k+1
    kp1 = torch.ceil(idx)
    # In case k == kp1 (idx is exactly on the bucket boundary), move kp1 up by 1.0.
    # Otherwise, this would result in a NaN in the returned two-hot tensor.
    kp1 = torch.where(k.eq(kp1), kp1 + 1.0, kp1)
    # Iff `kp1` is one beyond our last index (because incoming value is larger than
    # `upper_bound`), move it to one before k (kp1's weight is going to be 0.0 anyways,
    # so it doesn't matter where it points to; we are just avoiding an index error
    # with this).
    kp1 = torch.where(kp1.eq(num_buckets), kp1 - 2.0, kp1)
    # The actual values found at k and k+1 inside the set of buckets.
    values_k = lower_bound + k * bucket_delta
    values_kp1 = lower_bound + kp1 * bucket_delta
    # Compute the two-hot weights (adding up to 1.0) to use at index k and k+1.
    weights_k = (value - values_kp1) / (values_k - values_kp1)
    weights_kp1 = 1.0 - weights_k
    # Compile a tensor of full paths (indices from batch index to feature index) to
    # use for the scatter_nd op.
    indices_k = torch.stack([batch_indices, k], dim=-1)
    indices_kp1 = torch.stack([batch_indices, kp1], dim=-1)
    indices = torch.cat([indices_k, indices_kp1], dim=0).long()
    # The actual values (weights adding up to 1.0) to place at the computed indices.
    updates = torch.cat([weights_k, weights_kp1], dim=0)
    # Call the actual scatter update op, returning a zero-filled tensor, only changed
    # at the given indices.
    output = torch.zeros(value.shape[0], num_buckets, device=device)
    # Set our two-hot values at computed indices.
    output[indices[:, 0], indices[:, 1]] = updates
    return output


def _dynamo_is_available():
    # This only works if torch._dynamo is available
    try:
        # TODO(Artur): Remove this once torch._dynamo is available on CI
        import torch._dynamo as dynamo  # noqa: F401

        return True
    except ImportError:
        return False
