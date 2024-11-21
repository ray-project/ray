import torch
import torch.fx
import torch.nn.functional as F
from torch import nn, Tensor

from ..utils import _log_api_usage_once


def drop_block2d(
    input: Tensor, p: float, block_size: int, inplace: bool = False, eps: float = 1e-06, training: bool = True
) -> Tensor:
    """
    Implements DropBlock2d from `"DropBlock: A regularization method for convolutional networks"
    <https://arxiv.org/abs/1810.12890>`.

    Args:
        input (Tensor[N, C, H, W]): The input tensor or 4-dimensions with the first one
                    being its batch i.e. a batch with ``N`` rows.
        p (float): Probability of an element to be dropped.
        block_size (int): Size of the block to drop.
        inplace (bool): If set to ``True``, will do this operation in-place. Default: ``False``.
        eps (float): A value added to the denominator for numerical stability. Default: 1e-6.
        training (bool): apply dropblock if is ``True``. Default: ``True``.

    Returns:
        Tensor[N, C, H, W]: The randomly zeroed tensor after dropblock.
    """
    if not torch.jit.is_scripting() and not torch.jit.is_tracing():
        _log_api_usage_once(drop_block2d)
    if p < 0.0 or p > 1.0:
        raise ValueError(f"drop probability has to be between 0 and 1, but got {p}.")
    if input.ndim != 4:
        raise ValueError(f"input should be 4 dimensional. Got {input.ndim} dimensions.")
    if not training or p == 0.0:
        return input

    N, C, H, W = input.size()
    block_size = min(block_size, W, H)
    # compute the gamma of Bernoulli distribution
    gamma = (p * H * W) / ((block_size**2) * ((H - block_size + 1) * (W - block_size + 1)))
    noise = torch.empty((N, C, H - block_size + 1, W - block_size + 1), dtype=input.dtype, device=input.device)
    noise.bernoulli_(gamma)

    noise = F.pad(noise, [block_size // 2] * 4, value=0)
    noise = F.max_pool2d(noise, stride=(1, 1), kernel_size=(block_size, block_size), padding=block_size // 2)
    noise = 1 - noise
    normalize_scale = noise.numel() / (eps + noise.sum())
    if inplace:
        input.mul_(noise).mul_(normalize_scale)
    else:
        input = input * noise * normalize_scale
    return input


def drop_block3d(
    input: Tensor, p: float, block_size: int, inplace: bool = False, eps: float = 1e-06, training: bool = True
) -> Tensor:
    """
    Implements DropBlock3d from `"DropBlock: A regularization method for convolutional networks"
    <https://arxiv.org/abs/1810.12890>`.

    Args:
        input (Tensor[N, C, D, H, W]): The input tensor or 5-dimensions with the first one
                    being its batch i.e. a batch with ``N`` rows.
        p (float): Probability of an element to be dropped.
        block_size (int): Size of the block to drop.
        inplace (bool): If set to ``True``, will do this operation in-place. Default: ``False``.
        eps (float): A value added to the denominator for numerical stability. Default: 1e-6.
        training (bool): apply dropblock if is ``True``. Default: ``True``.

    Returns:
        Tensor[N, C, D, H, W]: The randomly zeroed tensor after dropblock.
    """
    if not torch.jit.is_scripting() and not torch.jit.is_tracing():
        _log_api_usage_once(drop_block3d)
    if p < 0.0 or p > 1.0:
        raise ValueError(f"drop probability has to be between 0 and 1, but got {p}.")
    if input.ndim != 5:
        raise ValueError(f"input should be 5 dimensional. Got {input.ndim} dimensions.")
    if not training or p == 0.0:
        return input

    N, C, D, H, W = input.size()
    block_size = min(block_size, D, H, W)
    # compute the gamma of Bernoulli distribution
    gamma = (p * D * H * W) / ((block_size**3) * ((D - block_size + 1) * (H - block_size + 1) * (W - block_size + 1)))
    noise = torch.empty(
        (N, C, D - block_size + 1, H - block_size + 1, W - block_size + 1), dtype=input.dtype, device=input.device
    )
    noise.bernoulli_(gamma)

    noise = F.pad(noise, [block_size // 2] * 6, value=0)
    noise = F.max_pool3d(
        noise, stride=(1, 1, 1), kernel_size=(block_size, block_size, block_size), padding=block_size // 2
    )
    noise = 1 - noise
    normalize_scale = noise.numel() / (eps + noise.sum())
    if inplace:
        input.mul_(noise).mul_(normalize_scale)
    else:
        input = input * noise * normalize_scale
    return input


torch.fx.wrap("drop_block2d")


class DropBlock2d(nn.Module):
    """
    See :func:`drop_block2d`.
    """

    def __init__(self, p: float, block_size: int, inplace: bool = False, eps: float = 1e-06) -> None:
        super().__init__()

        self.p = p
        self.block_size = block_size
        self.inplace = inplace
        self.eps = eps

    def forward(self, input: Tensor) -> Tensor:
        """
        Args:
            input (Tensor): Input feature map on which some areas will be randomly
                dropped.
        Returns:
            Tensor: The tensor after DropBlock layer.
        """
        return drop_block2d(input, self.p, self.block_size, self.inplace, self.eps, self.training)

    def __repr__(self) -> str:
        s = f"{self.__class__.__name__}(p={self.p}, block_size={self.block_size}, inplace={self.inplace})"
        return s


torch.fx.wrap("drop_block3d")


class DropBlock3d(DropBlock2d):
    """
    See :func:`drop_block3d`.
    """

    def __init__(self, p: float, block_size: int, inplace: bool = False, eps: float = 1e-06) -> None:
        super().__init__(p, block_size, inplace, eps)

    def forward(self, input: Tensor) -> Tensor:
        """
        Args:
            input (Tensor): Input feature map on which some areas will be randomly
                dropped.
        Returns:
            Tensor: The tensor after DropBlock layer.
        """
        return drop_block3d(input, self.p, self.block_size, self.inplace, self.eps, self.training)
