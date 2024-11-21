import torch

from torchvision import tv_tensors

from torchvision.utils import _log_api_usage_once

from ._utils import _get_kernel, _register_kernel_internal


def uniform_temporal_subsample(inpt: torch.Tensor, num_samples: int) -> torch.Tensor:
    """See :class:`~torchvision.transforms.v2.UniformTemporalSubsample` for details."""
    if torch.jit.is_scripting():
        return uniform_temporal_subsample_video(inpt, num_samples=num_samples)

    _log_api_usage_once(uniform_temporal_subsample)

    kernel = _get_kernel(uniform_temporal_subsample, type(inpt))
    return kernel(inpt, num_samples=num_samples)


@_register_kernel_internal(uniform_temporal_subsample, torch.Tensor)
@_register_kernel_internal(uniform_temporal_subsample, tv_tensors.Video)
def uniform_temporal_subsample_video(video: torch.Tensor, num_samples: int) -> torch.Tensor:
    # Reference: https://github.com/facebookresearch/pytorchvideo/blob/a0a131e/pytorchvideo/transforms/functional.py#L19
    t_max = video.shape[-4] - 1
    indices = torch.linspace(0, t_max, num_samples, device=video.device).long()
    return torch.index_select(video, -4, indices)
