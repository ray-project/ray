import numpy as np

from ray.rllib.utils import force_list
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


def dreamerv3_normal_initializer(parameters):
    """From Danijar Hafner's DreamerV3 JAX repo.

    Used on any layer whenever the config for that layer has `winit="normal"`.

    Note: Not identical with Glorot normal. Differs in the std computation
    glorot_std = sqrt(2/(fanin+fanout))
    this_std = sqrt(1/AVG(fanin, fanout)) / [somemagicnumber=0.879...]
    """
    for param in force_list(parameters):
        if param.dim() > 1:
            fanin, fanout = _fans(param.shape)
            scale = 1.0 / np.mean([fanin, fanout])
            std = np.sqrt(scale) / 0.87962566103423978
            with torch.no_grad():
                param.normal_(0, std)
                param.clamp_(-2, 2)


def _fans(shape):
    if len(shape) == 0:
        return 1, 1
    elif len(shape) == 1:
        return shape[0], shape[0]
    elif len(shape) == 2:
        return shape
    else:
        space = int(np.prod(shape[:-2]))
        return shape[-2] * space, shape[-1] * space
