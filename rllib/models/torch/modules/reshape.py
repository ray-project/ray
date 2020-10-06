from typing import List

from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class Reshape(nn.Module):
    """Standard module that reshapes/views a tensor
    """

    def __init__(self, shape: List):
        super().__init__()
        self.shape = shape

    def forward(self, x):
        return x.view(*self.shape)
