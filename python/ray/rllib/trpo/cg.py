from __future__ import absolute_import, division, print_function

import numpy as np
import ray
import scipy.sparse.linalg
import torch
from torch import Tensor
from torch.autograd import Variable

# TODO does this have to be compatible with autograd?


def cg(A, b, x0=None) -> Tensor:
    """Thin wrapper over scipy's conjugate gradient implementation."""

    def cast(arr) -> np.ndarray:
        if isinstance(arr, Tensor):
            return arr.numpy()
        elif isinstance(arr, Variable):
            return arr.data.numpy()
        # TODO check that this returns a ref
        elif isinstance(arr, np.ndarray) or arr is None:
            return arr
        else:
            raise NotImplementedError

    A, b, x0 = cast(A), cast(b), cast(x0)

    ans, info = scipy.sparse.linalg.cg(A, b, x0)

    ans = torch.from_numpy(ans)
    if info == 0:
        return ans
    elif info > 0:
        print('Convergence to tolerance not achieved')
        return ans
    else:
        raise ValueError('Illegal input or breakdown.')
