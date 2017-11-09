#!python
# cython: embedsignature=True, binding=True

#  Copyright 2016 Intel Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from libc.math cimport log
from numbers import Integral, Real
from typing import TypeVar, Union

import numpy as np
cimport numpy as np

T = TypeVar("T", bound=Real)

def masked_log(x):
    """Compute natural logarithm while accepting nonpositive input

    For nonpositive elements, return -inf.
    Output type is the same as input type, except when input is integral, in
    which case output is `np.float64`.

    Parameters
    ----------
    x: ndarray[T]

    Returns
    -------
    ndarray[Union[T, np.float64]]
    """
    if issubclass(x.dtype.type, Integral):
        out_type = np.float64
    else:
        out_type = x.dtype
    y = np.empty(x.shape, dtype=out_type)
    lim = x.shape[0]
    for i in range(lim):
        if x[i] <= 0:
            y[i] = float("-inf")
        else:
            y[i] = log(x[i])
    return y
