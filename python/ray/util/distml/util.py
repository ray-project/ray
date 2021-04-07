import numpy as np
import cupy as cp

# some operation for this ml system.
def ones(shape, cpu=True):
    if cpu:
        return np.ones(shape)
    else:
        return cp.ones(shape)

def zeros_like(x, cpu=True):
    if cpu:
        return np.ones_like(x)
    else:
        return cp.ones_like(x)

# some operation for this ml system.
def zeros(shape, cpu=True):
    if cpu:
        return np.zeros(shape)
    else:
        return cp.zeros(shape)

def zeros_like(x, cpu=True):
    if cpu:
        return np.zeros_like(x)
    else:
        return cp.zeros_like(x)



def numel(v):
    return np.size(v)