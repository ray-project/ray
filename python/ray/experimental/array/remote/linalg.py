import numpy as np
import ray

__all__ = [
    "matrix_power", "solve", "tensorsolve", "tensorinv", "inv", "cholesky",
    "eigvals", "eigvalsh", "pinv", "slogdet", "det", "svd", "eig", "eigh",
    "lstsq", "norm", "qr", "cond", "matrix_rank", "multi_dot"
]


@ray.remote
def matrix_power(M, n):
    return np.linalg.matrix_power(M, n)


@ray.remote
def solve(a, b):
    return np.linalg.solve(a, b)


@ray.remote(num_returns=2)
def tensorsolve(a):
    raise NotImplementedError


@ray.remote(num_returns=2)
def tensorinv(a):
    raise NotImplementedError


@ray.remote
def inv(a):
    return np.linalg.inv(a)


@ray.remote
def cholesky(a):
    return np.linalg.cholesky(a)


@ray.remote
def eigvals(a):
    return np.linalg.eigvals(a)


@ray.remote
def eigvalsh(a):
    raise NotImplementedError


@ray.remote
def pinv(a):
    return np.linalg.pinv(a)


@ray.remote
def slogdet(a):
    raise NotImplementedError


@ray.remote
def det(a):
    return np.linalg.det(a)


@ray.remote(num_returns=3)
def svd(a):
    return np.linalg.svd(a)


@ray.remote(num_returns=2)
def eig(a):
    return np.linalg.eig(a)


@ray.remote(num_returns=2)
def eigh(a):
    return np.linalg.eigh(a)


@ray.remote(num_returns=4)
def lstsq(a, b):
    return np.linalg.lstsq(a)


@ray.remote
def norm(x):
    return np.linalg.norm(x)


@ray.remote(num_returns=2)
def qr(a):
    return np.linalg.qr(a)


@ray.remote
def cond(x):
    return np.linalg.cond(x)


@ray.remote
def matrix_rank(M):
    return np.linalg.matrix_rank(M)


@ray.remote
def multi_dot(*a):
    raise NotImplementedError
