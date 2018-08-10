from __future__ import division, print_function
import numpy as np


def cov_iid(x, z=None, scale=1):
    """Identity kernel, scaled, for noise"""
    if z is None:
        z = x

    x = np.array(x)
    z = np.array(z)

    if x.ndim == 2:
        if z.shape[1] != x.shape[1]:
            raise Exception("Kernel error: input dimension does not match.")
    # Create kernel
    K = np.zeros((len(x), len(z)))
    if not np.all(x == z):
        return K
    for i in range(min(len(x), len(z))):
        K[i, i] = scale
    return K


def cov_matern_5_2(x, z=None, scale=1, ell=1):
    """Matern52 kernel, scaled"""
    if z is None:
        z = x
    # Check N by d
    x = np.array(x, ndmin=2)
    z = np.array(z, ndmin=2)
    if x.ndim != 2 or x.shape[1] != len(ell):
        raise Exception("Kernel error: first input dimension wrong.")
    if z.ndim != 2 or z.shape[1] != x.shape[1]:
        raise Exception("Kernel error: second input dimension wrong.")
    # Create kernel
    x = x * np.sqrt(5) / ell
    z = z * np.sqrt(5) / ell
    sqdist = np.sum(x ** 2, 1).reshape(-1, 1) + np.sum(z ** 2, 1) - 2 * np.dot(x, z.T)
    K = sqdist
    f = lambda a: 1 + a * (1 + a / 3)
    m = lambda b: f(b) * np.exp(-b)
    for i in range(len(K)):
        for j in range(len(K[i])):
            K[i, j] = m(K[i, j])
    K *= scale
    return K


def ft_K_t_t(t, t_star, scale, alpha, beta, tscale=1.0):
    """Exponential decay mixture kernel
    https://arxiv.org/pdf/1406.3896.pdf
    """

    t = np.array(t)
    t_star = np.array(t_star)
    if t.ndim != 1 and (t.ndim != 2 or t.shape[1] != 1):
        raise Exception("Kernel error: input dimension wrong.")
    if t_star.ndim != 1 and (t_star.ndim != 2 or t_star.shape[1] != 1):
        raise Exception("Kernel error: t_star input dimension wrong.")
    # Create kernel
    K_t_t = np.zeros((len(t), len(t_star)))
    for i in range(len(t)):
        for j in range(len(t_star)):
            K_t_t[i, j] = scale * ((beta * tscale) ** alpha) / \
                          ((t[i] + t_star[j] + beta * tscale) ** alpha)
    return K_t_t


def ft_K_t_t_plus_noise(t, t_star, scale, alpha, beta, log_noise, tscale):
    noise = np.exp(log_noise)
    K_t_t = ft_K_t_t(t, t_star, scale=scale, alpha=alpha, beta=beta, tscale=tscale)
    K_noise = cov_iid(t, t_star, scale=noise)
    return K_t_t + K_noise
