""" Utilities to aid in building SAC implementation """
import ray


def using_ray_8():
    """Returns True if ray version is 0.8.1"""
    return ray.__version__ == '0.8.1'


def using_ray_6():
    """Returns True if ray version is 0.8.1"""
    return ray.__version__ == '0.6.6'
