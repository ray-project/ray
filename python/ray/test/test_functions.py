from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

import numpy as np

# Test simple functionality


@ray.remote(num_return_vals=2)
def handle_int(a, b):
    return a + 1, b + 1


# Test timing


@ray.remote
def empty_function():
    pass


@ray.remote
def trivial_function():
    return 1


# Test keyword arguments





# Test variable numbers of arguments


# test throwing an exception



# test Python mode





# test no return values
