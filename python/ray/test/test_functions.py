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


@ray.remote
def keyword_fct1(a, b="hello"):
    return "{} {}".format(a, b)


@ray.remote
def keyword_fct2(a="hello", b="world"):
    return "{} {}".format(a, b)


@ray.remote
def keyword_fct3(a, b, c="hello", d="world"):
    return "{} {} {} {}".format(a, b, c, d)

# Test variable numbers of arguments


@ray.remote
def varargs_fct1(*a):
    return " ".join(map(str, a))


@ray.remote
def varargs_fct2(a, *b):
    return " ".join(map(str, b))


try:
    @ray.remote
    def kwargs_throw_exception(**c):
        return ()
    kwargs_exception_thrown = False
except Exception:
    kwargs_exception_thrown = True

try:
    @ray.remote
    def varargs_and_kwargs_throw_exception(a, b="hi", *c):
        return "{} {} {}".format(a, b, c)
    varargs_and_kwargs_exception_thrown = False
except Exception:
    varargs_and_kwargs_exception_thrown = True

# test throwing an exception


@ray.remote
def throw_exception_fct1():
    raise Exception("Test function 1 intentionally failed.")


@ray.remote
def throw_exception_fct2():
    raise Exception("Test function 2 intentionally failed.")


@ray.remote(num_return_vals=3)
def throw_exception_fct3(x):
    raise Exception("Test function 3 intentionally failed.")

# test Python mode


@ray.remote
def python_mode_f():
    return np.array([0, 0])


@ray.remote
def python_mode_g(x):
    x[0] = 1
    return x

# test no return values


@ray.remote
def no_op():
    pass
