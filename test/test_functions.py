import ray

import numpy as np

# Test simple functionality

@ray.remote([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

# Test aliasing

@ray.remote([], [np.ndarray])
def test_alias_f():
  return np.ones([3, 4, 5])

@ray.remote([], [np.ndarray])
def test_alias_g():
  return test_alias_f()

@ray.remote([], [np.ndarray])
def test_alias_h():
  return test_alias_g()

# Test timing

@ray.remote([], [])
def empty_function():
  return ()

@ray.remote([], [int])
def trivial_function():
  return 1

# Test keyword arguments

@ray.remote([int, str], [str])
def keyword_fct1(a, b="hello"):
  return "{} {}".format(a, b)

@ray.remote([str, str], [str])
def keyword_fct2(a="hello", b="world"):
  return "{} {}".format(a, b)

@ray.remote([int, int, str, str], [str])
def keyword_fct3(a, b, c="hello", d="world"):
  return "{} {} {} {}".format(a, b, c, d)

# Test variable numbers of arguments

@ray.remote([int], [str])
def varargs_fct1(*a):
  return " ".join(map(str, a))

@ray.remote([int, int], [str])
def varargs_fct2(a, *b):
  return " ".join(map(str, b))

try:
  @ray.remote([int], [])
  def kwargs_throw_exception(**c):
    return ()
  kwargs_exception_thrown = False
except:
  kwargs_exception_thrown = True

try:
  @ray.remote([int, str, int], [str])
  def varargs_and_kwargs_throw_exception(a, b="hi", *c):
    return "{} {} {}".format(a, b, c)
  varargs_and_kwargs_exception_thrown = False
except:
  varargs_and_kwargs_exception_thrown = True

# test throwing an exception

@ray.remote([], [])
def throw_exception_fct():
    raise Exception("Test function intentionally failed.")
