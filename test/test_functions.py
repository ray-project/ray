import halo

import numpy as np

# Test simple functionality

@halo.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  f = open("asdfasdf.txt", "w")
  f.write("successfully called print_string with argument {}.".format(string))
  return string

@halo.distributed([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

# Test aliasing

@halo.distributed([], [np.ndarray])
def test_alias_f():
  return np.ones([3, 4, 5])

@halo.distributed([], [np.ndarray])
def test_alias_g():
  return test_alias_f()

@halo.distributed([], [np.ndarray])
def test_alias_h():
  return test_alias_g()

# Test timing

@halo.distributed([], [])
def empty_function():
  return ()

@halo.distributed([], [int])
def trivial_function():
  return 1

# Test keyword arguments

@halo.distributed([int, str], [str])
def keyword_fct1(a, b="hello"):
  return "{} {}".format(a, b)

@halo.distributed([str, str], [str])
def keyword_fct2(a="hello", b="world"):
  return "{} {}".format(a, b)

@halo.distributed([int, int, str, str], [str])
def keyword_fct3(a, b, c="hello", d="world"):
  return "{} {} {} {}".format(a, b, c, d)
