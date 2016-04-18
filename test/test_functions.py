import orchpy

import numpy as np

# Test simple functionality

@orchpy.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  f = open("asdfasdf.txt", "w")
  f.write("successfully called print_string with argument {}.".format(string))
  return string

@orchpy.distributed([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

# Test aliasing

@orchpy.distributed([], [np.ndarray])
def test_alias_f():
  return np.ones([3, 4, 5])

@orchpy.distributed([], [np.ndarray])
def test_alias_g():
  return test_alias_f()

@orchpy.distributed([], [np.ndarray])
def test_alias_h():
  return test_alias_g()

# Test reference counting
