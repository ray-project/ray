from __future__ import print_function

import unittest

import common

BASE_SIMPLE_OBJECTS = [
  0, 1, 100000, 0L, 1L, 100000L, 1L << 100, 0.0, 0.5, 0.9, 100000.1, (), [], {},
  "", 990 * "h", u"", 990 * u"h"
]

LIST_SIMPLE_OBJECTS = [[obj] for obj in BASE_SIMPLE_OBJECTS]
TUPLE_SIMPLE_OBJECTS = [(obj,) for obj in BASE_SIMPLE_OBJECTS]
DICT_SIMPLE_OBJECTS = [{(): obj} for obj in BASE_SIMPLE_OBJECTS]

SIMPLE_OBJECTS = (BASE_SIMPLE_OBJECTS +
                  LIST_SIMPLE_OBJECTS +
                  TUPLE_SIMPLE_OBJECTS +
                  DICT_SIMPLE_OBJECTS)

# Create some complex objects that cannot be serialized by value in tasks.

l = []
l.append(l)

class Foo(object):
  def __init__(self):
    pass

BASE_COMPLEX_OBJECTS = [999 * "h", 999 * u"h", l, Foo(), 10 * [10 * [10 * [1]]]]

LIST_COMPLEX_OBJECTS = [[obj] for obj in BASE_COMPLEX_OBJECTS]
TUPLE_COMPLEX_OBJECTS = [(obj,) for obj in BASE_COMPLEX_OBJECTS]
DICT_COMPLEX_OBJECTS = [{(): obj} for obj in BASE_COMPLEX_OBJECTS]

COMPLEX_OBJECTS = (BASE_COMPLEX_OBJECTS +
                   LIST_COMPLEX_OBJECTS +
                   TUPLE_COMPLEX_OBJECTS +
                   DICT_COMPLEX_OBJECTS)

class TestPlasmaClient(unittest.TestCase):

  def test_serialize_by_value(self):

    for val in SIMPLE_OBJECTS:
      self.assertTrue(common.check_simple_value(val))
    for val in COMPLEX_OBJECTS:
      self.assertFalse(common.check_simple_value(val))

if __name__ == "__main__":
  unittest.main(verbosity=2)
