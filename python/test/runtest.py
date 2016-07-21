import unittest
import libnumbuf
import numpy as np
from numpy.testing import assert_equal

TEST_OBJECTS = [[1, "hello", 3.0], 42, 43L, "hello world", 42.0, 1L << 62,
                (1.0, "hi"), None, (None, None), ("hello", None),
                True, False, (True, False),
                {True: "hello", False: "world"},
                {"hello" : "world", 1: 42, 1.0: 45}, {},
                np.int8(3), np.int32(4), np.int64(5),
                np.uint8(3), np.uint32(4), np.uint64(5),
                np.float32(1.0), np.float64(1.0)]

class SerializationTests(unittest.TestCase):

  def roundTripTest(self, data):
    serialized = libnumbuf.serialize_list(data)
    result = libnumbuf.deserialize_list(serialized)
    assert_equal(data, result)

  def testSimple(self):
    self.roundTripTest([1, 2, 3])
    self.roundTripTest([1.0, 2.0, 3.0])
    self.roundTripTest(['hello', 'world'])
    self.roundTripTest([1, 'hello', 1.0])
    self.roundTripTest([{'hello': 1.0, 'world': 42}])
    self.roundTripTest([True, False])

  def testNone(self):
    self.roundTripTest([1, 2, None, 3])

  def testNested(self):
    self.roundTripTest([{"hello": {"world": (1, 2, 3)}}])
    self.roundTripTest([((1,), (1, 2, 3, (4, 5, 6), "string"))])
    self.roundTripTest([{"hello": [1, 2, 3]}])
    self.roundTripTest([{"hello": [1, [2, 3]]}])
    self.roundTripTest([{"hello": (None, 2, [3, 4])}])
    self.roundTripTest([{"hello": (None, 2, [3, 4], np.ndarray([1.0, 2.0, 3.0]))}])

  def numpyTest(self, t):
    a = np.random.randint(0, 10, size=(100, 100)).astype(t)
    self.roundTripTest([a])

  def testArrays(self):
    for t in ["int8", "uint8", "int16", "uint16", "int32", "uint32", "float32", "float64"]:
      self.numpyTest(t)

  def testRay(self):
    for obj in TEST_OBJECTS:
      self.roundTripTest([obj])

  def testBuffer(self):
    for (i, obj) in enumerate(TEST_OBJECTS):
      x = libnumbuf.serialize_list([1, 2, 3])
      schema = libnumbuf.get_schema_metadata(x)
      size = libnumbuf.get_serialized_size(x) + 4096 # INITIAL_METADATA_SIZE in arrow
      buff = np.zeros(size, dtype="uint8")
      metadata_offset = libnumbuf.write_to_buffer(x, memoryview(buff))
      array = libnumbuf.read_from_buffer(memoryview(buff), schema, metadata_offset)

if __name__ == "__main__":
    unittest.main()
