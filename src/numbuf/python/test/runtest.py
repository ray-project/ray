from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray.numbuf as numbuf
import numpy as np
from numpy.testing import assert_equal
import os
import sys

TEST_OBJECTS = [{(1, 2): 1}, {(): 2}, [1, "hello", 3.0], 42, 43,
                "hello world",
                u"x", u"\u262F", 42.0,
                1 << 62, (1.0, "hi"),
                None, (None, None), ("hello", None),
                True, False, (True, False), "hello",
                {True: "hello", False: "world"},
                {"hello": "world", 1: 42, 2.5: 45}, {},
                np.int8(3), np.int32(4), np.int64(5),
                np.uint8(3), np.uint32(4), np.uint64(5),
                np.float32(1.0), np.float64(1.0)]

if sys.version_info < (3, 0):
    TEST_OBJECTS += [long(42), long(1 << 62)]  # noqa: F821


class SerializationTests(unittest.TestCase):

    def roundTripTest(self, data):
        size, serialized = numbuf.serialize_list(data)
        result = numbuf.deserialize_list(serialized)
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
        self.roundTripTest(
            [{"hello": (None, 2, [3, 4], np.array([1.0, 2.0, 3.0]))}])

    def numpyTest(self, t):
        a = np.random.randint(0, 10, size=(100, 100)).astype(t)
        self.roundTripTest([a])

    def testArrays(self):
        for t in ["int8", "uint8", "int16", "uint16", "int32", "uint32",
                  "float32", "float64"]:
            self.numpyTest(t)

    def testRay(self):
        for obj in TEST_OBJECTS:
            self.roundTripTest([obj])

    def testCallback(self):

        class Foo(object):
            def __init__(self):
                self.x = 1

        class Bar(object):
            def __init__(self):
                self.foo = Foo()

        def serialize(obj):
            return dict(obj.__dict__, **{"_pytype_": type(obj).__name__})

        def deserialize(obj):
            if obj["_pytype_"] == "Foo":
                result = Foo()
            elif obj["_pytype_"] == "Bar":
                result = Bar()

            obj.pop("_pytype_", None)
            result.__dict__ = obj
            return result

        bar = Bar()
        bar.foo.x = 42

        numbuf.register_callbacks(serialize, deserialize)

        size, serialized = numbuf.serialize_list([bar])
        self.assertEqual(numbuf.deserialize_list(serialized)[0].foo.x, 42)

    def testObjectArray(self):
        x = np.array([1, 2, "hello"], dtype=object)
        y = np.array([[1, 2], [3, 4]], dtype=object)

        def myserialize(obj):
            return {"_pytype_": "numpy.array", "data": obj.tolist()}

        def mydeserialize(obj):
            if obj["_pytype_"] == "numpy.array":
                return np.array(obj["data"], dtype=object)

        numbuf.register_callbacks(myserialize, mydeserialize)

        size, serialized = numbuf.serialize_list([x, y])

        assert_equal(numbuf.deserialize_list(serialized), [x, y])

    def testBuffer(self):
        for (i, obj) in enumerate(TEST_OBJECTS):
            size, batch = numbuf.serialize_list([obj])
            size = size
            buff = np.zeros(size, dtype="uint8")
            numbuf.write_to_buffer(batch, memoryview(buff))
            array = numbuf.read_from_buffer(memoryview(buff))
            result = numbuf.deserialize_list(array)
            assert_equal(result[0], obj)

    def testObjectArrayImmutable(self):
        obj = np.zeros([10])
        size, serialized = numbuf.serialize_list([obj])
        result = numbuf.deserialize_list(serialized)
        assert_equal(result[0], obj)
        with self.assertRaises(ValueError):
            result[0][0] = 1

    def testArrowLimits(self):
        # Test that objects that are too large for Arrow throw a Python
        # exception. These tests give out of memory errors on Travis and need
        # to be run on a machine with lots of RAM.
        if os.getenv("TRAVIS") is None:
            l = 2 ** 29 * [1.0]
            with self.assertRaises(numbuf.numbuf_error):
                self.roundTripTest(l)
                self.roundTripTest([l])
            del l
            l = 2 ** 29 * ["s"]
            with self.assertRaises(numbuf.numbuf_error):
                self.roundTripTest(l)
                self.roundTripTest([l])
            del l
            l = 2 ** 29 * [["1"], 2, 3, [{"s": 4}]]
            with self.assertRaises(numbuf.numbuf_error):
                self.roundTripTest(l)
                self.roundTripTest([l])
            del l
            with self.assertRaises(numbuf.numbuf_error):
                l = 2 ** 29 * [{"s": 1}] + 2 ** 29 * [1.0]
                self.roundTripTest(l)
                self.roundTripTest([l])
            del l
            with self.assertRaises(numbuf.numbuf_error):
                l = np.zeros(2 ** 25)
                self.roundTripTest([l])
            del l
            with self.assertRaises(numbuf.numbuf_error):
                l = [np.zeros(2 ** 18) for _ in range(2 ** 7)]
                self.roundTripTest(l)
                self.roundTripTest([l])
            del l
        else:
            print("Not running testArrowLimits on Travis because of the "
                  "test's memory requirements.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
