from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import re
import string
import sys
import threading
import time
import unittest
from collections import defaultdict, namedtuple, OrderedDict

import numpy as np

import ray
import ray.ray_constants as ray_constants
import ray.test.test_utils


def assert_equal(obj1, obj2):
    module_numpy = (type(obj1).__module__ == np.__name__
                    or type(obj2).__module__ == np.__name__)
    if module_numpy:
        empty_shape = ((hasattr(obj1, "shape") and obj1.shape == ())
                       or (hasattr(obj2, "shape") and obj2.shape == ()))
        if empty_shape:
            # This is a special case because currently np.testing.assert_equal
            # fails because we do not properly handle different numerical
            # types.
            assert obj1 == obj2, ("Objects {} and {} are "
                                  "different.".format(obj1, obj2))
        else:
            np.testing.assert_equal(obj1, obj2)
    elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
        special_keys = ["_pytype_"]
        assert (set(list(obj1.__dict__.keys()) + special_keys) == set(
            list(obj2.__dict__.keys()) + special_keys)), ("Objects {} "
                                                          "and {} are "
                                                          "different.".format(
                                                              obj1, obj2))
        for key in obj1.__dict__.keys():
            if key not in special_keys:
                assert_equal(obj1.__dict__[key], obj2.__dict__[key])
    elif type(obj1) is dict or type(obj2) is dict:
        assert_equal(obj1.keys(), obj2.keys())
        for key in obj1.keys():
            assert_equal(obj1[key], obj2[key])
    elif type(obj1) is list or type(obj2) is list:
        assert len(obj1) == len(obj2), ("Objects {} and {} are lists with "
                                        "different lengths.".format(
                                            obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    elif type(obj1) is tuple or type(obj2) is tuple:
        assert len(obj1) == len(obj2), ("Objects {} and {} are tuples with "
                                        "different lengths.".format(
                                            obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    elif (ray.serialization.is_named_tuple(type(obj1))
          or ray.serialization.is_named_tuple(type(obj2))):
        assert len(obj1) == len(obj2), ("Objects {} and {} are named tuples "
                                        "with different lengths.".format(
                                            obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    else:
        assert obj1 == obj2, "Objects {} and {} are different.".format(
            obj1, obj2)


if sys.version_info >= (3, 0):
    long_extras = [0, np.array([["hi", u"hi"], [1.3, 1]])]
else:

    long_extras = [
        long(0),  # noqa: E501,F821
        np.array([
            ["hi", u"hi"],
            [1.3, long(1)]  # noqa: E501,F821
        ])
    ]

PRIMITIVE_OBJECTS = [
    0, 0.0, 0.9, 1 << 62, 1 << 100, 1 << 999, [1 << 100, [1 << 100]], "a",
    string.printable, "\u262F", u"hello world", u"\xff\xfe\x9c\x001\x000\x00",
    None, True, False, [], (), {},
    np.int8(3),
    np.int32(4),
    np.int64(5),
    np.uint8(3),
    np.uint32(4),
    np.uint64(5),
    np.float32(1.9),
    np.float64(1.9),
    np.zeros([100, 100]),
    np.random.normal(size=[100, 100]),
    np.array(["hi", 3]),
    np.array(["hi", 3], dtype=object)
] + long_extras

COMPLEX_OBJECTS = [
    [[[[[[[[[[[[]]]]]]]]]]]],
    {"obj{}".format(i): np.random.normal(size=[100, 100])
     for i in range(10)},
    # {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {
    #      (): {(): {}}}}}}}}}}}}},
    (
        (((((((((), ), ), ), ), ), ), ), ), ),
    {
        "a": {
            "b": {
                "c": {
                    "d": {}
                }
            }
        }
    }
]


class Foo(object):
    def __init__(self, value=0):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return other.value == self.value


class Bar(object):
    def __init__(self):
        for i, val in enumerate(PRIMITIVE_OBJECTS + COMPLEX_OBJECTS):
            setattr(self, "field{}".format(i), val)


class Baz(object):
    def __init__(self):
        self.foo = Foo()
        self.bar = Bar()

    def method(self, arg):
        pass


class Qux(object):
    def __init__(self):
        self.objs = [Foo(), Bar(), Baz()]


class SubQux(Qux):
    def __init__(self):
        Qux.__init__(self)


class CustomError(Exception):
    pass


Point = namedtuple("Point", ["x", "y"])
NamedTupleExample = namedtuple("Example",
                               "field1, field2, field3, field4, field5")

CUSTOM_OBJECTS = [
    Exception("Test object."),
    CustomError(),
    Point(11, y=22),
    Foo(),
    Bar(),
    Baz(),  # Qux(), SubQux(),
    NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3])
]

BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
TUPLE_OBJECTS = [(obj, ) for obj in BASE_OBJECTS]
# The check that type(obj).__module__ != "numpy" should be unnecessary, but
# otherwise this seems to fail on Mac OS X on Travis.
DICT_OBJECTS = (
    [{
        obj: obj
    } for obj in PRIMITIVE_OBJECTS
     if (obj.__hash__ is not None and type(obj).__module__ != "numpy")] + [{
         0: obj
     } for obj in BASE_OBJECTS] + [{
         Foo(123): Foo(456)
     }])

RAY_TEST_OBJECTS = BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=1)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_passing_arguments_by_value(ray_start):
    @ray.remote
    def f(x):
        return x

    # Check that we can pass arguments by value to remote functions and
    # that they are uncorrupted.
    for obj in RAY_TEST_OBJECTS:
        assert_equal(obj, ray.get(f.remote(obj)))


def test_ray_recursive_objects(ray_start):
    class ClassA(object):
        pass

    # Make a list that contains itself.
    lst = []
    lst.append(lst)
    # Make an object that contains itself as a field.
    a1 = ClassA()
    a1.field = a1
    # Make two objects that contain each other as fields.
    a2 = ClassA()
    a3 = ClassA()
    a2.field = a3
    a3.field = a2
    # Make a dictionary that contains itself.
    d1 = {}
    d1["key"] = d1
    # Create a list of recursive objects.
    recursive_objects = [lst, a1, a2, a3, d1]

    # Check that exceptions are thrown when we serialize the recursive
    # objects.
    for obj in recursive_objects:
        with pytest.raises(Exception):
            ray.put(obj)


def test_passing_arguments_by_value_out_of_the_box(ray_start):
    @ray.remote
    def f(x):
        return x

    # Test passing lambdas.

    def temp():
        return 1

    assert ray.get(f.remote(temp))() == 1
    assert ray.get(f.remote(lambda x: x + 1))(3) == 4

    # Test sets.
    assert ray.get(f.remote(set())) == set()
    s = {1, (1, 2, "hi")}
    assert ray.get(f.remote(s)) == s

    # Test types.
    assert ray.get(f.remote(int)) == int
    assert ray.get(f.remote(float)) == float
    assert ray.get(f.remote(str)) == str

    class Foo(object):
        def __init__(self):
            pass

    # Make sure that we can put and get a custom type. Note that the result
    # won't be "equal" to Foo.
    ray.get(ray.put(Foo))


def test_putting_object_that_closes_over_object_id(ray_start):
    # This test is here to prevent a regression of
    # https://github.com/ray-project/ray/issues/1317.

    class Foo(object):
        def __init__(self):
            self.val = ray.put(0)

        def method(self):
            f

    f = Foo()
    with pytest.raises(ray.local_scheduler.common_error):
        ray.put(f)


class WorkerTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testPythonWorkers(self):
        # Test the codepath for starting workers from the Python script,
        # instead of the local scheduler. This codepath is for debugging
        # purposes only.
        num_workers = 4
        ray.worker._init(
            num_workers=num_workers,
            start_workers_from_local_scheduler=False,
            start_ray_local=True)

        @ray.remote
        def f(x):
            return x

        values = ray.get([f.remote(1) for i in range(num_workers * 2)])
        assert values == [1] * (num_workers * 2)

    def testPutGet(self):
        ray.init(num_workers=0)

        for i in range(100):
            value_before = i * 10**6
            objectid = ray.put(value_before)
            value_after = ray.get(objectid)
            assert value_before == value_after

        for i in range(100):
            value_before = i * 10**6 * 1.0
            objectid = ray.put(value_before)
            value_after = ray.get(objectid)
            assert value_before == value_after

        for i in range(100):
            value_before = "h" * i
            objectid = ray.put(value_before)
            value_after = ray.get(objectid)
            assert value_before == value_after

        for i in range(100):
            value_before = [1] * i
            objectid = ray.put(value_before)
            value_after = ray.get(objectid)
            assert value_before == value_after


class APITest(unittest.TestCase):
    def init_ray(self, **kwargs):
        if kwargs is None:
            kwargs = {}
        ray.init(**kwargs)

    def tearDown(self):
        ray.shutdown()

    def testCustomSerializers(self):
        self.init_ray(num_workers=1)

        class Foo(object):
            def __init__(self):
                self.x = 3

        def custom_serializer(obj):
            return 3, "string1", type(obj).__name__

        def custom_deserializer(serialized_obj):
            return serialized_obj, "string2"

        ray.register_custom_serializer(
            Foo,
            serializer=custom_serializer,
            deserializer=custom_deserializer)

        assert ray.get(ray.put(Foo())) == ((3, "string1", Foo.__name__),
                                           "string2")

        class Bar(object):
            def __init__(self):
                self.x = 3

        ray.register_custom_serializer(
            Bar,
            serializer=custom_serializer,
            deserializer=custom_deserializer)

        @ray.remote
        def f():
            return Bar()

        assert ray.get(f.remote()) == ((3, "string1", Bar.__name__), "string2")

    def testRegisterClass(self):
        self.init_ray(num_workers=2)

        # Check that putting an object of a class that has not been registered
        # throws an exception.
        class TempClass(object):
            pass

        ray.get(ray.put(TempClass()))

        # Test subtypes of dictionaries.
        value_before = OrderedDict([("hello", 1), ("world", 2)])
        object_id = ray.put(value_before)
        assert value_before == ray.get(object_id)

        value_before = defaultdict(lambda: 0, [("hello", 1), ("world", 2)])
        object_id = ray.put(value_before)
        assert value_before == ray.get(object_id)

        value_before = defaultdict(lambda: [], [("hello", 1), ("world", 2)])
        object_id = ray.put(value_before)
        assert value_before == ray.get(object_id)

        # Test passing custom classes into remote functions from the driver.
        @ray.remote
        def f(x):
            return x

        foo = ray.get(f.remote(Foo(7)))
        assert foo == Foo(7)

        regex = re.compile(r"\d+\.\d*")
        new_regex = ray.get(f.remote(regex))
        # This seems to fail on the system Python 3 that comes with
        # Ubuntu, so it is commented out for now:
        # self.assertEqual(regex, new_regex)
        # Instead, we do this:
        assert regex.pattern == new_regex.pattern

        # Test returning custom classes created on workers.
        @ray.remote
        def g():
            return SubQux(), Qux()

        subqux, qux = ray.get(g.remote())
        assert subqux.objs[2].foo.value == 0

        # Test exporting custom class definitions from one worker to another
        # when the worker is blocked in a get.
        class NewTempClass(object):
            def __init__(self, value):
                self.value = value

        @ray.remote
        def h1(x):
            return NewTempClass(x)

        @ray.remote
        def h2(x):
            return ray.get(h1.remote(x))

        assert ray.get(h2.remote(10)).value == 10

        # Test registering multiple classes with the same name.
        @ray.remote(num_return_vals=3)
        def j():
            class Class0(object):
                def method0(self):
                    pass

            c0 = Class0()

            class Class0(object):
                def method1(self):
                    pass

            c1 = Class0()

            class Class0(object):
                def method2(self):
                    pass

            c2 = Class0()

            return c0, c1, c2

        results = []
        for _ in range(5):
            results += j.remote()
        for i in range(len(results) // 3):
            c0, c1, c2 = ray.get(results[(3 * i):(3 * (i + 1))])

            c0.method0()
            c1.method1()
            c2.method2()

            assert not hasattr(c0, "method1")
            assert not hasattr(c0, "method2")
            assert not hasattr(c1, "method0")
            assert not hasattr(c1, "method2")
            assert not hasattr(c2, "method0")
            assert not hasattr(c2, "method1")

        @ray.remote
        def k():
            class Class0(object):
                def method0(self):
                    pass

            c0 = Class0()

            class Class0(object):
                def method1(self):
                    pass

            c1 = Class0()

            class Class0(object):
                def method2(self):
                    pass

            c2 = Class0()

            return c0, c1, c2

        results = ray.get([k.remote() for _ in range(5)])
        for c0, c1, c2 in results:
            c0.method0()
            c1.method1()
            c2.method2()

            assert not hasattr(c0, "method1")
            assert not hasattr(c0, "method2")
            assert not hasattr(c1, "method0")
            assert not hasattr(c1, "method2")
            assert not hasattr(c2, "method0")
            assert not hasattr(c2, "method1")

    def testKeywordArgs(self):
        @ray.remote
        def keyword_fct1(a, b="hello"):
            return "{} {}".format(a, b)

        @ray.remote
        def keyword_fct2(a="hello", b="world"):
            return "{} {}".format(a, b)

        @ray.remote
        def keyword_fct3(a, b, c="hello", d="world"):
            return "{} {} {} {}".format(a, b, c, d)

        self.init_ray()

        x = keyword_fct1.remote(1)
        assert ray.get(x) == "1 hello"
        x = keyword_fct1.remote(1, "hi")
        assert ray.get(x) == "1 hi"
        x = keyword_fct1.remote(1, b="world")
        assert ray.get(x) == "1 world"
        x = keyword_fct1.remote(a=1, b="world")
        assert ray.get(x) == "1 world"

        x = keyword_fct2.remote(a="w", b="hi")
        assert ray.get(x) == "w hi"
        x = keyword_fct2.remote(b="hi", a="w")
        assert ray.get(x) == "w hi"
        x = keyword_fct2.remote(a="w")
        assert ray.get(x) == "w world"
        x = keyword_fct2.remote(b="hi")
        assert ray.get(x) == "hello hi"
        x = keyword_fct2.remote("w")
        assert ray.get(x) == "w world"
        x = keyword_fct2.remote("w", "hi")
        assert ray.get(x) == "w hi"

        x = keyword_fct3.remote(0, 1, c="w", d="hi")
        assert ray.get(x) == "0 1 w hi"
        x = keyword_fct3.remote(0, b=1, c="w", d="hi")
        assert ray.get(x) == "0 1 w hi"
        x = keyword_fct3.remote(a=0, b=1, c="w", d="hi")
        assert ray.get(x) == "0 1 w hi"
        x = keyword_fct3.remote(0, 1, d="hi", c="w")
        assert ray.get(x) == "0 1 w hi"
        x = keyword_fct3.remote(0, 1, c="w")
        assert ray.get(x) == "0 1 w world"
        x = keyword_fct3.remote(0, 1, d="hi")
        assert ray.get(x) == "0 1 hello hi"
        x = keyword_fct3.remote(0, 1)
        assert ray.get(x) == "0 1 hello world"
        x = keyword_fct3.remote(a=0, b=1)
        assert ray.get(x) == "0 1 hello world"

        # Check that we cannot pass invalid keyword arguments to functions.
        @ray.remote
        def f1():
            return

        @ray.remote
        def f2(x, y=0, z=0):
            return

        # Make sure we get an exception if too many arguments are passed in.
        with pytest.raises(Exception):
            f1.remote(3)

        with pytest.raises(Exception):
            f1.remote(x=3)

        with pytest.raises(Exception):
            f2.remote(0, w=0)

        with pytest.raises(Exception):
            f2.remote(3, x=3)

        # Make sure we get an exception if too many arguments are passed in.
        with pytest.raises(Exception):
            f2.remote(1, 2, 3, 4)

        @ray.remote
        def f3(x):
            return x

        assert ray.get(f3.remote(4)) == 4

    def testVariableNumberOfArgs(self):
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

        self.init_ray()

        x = varargs_fct1.remote(0, 1, 2)
        assert ray.get(x) == "0 1 2"
        x = varargs_fct2.remote(0, 1, 2)
        assert ray.get(x) == "1 2"

        assert kwargs_exception_thrown

        @ray.remote
        def f1(*args):
            return args

        @ray.remote
        def f2(x, y, *args):
            return x, y, args

        assert ray.get(f1.remote()) == ()
        assert ray.get(f1.remote(1)) == (1, )
        assert ray.get(f1.remote(1, 2, 3)) == (1, 2, 3)
        with pytest.raises(Exception):
            f2.remote()
        with pytest.raises(Exception):
            f2.remote(1)
        assert ray.get(f2.remote(1, 2)) == (1, 2, ())
        assert ray.get(f2.remote(1, 2, 3)) == (1, 2, (3, ))
        assert ray.get(f2.remote(1, 2, 3, 4)) == (1, 2, (3, 4))

    def testNoArgs(self):
        @ray.remote
        def no_op():
            pass

        self.init_ray()

        ray.get(no_op.remote())

    def testDefiningRemoteFunctions(self):
        self.init_ray(num_cpus=3)

        # Test that we can define a remote function in the shell.
        @ray.remote
        def f(x):
            return x + 1

        assert ray.get(f.remote(0)) == 1

        # Test that we can redefine the remote function.
        @ray.remote
        def f(x):
            return x + 10

        while True:
            val = ray.get(f.remote(0))
            assert val in [1, 10]
            if val == 10:
                break
            else:
                print("Still using old definition of f, trying again.")

        # Test that we can close over plain old data.
        data = [
            np.zeros([3, 5]), (1, 2, "a"), [0.0, 1.0, 1 << 62], 1 << 60, {
                "a": np.zeros(3)
            }
        ]

        @ray.remote
        def g():
            return data

        ray.get(g.remote())

        # Test that we can close over modules.
        @ray.remote
        def h():
            return np.zeros([3, 5])

        assert_equal(ray.get(h.remote()), np.zeros([3, 5]))

        @ray.remote
        def j():
            return time.time()

        ray.get(j.remote())

        # Test that we can define remote functions that call other remote
        # functions.
        @ray.remote
        def k(x):
            return x + 1

        @ray.remote
        def k2(x):
            return ray.get(k.remote(x))

        @ray.remote
        def m(x):
            return ray.get(k2.remote(x))

        assert ray.get(k.remote(1)) == 2
        assert ray.get(k2.remote(1)) == 2
        assert ray.get(m.remote(1)) == 2

    def testSubmitAPI(self):
        self.init_ray(num_gpus=1, resources={"Custom": 1}, num_workers=1)

        @ray.remote
        def f(n):
            return list(range(n))

        @ray.remote
        def g():
            return ray.get_gpu_ids()

        assert f._submit([0], num_return_vals=0) is None
        id1 = f._submit(args=[1], num_return_vals=1)
        assert ray.get(id1) == [0]
        id1, id2 = f._submit(args=[2], num_return_vals=2)
        assert ray.get([id1, id2]) == [0, 1]
        id1, id2, id3 = f._submit(args=[3], num_return_vals=3)
        assert ray.get([id1, id2, id3]) == [0, 1, 2]
        assert ray.get(
            g._submit(
                args=[], num_cpus=1, num_gpus=1,
                resources={"Custom": 1})) == [0]
        infeasible_id = g._submit(args=[], resources={"NonexistentCustom": 1})
        ready_ids, remaining_ids = ray.wait([infeasible_id], timeout=50)
        assert len(ready_ids) == 0
        assert len(remaining_ids) == 1

        @ray.remote
        class Actor(object):
            def __init__(self, x, y=0):
                self.x = x
                self.y = y

            def method(self, a, b=0):
                return self.x, self.y, a, b

            def gpu_ids(self):
                return ray.get_gpu_ids()

        a = Actor._submit(
            args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

        id1, id2, id3, id4 = a.method._submit(
            args=["test"], kwargs={"b": 2}, num_return_vals=4)
        assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]

    def testGetMultiple(self):
        self.init_ray()
        object_ids = [ray.put(i) for i in range(10)]
        assert ray.get(object_ids) == list(range(10))

        # Get a random choice of object IDs with duplicates.
        indices = list(np.random.choice(range(10), 5))
        indices += indices
        results = ray.get([object_ids[i] for i in indices])
        assert results == indices

    def testGetMultipleExperimental(self):
        self.init_ray()
        object_ids = [ray.put(i) for i in range(10)]

        object_ids_tuple = tuple(object_ids)
        assert ray.experimental.get(object_ids_tuple) == list(range(10))

        object_ids_nparray = np.array(object_ids)
        assert ray.experimental.get(object_ids_nparray) == list(range(10))

    def testGetDict(self):
        self.init_ray()
        d = {str(i): ray.put(i) for i in range(5)}
        for i in range(5, 10):
            d[str(i)] = i
        result = ray.experimental.get(d)
        expected = {str(i): i for i in range(10)}
        assert result == expected

    def testWait(self):
        self.init_ray(num_cpus=1)

        @ray.remote
        def f(delay):
            time.sleep(delay)
            return 1

        objectids = [
            f.remote(1.0),
            f.remote(0.5),
            f.remote(0.5),
            f.remote(0.5)
        ]
        ready_ids, remaining_ids = ray.wait(objectids)
        assert len(ready_ids) == 1
        assert len(remaining_ids) == 3
        ready_ids, remaining_ids = ray.wait(objectids, num_returns=4)
        assert set(ready_ids) == set(objectids)
        assert remaining_ids == []

        objectids = [
            f.remote(0.5),
            f.remote(0.5),
            f.remote(0.5),
            f.remote(0.5)
        ]
        start_time = time.time()
        ready_ids, remaining_ids = ray.wait(
            objectids, timeout=1750, num_returns=4)
        assert time.time() - start_time < 2
        assert len(ready_ids) == 3
        assert len(remaining_ids) == 1
        ray.wait(objectids)
        objectids = [
            f.remote(1.0),
            f.remote(0.5),
            f.remote(0.5),
            f.remote(0.5)
        ]
        start_time = time.time()
        ready_ids, remaining_ids = ray.wait(objectids, timeout=5000)
        assert time.time() - start_time < 5
        assert len(ready_ids) == 1
        assert len(remaining_ids) == 3

        # Verify that calling wait with duplicate object IDs throws an
        # exception.
        x = ray.put(1)
        with pytest.raises(Exception):
            ray.wait([x, x])

        # Make sure it is possible to call wait with an empty list.
        ready_ids, remaining_ids = ray.wait([])
        assert ready_ids == []
        assert remaining_ids == []

        # Test semantics of num_returns with no timeout.
        oids = [ray.put(i) for i in range(10)]
        (found, rest) = ray.wait(oids, num_returns=2)
        assert len(found) == 2
        assert len(rest) == 8

        # Verify that incorrect usage raises a TypeError.
        x = ray.put(1)
        with pytest.raises(TypeError):
            ray.wait(x)
        with pytest.raises(TypeError):
            ray.wait(1)
        with pytest.raises(TypeError):
            ray.wait([1])

    def testWaitIterables(self):
        self.init_ray(num_cpus=1)

        @ray.remote
        def f(delay):
            time.sleep(delay)
            return 1

        objectids = (f.remote(1.0), f.remote(0.5), f.remote(0.5),
                     f.remote(0.5))
        ready_ids, remaining_ids = ray.experimental.wait(objectids)
        assert len(ready_ids) == 1
        assert len(remaining_ids) == 3

        objectids = np.array(
            [f.remote(1.0),
             f.remote(0.5),
             f.remote(0.5),
             f.remote(0.5)])
        ready_ids, remaining_ids = ray.experimental.wait(objectids)
        assert len(ready_ids) == 1
        assert len(remaining_ids) == 3

    def testMultipleWaitsAndGets(self):
        # It is important to use three workers here, so that the three tasks
        # launched in this experiment can run at the same time.
        self.init_ray(num_cpus=3)

        @ray.remote
        def f(delay):
            time.sleep(delay)
            return 1

        @ray.remote
        def g(l):
            # The argument l should be a list containing one object ID.
            ray.wait([l[0]])

        @ray.remote
        def h(l):
            # The argument l should be a list containing one object ID.
            ray.get(l[0])

        # Make sure that multiple wait requests involving the same object ID
        # all return.
        x = f.remote(1)
        ray.get([g.remote([x]), g.remote([x])])

        # Make sure that multiple get requests involving the same object ID all
        # return.
        x = f.remote(1)
        ray.get([h.remote([x]), h.remote([x])])

    def testCachingFunctionsToRun(self):
        # Test that we export functions to run on all workers before the driver
        # is connected.
        def f(worker_info):
            sys.path.append(1)

        ray.worker.global_worker.run_function_on_all_workers(f)

        def f(worker_info):
            sys.path.append(2)

        ray.worker.global_worker.run_function_on_all_workers(f)

        def g(worker_info):
            sys.path.append(3)

        ray.worker.global_worker.run_function_on_all_workers(g)

        def f(worker_info):
            sys.path.append(4)

        ray.worker.global_worker.run_function_on_all_workers(f)

        self.init_ray()

        @ray.remote
        def get_state():
            time.sleep(1)
            return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

        res1 = get_state.remote()
        res2 = get_state.remote()
        assert ray.get(res1) == (1, 2, 3, 4)
        assert ray.get(res2) == (1, 2, 3, 4)

        # Clean up the path on the workers.
        def f(worker_info):
            sys.path.pop()
            sys.path.pop()
            sys.path.pop()
            sys.path.pop()

        ray.worker.global_worker.run_function_on_all_workers(f)

    def testRunningFunctionOnAllWorkers(self):
        self.init_ray()

        def f(worker_info):
            sys.path.append("fake_directory")

        ray.worker.global_worker.run_function_on_all_workers(f)

        @ray.remote
        def get_path1():
            return sys.path

        assert "fake_directory" == ray.get(get_path1.remote())[-1]

        def f(worker_info):
            sys.path.pop(-1)

        ray.worker.global_worker.run_function_on_all_workers(f)

        # Create a second remote function to guarantee that when we call
        # get_path2.remote(), the second function to run will have been run on
        # the worker.
        @ray.remote
        def get_path2():
            return sys.path

        assert "fake_directory" not in ray.get(get_path2.remote())

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray (nor is it intended to).")
    def testLoggingAPI(self):
        self.init_ray()

        def events():
            # This is a hack for getting the event log. It is not part of the
            # API.
            keys = ray.worker.global_worker.redis_client.keys("event_log:*")
            res = []
            for key in keys:
                res.extend(
                    ray.worker.global_worker.redis_client.zrange(key, 0, -1))
            return res

        def wait_for_num_events(num_events, timeout=10):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if len(events()) >= num_events:
                    return
                time.sleep(0.1)
            print("Timing out of wait.")

        @ray.remote
        def test_log_span():
            with ray.profile("event_type2", extra_data={"key": "val"}):
                pass

        # Make sure that we can call ray.log_span in a remote function.
        ray.get(test_log_span.remote())

        # Wait for the events to appear in the event log.
        wait_for_num_events(1)
        assert len(events()) == 1

        @ray.remote
        def test_log_span_exception():
            with ray.log_span("event_type2", extra_data={"key": "val"}):
                raise Exception("This failed.")

        # Make sure that logging a span works if an exception is thrown.
        test_log_span_exception.remote()
        # Wait for the events to appear in the event log.
        wait_for_num_events(2)
        assert len(events()) == 2

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") != "1",
        "This test only works with xray.")
    def testProfilingAPI(self):
        self.init_ray(num_cpus=2)

        @ray.remote
        def f():
            with ray.profile(
                    "custom_event",
                    extra_data={"name": "custom name"}) as ray_prof:
                ray_prof.set_attribute("key", "value")

        ray.put(1)
        object_id = f.remote()
        ray.wait([object_id])
        ray.get(object_id)

        # Wait until all of the profiling information appears in the profile
        # table.
        timeout_seconds = 20
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout_seconds:
                raise Exception("Timed out while waiting for information in "
                                "profile table.")
            profile_data = ray.global_state.chrome_tracing_dump()
            event_types = {event["cat"] for event in profile_data}
            expected_types = [
                "get_task",
                "task",
                "task:deserialize_arguments",
                "task:execute",
                "task:store_outputs",
                "wait_for_function",
                "ray.get",
                "ray.put",
                "ray.wait",
                "submit_task",
                "fetch_and_run_function",
                "register_remote_function",
                "custom_event",  # This is the custom one from ray.profile.
            ]

            if all(expected_type in event_types
                   for expected_type in expected_types):
                break

    def testIdenticalFunctionNames(self):
        # Define a bunch of remote functions and make sure that we don't
        # accidentally call an older version.
        self.init_ray()

        num_calls = 200

        @ray.remote
        def f():
            return 1

        results1 = [f.remote() for _ in range(num_calls)]

        @ray.remote
        def f():
            return 2

        results2 = [f.remote() for _ in range(num_calls)]

        @ray.remote
        def f():
            return 3

        results3 = [f.remote() for _ in range(num_calls)]

        @ray.remote
        def f():
            return 4

        results4 = [f.remote() for _ in range(num_calls)]

        @ray.remote
        def f():
            return 5

        results5 = [f.remote() for _ in range(num_calls)]

        assert ray.get(results1) == num_calls * [1]
        assert ray.get(results2) == num_calls * [2]
        assert ray.get(results3) == num_calls * [3]
        assert ray.get(results4) == num_calls * [4]
        assert ray.get(results5) == num_calls * [5]

        @ray.remote
        def g():
            return 1

        @ray.remote  # noqa: F811
        def g():
            return 2

        @ray.remote  # noqa: F811
        def g():
            return 3

        @ray.remote  # noqa: F811
        def g():
            return 4

        @ray.remote  # noqa: F811
        def g():
            return 5

        result_values = ray.get([g.remote() for _ in range(num_calls)])
        assert result_values == num_calls * [5]

    def testIllegalAPICalls(self):
        self.init_ray()

        # Verify that we cannot call put on an ObjectID.
        x = ray.put(1)
        with pytest.raises(Exception):
            ray.put(x)
        # Verify that we cannot call get on a regular value.
        with pytest.raises(Exception):
            ray.get(3)

    def testMultithreading(self):
        self.init_ray()

        @ray.remote
        def f():
            pass

        def g(n):
            for _ in range(1000 // n):
                ray.get([f.remote() for _ in range(n)])
            res = [ray.put(i) for i in range(1000 // n)]
            ray.wait(res, len(res))

        def test_multi_threading():
            threads = [
                threading.Thread(target=g, args=(n, ))
                for n in [1, 5, 10, 100, 1000]
            ]

            [thread.start() for thread in threads]
            [thread.join() for thread in threads]

        @ray.remote
        def test_multi_threading_in_worker():
            test_multi_threading()

        # test multi-threading in the driver
        test_multi_threading()
        # test multi-threading in the worker
        ray.get(test_multi_threading_in_worker.remote())

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") != "1",
        "This test only works with xray.")
    def testFreeObjectsMultiNode(self):
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=3,
            num_workers=1,
            num_cpus=[1, 1, 1],
            resources=[{
                "Custom0": 1
            }, {
                "Custom1": 1
            }, {
                "Custom2": 1
            }],
            use_raylet=True)

        @ray.remote(resources={"Custom0": 1})
        def run_on_0():
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"Custom1": 1})
        def run_on_1():
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"Custom2": 1})
        def run_on_2():
            return ray.worker.global_worker.plasma_client.store_socket_name

        def create():
            a = run_on_0.remote()
            b = run_on_1.remote()
            c = run_on_2.remote()
            (l1, l2) = ray.wait([a, b, c], num_returns=3)
            assert len(l1) == 3
            assert len(l2) == 0
            return (a, b, c)

        def flush():
            # Flush the Release History.
            # Current Plasma Client Cache will maintain 64-item list.
            # If the number changed, this will fail.
            print("Start Flush!")
            for i in range(64):
                ray.get(
                    [run_on_0.remote(),
                     run_on_1.remote(),
                     run_on_2.remote()])
            print("Flush finished!")

        def run_one_test(local_only):
            (a, b, c) = create()
            # The three objects should be generated on different object stores.
            assert ray.get(a) != ray.get(b)
            assert ray.get(a) != ray.get(c)
            assert ray.get(c) != ray.get(b)
            ray.internal.free([a, b, c], local_only=local_only)
            flush()
            return (a, b, c)

        # Case 1: run this local_only=False. All 3 objects will be deleted.
        (a, b, c) = run_one_test(False)
        (l1, l2) = ray.wait([a, b, c], timeout=10, num_returns=1)
        # All the objects are deleted.
        assert len(l1) == 0
        assert len(l2) == 3
        # Case 2: run this local_only=True. Only 1 object will be deleted.
        (a, b, c) = run_one_test(True)
        (l1, l2) = ray.wait([a, b, c], timeout=10, num_returns=3)
        # One object is deleted and 2 objects are not.
        assert len(l1) == 2
        assert len(l2) == 1
        # The deleted object will have the same store with the driver.
        local_return = ray.worker.global_worker.plasma_client.store_socket_name
        for object_id in l1:
            assert ray.get(object_id) != local_return


@unittest.skipIf(
    os.environ.get("RAY_USE_NEW_GCS", False),
    "For now, RAY_USE_NEW_GCS supports 1 shard, and credis "
    "supports 1-node chain for that shard only.")
class APITestSharded(APITest):
    def init_ray(self, **kwargs):
        if kwargs is None:
            kwargs = {}
        kwargs["start_ray_local"] = True
        kwargs["num_redis_shards"] = 20
        kwargs["redirect_output"] = True
        ray.worker._init(**kwargs)


class LocalModeTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testLocalMode(self):
        @ray.remote
        def local_mode_f():
            return np.array([0, 0])

        @ray.remote
        def local_mode_g(x):
            x[0] = 1
            return x

        ray.init(local_mode=True)

        @ray.remote
        def f():
            return np.ones([3, 4, 5])

        xref = f.remote()
        # Remote functions should return by value.
        assert_equal(xref, np.ones([3, 4, 5]))
        # Check that ray.get is the identity.
        assert_equal(xref, ray.get(xref))
        y = np.random.normal(size=[11, 12])
        # Check that ray.put is the identity.
        assert_equal(y, ray.put(y))

        # Make sure objects are immutable, this example is why we need to copy
        # arguments before passing them into remote functions in python mode
        aref = local_mode_f.remote()
        assert_equal(aref, np.array([0, 0]))
        bref = local_mode_g.remote(aref)
        # Make sure local_mode_g does not mutate aref.
        assert_equal(aref, np.array([0, 0]))
        assert_equal(bref, np.array([1, 0]))

        # wait should return the first num_returns values passed in as the
        # first list and the remaining values as the second list
        num_returns = 5
        object_ids = [ray.put(i) for i in range(20)]
        ready, remaining = ray.wait(
            object_ids, num_returns=num_returns, timeout=None)
        assert_equal(ready, object_ids[:num_returns])
        assert_equal(remaining, object_ids[num_returns:])

        # Test actors in LOCAL_MODE.

        @ray.remote
        class LocalModeTestClass(object):
            def __init__(self, array):
                self.array = array

            def set_array(self, array):
                self.array = array

            def get_array(self):
                return self.array

            def modify_and_set_array(self, array):
                array[0] = -1
                self.array = array

        test_actor = LocalModeTestClass.remote(np.arange(10))
        # Remote actor functions should return by value
        assert_equal(test_actor.get_array.remote(), np.arange(10))

        test_array = np.arange(10)
        # Remote actor functions should not mutate arguments
        test_actor.modify_and_set_array.remote(test_array)
        assert_equal(test_array, np.arange(10))
        # Remote actor functions should keep state
        test_array[0] = -1
        assert_equal(test_array, test_actor.get_array.remote())

        # Check that actor handles work in Python mode.

        @ray.remote
        def use_actor_handle(handle):
            array = np.ones(10)
            handle.set_array.remote(array)
            assert np.alltrue(array == ray.get(handle.get_array.remote()))

        ray.get(use_actor_handle.remote(test_actor))


class ResourcesTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testResourceConstraints(self):
        num_workers = 20
        ray.init(num_workers=num_workers, num_cpus=10, num_gpus=2)

        @ray.remote(num_cpus=0)
        def get_worker_id():
            time.sleep(0.1)
            return os.getpid()

        # Attempt to wait for all of the workers to start up.
        while True:
            if len(
                    set(
                        ray.get([
                            get_worker_id.remote() for _ in range(num_workers)
                        ]))) == num_workers:
                break

        time_buffer = 0.3

        # At most 10 copies of this can run at once.
        @ray.remote(num_cpus=1)
        def f(n):
            time.sleep(n)

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(10)])
        duration = time.time() - start_time
        assert duration < 0.5 + time_buffer
        assert duration > 0.5

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(11)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

        @ray.remote(num_cpus=3)
        def f(n):
            time.sleep(n)

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(3)])
        duration = time.time() - start_time
        assert duration < 0.5 + time_buffer
        assert duration > 0.5

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(4)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

        @ray.remote(num_gpus=1)
        def f(n):
            time.sleep(n)

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(2)])
        duration = time.time() - start_time
        assert duration < 0.5 + time_buffer
        assert duration > 0.5

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(3)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

        start_time = time.time()
        ray.get([f.remote(0.5) for _ in range(4)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

    def testMultiResourceConstraints(self):
        num_workers = 20
        ray.init(num_workers=num_workers, num_cpus=10, num_gpus=10)

        @ray.remote(num_cpus=0)
        def get_worker_id():
            time.sleep(0.1)
            return os.getpid()

        # Attempt to wait for all of the workers to start up.
        while True:
            if len(
                    set(
                        ray.get([
                            get_worker_id.remote() for _ in range(num_workers)
                        ]))) == num_workers:
                break

        @ray.remote(num_cpus=1, num_gpus=9)
        def f(n):
            time.sleep(n)

        @ray.remote(num_cpus=9, num_gpus=1)
        def g(n):
            time.sleep(n)

        time_buffer = 0.3

        start_time = time.time()
        ray.get([f.remote(0.5), g.remote(0.5)])
        duration = time.time() - start_time
        assert duration < 0.5 + time_buffer
        assert duration > 0.5

        start_time = time.time()
        ray.get([f.remote(0.5), f.remote(0.5)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

        start_time = time.time()
        ray.get([g.remote(0.5), g.remote(0.5)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

        start_time = time.time()
        ray.get([f.remote(0.5), f.remote(0.5), g.remote(0.5), g.remote(0.5)])
        duration = time.time() - start_time
        assert duration < 1 + time_buffer
        assert duration > 1

    def testGPUIDs(self):
        num_gpus = 10
        ray.init(num_cpus=10, num_gpus=num_gpus)

        @ray.remote(num_gpus=0)
        def f0():
            time.sleep(0.1)
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 0
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            for gpu_id in gpu_ids:
                assert gpu_id in range(num_gpus)
            return gpu_ids

        @ray.remote(num_gpus=1)
        def f1():
            time.sleep(0.1)
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            for gpu_id in gpu_ids:
                assert gpu_id in range(num_gpus)
            return gpu_ids

        @ray.remote(num_gpus=2)
        def f2():
            time.sleep(0.1)
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 2
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            for gpu_id in gpu_ids:
                assert gpu_id in range(num_gpus)
            return gpu_ids

        @ray.remote(num_gpus=3)
        def f3():
            time.sleep(0.1)
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 3
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            for gpu_id in gpu_ids:
                assert gpu_id in range(num_gpus)
            return gpu_ids

        @ray.remote(num_gpus=4)
        def f4():
            time.sleep(0.1)
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 4
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            for gpu_id in gpu_ids:
                assert gpu_id in range(num_gpus)
            return gpu_ids

        @ray.remote(num_gpus=5)
        def f5():
            time.sleep(0.1)
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 5
            assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                [str(i) for i in gpu_ids]))
            for gpu_id in gpu_ids:
                assert gpu_id in range(num_gpus)
            return gpu_ids

        # Wait for all workers to start up.
        @ray.remote
        def f():
            time.sleep(0.1)
            return os.getpid()

        start_time = time.time()
        while True:
            if len(set(ray.get([f.remote() for _ in range(10)]))) == 10:
                break
            if time.time() > start_time + 10:
                raise Exception("Timed out while waiting for workers to start "
                                "up.")

        list_of_ids = ray.get([f0.remote() for _ in range(10)])
        assert list_of_ids == 10 * [[]]

        list_of_ids = ray.get([f1.remote() for _ in range(10)])
        set_of_ids = {tuple(gpu_ids) for gpu_ids in list_of_ids}
        assert set_of_ids == {(i, ) for i in range(10)}

        list_of_ids = ray.get([f2.remote(), f4.remote(), f4.remote()])
        all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
        assert set(all_ids) == set(range(10))

        remaining = [f5.remote() for _ in range(20)]
        for _ in range(10):
            t1 = time.time()
            ready, remaining = ray.wait(remaining, num_returns=2)
            t2 = time.time()
            # There are only 10 GPUs, and each task uses 2 GPUs, so there
            # should only be 2 tasks scheduled at a given time, so if we wait
            # for 2 tasks to finish, then it should take at least 0.1 seconds
            # for each pair of tasks to finish.
            assert t2 - t1 > 0.09
            list_of_ids = ray.get(ready)
            all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
            # Commenting out the below assert because it seems to fail a lot.
            # self.assertEqual(set(all_ids), set(range(10)))

        # Test that actors have CUDA_VISIBLE_DEVICES set properly.

        @ray.remote
        class Actor0(object):
            def __init__(self):
                gpu_ids = ray.get_gpu_ids()
                assert len(gpu_ids) == 0
                assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                    [str(i) for i in gpu_ids]))
                # Set self.x to make sure that we got here.
                self.x = 1

            def test(self):
                gpu_ids = ray.get_gpu_ids()
                assert len(gpu_ids) == 0
                assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                    [str(i) for i in gpu_ids]))
                return self.x

        @ray.remote(num_gpus=1)
        class Actor1(object):
            def __init__(self):
                gpu_ids = ray.get_gpu_ids()
                assert len(gpu_ids) == 1
                assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                    [str(i) for i in gpu_ids]))
                # Set self.x to make sure that we got here.
                self.x = 1

            def test(self):
                gpu_ids = ray.get_gpu_ids()
                assert len(gpu_ids) == 1
                assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
                    [str(i) for i in gpu_ids]))
                return self.x

        a0 = Actor0.remote()
        ray.get(a0.test.remote())

        a1 = Actor1.remote()
        ray.get(a1.test.remote())

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") != "1",
        "This test only works with xray.")
    def testZeroCPUs(self):
        ray.init(num_cpus=0)

        @ray.remote(num_cpus=0)
        def f():
            return 1

        # The task should be able to execute.
        ray.get(f.remote())

    def testZeroCPUsActor(self):
        ray.worker._init(
            start_ray_local=True, num_local_schedulers=2, num_cpus=[0, 2])

        local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote
        class Foo(object):
            def method(self):
                return ray.worker.global_worker.plasma_client.store_socket_name

        # Make sure tasks and actors run on the remote local scheduler.
        a = Foo.remote()
        assert ray.get(a.method.remote()) != local_plasma

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") != "1",
        "This test only works with xray.")
    def testFractionalResources(self):
        ray.init(num_cpus=6, num_gpus=3, resources={"Custom": 1})

        @ray.remote(num_gpus=0.5)
        class Foo1(object):
            def method(self):
                gpu_ids = ray.get_gpu_ids()
                assert len(gpu_ids) == 1
                return gpu_ids[0]

        foos = [Foo1.remote() for _ in range(6)]
        gpu_ids = ray.get([f.method.remote() for f in foos])
        for i in range(3):
            assert gpu_ids.count(i) == 2
        del foos

        @ray.remote
        class Foo2(object):
            def method(self):
                pass

        # Create an actor that requires 0.7 of the custom resource.
        f1 = Foo2._submit([], {}, resources={"Custom": 0.7})
        ray.get(f1.method.remote())
        # Make sure that we cannot create an actor that requires 0.7 of the
        # custom resource. TODO(rkn): Re-enable this once ray.wait is
        # implemented.
        f2 = Foo2._submit([], {}, resources={"Custom": 0.7})
        ready, _ = ray.wait([f2.method.remote()], timeout=500)
        assert len(ready) == 0
        # Make sure we can start an actor that requries only 0.3 of the custom
        # resource.
        f3 = Foo2._submit([], {}, resources={"Custom": 0.3})
        ray.get(f3.method.remote())

        del f1, f3

        # Make sure that we get exceptions if we submit tasks that require a
        # fractional number of resources greater than 1.

        @ray.remote(num_cpus=1.5)
        def test():
            pass

        with pytest.raises(ValueError):
            test.remote()

        with pytest.raises(ValueError):
            Foo2._submit([], {}, resources={"Custom": 1.5})

    def testMultipleLocalSchedulers(self):
        # This test will define a bunch of tasks that can only be assigned to
        # specific local schedulers, and we will check that they are assigned
        # to the correct local schedulers.
        address_info = ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=3,
            num_workers=1,
            num_cpus=[100, 5, 10],
            num_gpus=[0, 5, 1])

        # Define a bunch of remote functions that all return the socket name of
        # the plasma store. Since there is a one-to-one correspondence between
        # plasma stores and local schedulers (at least right now), this can be
        # used to identify which local scheduler the task was assigned to.

        # This must be run on the zeroth local scheduler.
        @ray.remote(num_cpus=11)
        def run_on_0():
            return ray.worker.global_worker.plasma_client.store_socket_name

        # This must be run on the first local scheduler.
        @ray.remote(num_gpus=2)
        def run_on_1():
            return ray.worker.global_worker.plasma_client.store_socket_name

        # This must be run on the second local scheduler.
        @ray.remote(num_cpus=6, num_gpus=1)
        def run_on_2():
            return ray.worker.global_worker.plasma_client.store_socket_name

        # This can be run anywhere.
        @ray.remote(num_cpus=0, num_gpus=0)
        def run_on_0_1_2():
            return ray.worker.global_worker.plasma_client.store_socket_name

        # This must be run on the first or second local scheduler.
        @ray.remote(num_gpus=1)
        def run_on_1_2():
            return ray.worker.global_worker.plasma_client.store_socket_name

        # This must be run on the zeroth or second local scheduler.
        @ray.remote(num_cpus=8)
        def run_on_0_2():
            return ray.worker.global_worker.plasma_client.store_socket_name

        def run_lots_of_tasks():
            names = []
            results = []
            for i in range(100):
                index = np.random.randint(6)
                if index == 0:
                    names.append("run_on_0")
                    results.append(run_on_0.remote())
                elif index == 1:
                    names.append("run_on_1")
                    results.append(run_on_1.remote())
                elif index == 2:
                    names.append("run_on_2")
                    results.append(run_on_2.remote())
                elif index == 3:
                    names.append("run_on_0_1_2")
                    results.append(run_on_0_1_2.remote())
                elif index == 4:
                    names.append("run_on_1_2")
                    results.append(run_on_1_2.remote())
                elif index == 5:
                    names.append("run_on_0_2")
                    results.append(run_on_0_2.remote())
            return names, results

        store_names = [
            object_store_address.name
            for object_store_address in address_info["object_store_addresses"]
        ]

        def validate_names_and_results(names, results):
            for name, result in zip(names, ray.get(results)):
                if name == "run_on_0":
                    assert result in [store_names[0]]
                elif name == "run_on_1":
                    assert result in [store_names[1]]
                elif name == "run_on_2":
                    assert result in [store_names[2]]
                elif name == "run_on_0_1_2":
                    assert (result in [
                        store_names[0], store_names[1], store_names[2]
                    ])
                elif name == "run_on_1_2":
                    assert result in [store_names[1], store_names[2]]
                elif name == "run_on_0_2":
                    assert result in [store_names[0], store_names[2]]
                else:
                    raise Exception("This should be unreachable.")
                assert set(ray.get(results)) == set(store_names)

        names, results = run_lots_of_tasks()
        validate_names_and_results(names, results)

        # Make sure the same thing works when this is nested inside of a task.

        @ray.remote
        def run_nested1():
            names, results = run_lots_of_tasks()
            return names, results

        @ray.remote
        def run_nested2():
            names, results = ray.get(run_nested1.remote())
            return names, results

        names, results = ray.get(run_nested2.remote())
        validate_names_and_results(names, results)

    def testCustomResources(self):
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=2,
            num_cpus=[3, 3],
            resources=[{
                "CustomResource": 0
            }, {
                "CustomResource": 1
            }])

        @ray.remote
        def f():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"CustomResource": 1})
        def g():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"CustomResource": 1})
        def h():
            ray.get([f.remote() for _ in range(5)])
            return ray.worker.global_worker.plasma_client.store_socket_name

        # The f tasks should be scheduled on both local schedulers.
        assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2

        local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

        # The g tasks should be scheduled only on the second local scheduler.
        local_scheduler_ids = set(ray.get([g.remote() for _ in range(50)]))
        assert len(local_scheduler_ids) == 1
        assert list(local_scheduler_ids)[0] != local_plasma

        # Make sure that resource bookkeeping works when a task that uses a
        # custom resources gets blocked.
        ray.get([h.remote() for _ in range(5)])

    def testTwoCustomResources(self):
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=2,
            num_cpus=[3, 3],
            resources=[{
                "CustomResource1": 1,
                "CustomResource2": 2
            }, {
                "CustomResource1": 3,
                "CustomResource2": 4
            }])

        @ray.remote(resources={"CustomResource1": 1})
        def f():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"CustomResource2": 1})
        def g():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"CustomResource1": 1, "CustomResource2": 3})
        def h():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"CustomResource1": 4})
        def j():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        @ray.remote(resources={"CustomResource3": 1})
        def k():
            time.sleep(0.001)
            return ray.worker.global_worker.plasma_client.store_socket_name

        # The f and g tasks should be scheduled on both local schedulers.
        assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2
        assert len(set(ray.get([g.remote() for _ in range(50)]))) == 2

        local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

        # The h tasks should be scheduled only on the second local scheduler.
        local_scheduler_ids = set(ray.get([h.remote() for _ in range(50)]))
        assert len(local_scheduler_ids) == 1
        assert list(local_scheduler_ids)[0] != local_plasma

        # Make sure that tasks with unsatisfied custom resource requirements do
        # not get scheduled.
        ready_ids, remaining_ids = ray.wait(
            [j.remote(), k.remote()], timeout=500)
        assert ready_ids == []

    def testManyCustomResources(self):
        num_custom_resources = 10000
        total_resources = {
            str(i): np.random.randint(1, 7)
            for i in range(num_custom_resources)
        }
        ray.init(num_cpus=5, resources=total_resources)

        def f():
            return 1

        remote_functions = []
        for _ in range(20):
            num_resources = np.random.randint(0, num_custom_resources + 1)
            permuted_resources = np.random.permutation(
                num_custom_resources)[:num_resources]
            random_resources = {
                str(i): total_resources[str(i)]
                for i in permuted_resources
            }
            remote_function = ray.remote(resources=random_resources)(f)
            remote_functions.append(remote_function)

        remote_functions.append(ray.remote(f))
        remote_functions.append(ray.remote(resources=total_resources)(f))

        results = []
        for remote_function in remote_functions:
            results.append(remote_function.remote())
            results.append(remote_function.remote())
            results.append(remote_function.remote())

        ray.get(results)


class CudaVisibleDevicesTest(unittest.TestCase):
    def setUp(self):
        # Record the curent value of this environment variable so that we can
        # reset it after the test.
        self.original_gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    def tearDown(self):
        ray.shutdown()
        # Reset the environment variable.
        if self.original_gpu_ids is not None:
            os.environ["CUDA_VISIBLE_DEVICES"] = self.original_gpu_ids
        else:
            del os.environ["CUDA_VISIBLE_DEVICES"]

    def testSpecificGPUs(self):
        allowed_gpu_ids = [4, 5, 6]
        os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
            [str(i) for i in allowed_gpu_ids])
        ray.init(num_gpus=3)

        @ray.remote(num_gpus=1)
        def f():
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert gpu_ids[0] in allowed_gpu_ids

        @ray.remote(num_gpus=2)
        def g():
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 2
            assert gpu_ids[0] in allowed_gpu_ids
            assert gpu_ids[1] in allowed_gpu_ids

        ray.get([f.remote() for _ in range(100)])
        ray.get([g.remote() for _ in range(100)])


class WorkerPoolTests(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testNoWorkers(self):
        ray.init(num_workers=0)

        @ray.remote
        def f():
            return 1

        # Make sure we can call a remote function. This will require starting a
        # new worker.
        ray.get(f.remote())

        ray.get([f.remote() for _ in range(100)])

    def testBlockingTasks(self):
        ray.init(num_cpus=1)

        @ray.remote
        def f(i, j):
            return (i, j)

        @ray.remote
        def g(i):
            # Each instance of g submits and blocks on the result of another
            # remote task.
            object_ids = [f.remote(i, j) for j in range(2)]
            return ray.get(object_ids)

        ray.get([g.remote(i) for i in range(4)])

        @ray.remote
        def _sleep(i):
            time.sleep(0.01)
            return (i)

        @ray.remote
        def sleep():
            # Each instance of sleep submits and blocks on the result of
            # another remote task, which takes some time to execute.
            ray.get([_sleep.remote(i) for i in range(10)])

        ray.get(sleep.remote())

    def testMaxCallTasks(self):
        ray.init(num_cpus=1)

        @ray.remote(max_calls=1)
        def f():
            return os.getpid()

        pid = ray.get(f.remote())
        ray.test.test_utils.wait_for_pid_to_exit(pid)

        @ray.remote(max_calls=2)
        def f():
            return os.getpid()

        pid1 = ray.get(f.remote())
        pid2 = ray.get(f.remote())
        assert pid1 == pid2
        ray.test.test_utils.wait_for_pid_to_exit(pid1)


class SchedulingAlgorithm(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def attempt_to_load_balance(self,
                                remote_function,
                                args,
                                total_tasks,
                                num_local_schedulers,
                                minimum_count,
                                num_attempts=100):
        attempts = 0
        while attempts < num_attempts:
            locations = ray.get(
                [remote_function.remote(*args) for _ in range(total_tasks)])
            names = set(locations)
            counts = [locations.count(name) for name in names]
            print("Counts are {}.".format(counts))
            if (len(names) == num_local_schedulers
                    and all(count >= minimum_count for count in counts)):
                break
            attempts += 1
        assert attempts < num_attempts

    def testLoadBalancing(self):
        # This test ensures that tasks are being assigned to all local
        # schedulers in a roughly equal manner.
        num_local_schedulers = 3
        num_cpus = 7
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=num_local_schedulers,
            num_cpus=num_cpus)

        @ray.remote
        def f():
            time.sleep(0.01)
            return ray.worker.global_worker.plasma_client.store_socket_name

        self.attempt_to_load_balance(f, [], 100, num_local_schedulers, 10)
        self.attempt_to_load_balance(f, [], 1000, num_local_schedulers, 100)

    def testLoadBalancingWithDependencies(self):
        # This test ensures that tasks are being assigned to all local
        # schedulers in a roughly equal manner even when the tasks have
        # dependencies.
        num_workers = 3
        num_local_schedulers = 3
        ray.worker._init(
            start_ray_local=True,
            num_workers=num_workers,
            num_local_schedulers=num_local_schedulers)

        @ray.remote
        def f(x):
            time.sleep(0.010)
            return ray.worker.global_worker.plasma_client.store_socket_name

        # This object will be local to one of the local schedulers. Make sure
        # this doesn't prevent tasks from being scheduled on other local
        # schedulers.
        x = ray.put(np.zeros(1000000))

        self.attempt_to_load_balance(f, [x], 100, num_local_schedulers, 25)


def wait_for_num_tasks(num_tasks, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.global_state.task_table()) >= num_tasks:
            return
        time.sleep(0.1)
    raise Exception("Timed out while waiting for global state.")


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.global_state.object_table()) >= num_objects:
            return
        time.sleep(0.1)
    raise Exception("Timed out while waiting for global state.")


@unittest.skipIf(
    os.environ.get("RAY_USE_NEW_GCS", False),
    "New GCS API doesn't have a Python API yet.")
class GlobalStateAPI(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testGlobalStateAPI(self):
        with pytest.raises(Exception):
            ray.global_state.object_table()

        with pytest.raises(Exception):
            ray.global_state.task_table()

        with pytest.raises(Exception):
            ray.global_state.client_table()

        with pytest.raises(Exception):
            ray.global_state.function_table()

        with pytest.raises(Exception):
            ray.global_state.log_files()

        ray.init(num_cpus=5, num_gpus=3, resources={"CustomResource": 1})

        resources = {"CPU": 5, "GPU": 3, "CustomResource": 1}
        assert ray.global_state.cluster_resources() == resources

        assert ray.global_state.object_table() == {}

        driver_id = ray.experimental.state.binary_to_hex(
            ray.worker.global_worker.worker_id)
        driver_task_id = ray.experimental.state.binary_to_hex(
            ray.worker.global_worker.current_task_id.id())

        # One task is put in the task table which corresponds to this driver.
        wait_for_num_tasks(1)
        task_table = ray.global_state.task_table()
        assert len(task_table) == 1
        assert driver_task_id == list(task_table.keys())[0]
        if not ray.worker.global_worker.use_raylet:
            assert (task_table[driver_task_id]["State"] ==
                    ray.experimental.state.TASK_STATUS_RUNNING)
        if not ray.worker.global_worker.use_raylet:
            task_spec = task_table[driver_task_id]["TaskSpec"]
        else:
            assert len(task_table[driver_task_id]) == 1
            task_spec = task_table[driver_task_id][0]["TaskSpec"]

        assert task_spec["TaskID"] == driver_task_id
        assert task_spec["ActorID"] == ray_constants.ID_SIZE * "ff"
        assert task_spec["Args"] == []
        assert task_spec["DriverID"] == driver_id
        assert task_spec["FunctionID"] == ray_constants.ID_SIZE * "ff"
        assert task_spec["ReturnObjectIDs"] == []

        client_table = ray.global_state.client_table()
        node_ip_address = ray.worker.global_worker.node_ip_address

        if not ray.worker.global_worker.use_raylet:
            assert len(client_table[node_ip_address]) == 3
            manager_client = [
                c for c in client_table[node_ip_address]
                if c["ClientType"] == "plasma_manager"
            ][0]
        else:
            assert len(client_table) == 1
            assert client_table[0]["NodeManagerAddress"] == node_ip_address

        @ray.remote
        def f(*xs):
            return 1

        x_id = ray.put(1)
        result_id = f.remote(1, "hi", x_id)

        # Wait for one additional task to complete.
        start_time = time.time()
        while time.time() - start_time < 10:
            wait_for_num_tasks(1 + 1)
            task_table = ray.global_state.task_table()
            assert len(task_table) == 1 + 1
            task_id_set = set(task_table.keys())
            task_id_set.remove(driver_task_id)
            task_id = list(task_id_set)[0]
            if ray.worker.global_worker.use_raylet:
                break
            if (task_table[task_id]["State"] ==
                    ray.experimental.state.TASK_STATUS_DONE):
                break
            time.sleep(0.1)
        function_table = ray.global_state.function_table()
        if not ray.worker.global_worker.use_raylet:
            task_spec = task_table[task_id]["TaskSpec"]
        else:
            task_spec = task_table[task_id][0]["TaskSpec"]
        assert task_spec["ActorID"] == ray_constants.ID_SIZE * "ff"
        assert task_spec["Args"] == [1, "hi", x_id]
        assert task_spec["DriverID"] == driver_id
        assert task_spec["ReturnObjectIDs"] == [result_id]
        function_table_entry = function_table[task_spec["FunctionID"]]
        assert function_table_entry["Name"] == "runtest.f"
        assert function_table_entry["DriverID"] == driver_id
        assert function_table_entry["Module"] == "runtest"

        assert task_table[task_id] == ray.global_state.task_table(task_id)

        # Wait for two objects, one for the x_id and one for result_id.
        wait_for_num_objects(2)

        def wait_for_object_table():
            timeout = 10
            start_time = time.time()
            while time.time() - start_time < timeout:
                object_table = ray.global_state.object_table()
                tables_ready = (
                    object_table[x_id]["ManagerIDs"] is not None
                    and object_table[result_id]["ManagerIDs"] is not None)
                if tables_ready:
                    return
                time.sleep(0.1)
            raise Exception("Timed out while waiting for object table to "
                            "update.")

        # Wait for the object table to be updated.
        if not ray.worker.global_worker.use_raylet:
            wait_for_object_table()

        object_table = ray.global_state.object_table()
        assert len(object_table) == 2

        if not ray.worker.global_worker.use_raylet:
            db_client_id = manager_client["DBClientID"]
            assert object_table[x_id]["IsPut"] is True
            assert object_table[x_id]["TaskID"] == driver_task_id
            assert object_table[x_id]["ManagerIDs"] == [db_client_id]

            assert object_table[result_id]["IsPut"] is False
            assert object_table[result_id]["TaskID"] == task_id
            assert object_table[result_id]["ManagerIDs"] == [db_client_id]

        else:
            assert len(object_table[x_id]) == 1
            assert object_table[x_id][0]["IsEviction"] is False
            assert object_table[x_id][0]["NumEvictions"] == 0

            assert len(object_table[result_id]) == 1
            assert object_table[result_id][0]["IsEviction"] is False
            assert object_table[result_id][0]["NumEvictions"] == 0

        assert object_table[x_id] == ray.global_state.object_table(x_id)
        object_table_entry = ray.global_state.object_table(result_id)
        assert object_table[result_id] == object_table_entry

    def testLogFileAPI(self):
        ray.init(redirect_worker_output=True)

        message = "unique message"

        @ray.remote
        def f():
            print(message)
            # The call to sys.stdout.flush() seems to be necessary when using
            # the system Python 2.7 on Ubuntu.
            sys.stdout.flush()

        ray.get(f.remote())

        # Make sure that the message appears in the log files.
        start_time = time.time()
        found_message = False
        while time.time() - start_time < 10:
            log_files = ray.global_state.log_files()
            for ip, innerdict in log_files.items():
                for filename, contents in innerdict.items():
                    contents_str = "".join(contents)
                    if message in contents_str:
                        found_message = True
            if found_message:
                break
            time.sleep(0.1)

        assert found_message is True

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray (nor is it intended to).")
    def testTaskProfileAPI(self):
        ray.init(redirect_output=True)

        @ray.remote
        def f():
            return 1

        num_calls = 5
        [f.remote() for _ in range(num_calls)]

        # Make sure the event log has the correct number of events.
        start_time = time.time()
        while time.time() - start_time < 10:
            profiles = ray.global_state.task_profiles(
                100, start=0, end=time.time())
            limited_profiles = ray.global_state.task_profiles(
                1, start=0, end=time.time())
            if len(profiles) == num_calls and len(limited_profiles) == 1:
                break
            time.sleep(0.1)
        assert len(profiles) == num_calls
        assert len(limited_profiles) == 1

        # Make sure that each entry is properly formatted.
        for task_id, data in profiles.items():
            assert "execute_start" in data
            assert "execute_end" in data
            assert "get_arguments_start" in data
            assert "get_arguments_end" in data
            assert "store_outputs_start" in data
            assert "store_outputs_end" in data

    def testWorkers(self):
        num_workers = 3
        ray.init(
            redirect_worker_output=True,
            num_cpus=num_workers,
            num_workers=num_workers)

        @ray.remote
        def f():
            return id(ray.worker.global_worker), os.getpid()

        # Wait until all of the workers have started.
        worker_ids = set()
        while len(worker_ids) != num_workers:
            worker_ids = set(ray.get([f.remote() for _ in range(10)]))

        worker_info = ray.global_state.workers()
        assert len(worker_info) >= num_workers
        for worker_id, info in worker_info.items():
            assert "node_ip_address" in info
            assert "local_scheduler_socket" in info
            assert "plasma_manager_socket" in info
            assert "plasma_store_socket" in info
            assert "stderr_file" in info
            assert "stdout_file" in info

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testDumpTraceFile(self):
        ray.init(redirect_output=True)

        @ray.remote
        def f(*xs):
            return 1

        @ray.remote
        class Foo(object):
            def __init__(self):
                pass

            def method(self):
                pass

        # We use a number of test objects because objects that are not JSON
        # serializable caused problems in the past.
        test_objects = [
            0, 0.5, "hi", b"hi",
            ray.put(0),
            np.zeros(3), [0], (0, ), {
                0: 0
            }, True, False, None
        ]
        ray.get([f.remote(obj) for obj in test_objects])
        actors = [Foo.remote() for _ in range(5)]
        ray.get([actor.method.remote() for actor in actors])
        ray.get([actor.method.remote() for actor in actors])

        path = os.path.join("/tmp/ray_test_trace")
        task_info = ray.global_state.task_profiles(
            100, start=0, end=time.time())
        ray.global_state.dump_catapult_trace(path, task_info)

        # TODO(rkn): This test is not perfect because it does not verify that
        # the visualization actually renders (e.g., the context of the dumped
        # trace could be malformed).

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testFlushAPI(self):
        ray.init(num_cpus=1)

        @ray.remote
        def f():
            return 1

        [ray.put(1) for _ in range(10)]
        ray.get([f.remote() for _ in range(10)])

        # Wait until all of the task and object information has been stored in
        # Redis. Note that since a given key may be updated multiple times
        # (e.g., multiple calls to TaskTableUpdate), this is an attempt to wait
        # until all updates have happened. Note that in a real application we
        # could encounter this kind of issue as well.
        while True:
            object_table = ray.global_state.object_table()
            task_table = ray.global_state.task_table()

            tables_ready = True

            if len(object_table) != 20:
                tables_ready = False

            for object_info in object_table.values():
                if len(object_info) != 5:
                    tables_ready = False
                if (object_info["ManagerIDs"] is None
                        or object_info["DataSize"] == -1
                        or object_info["Hash"] == ""):
                    tables_ready = False

            if len(task_table) != 10 + 1:
                tables_ready = False

            driver_task_id = ray.utils.binary_to_hex(
                ray.worker.global_worker.current_task_id.id())

            for info in task_table.values():
                if info["State"] != ray.experimental.state.TASK_STATUS_DONE:
                    if info["TaskSpec"]["TaskID"] != driver_task_id:
                        tables_ready = False

            if tables_ready:
                break

        # Flush the tables.
        ray.experimental.flush_redis_unsafe()
        ray.experimental.flush_task_and_object_metadata_unsafe()

        # Make sure the tables are empty.
        assert len(ray.global_state.object_table()) == 0
        assert len(ray.global_state.task_table()) == 0

        # Run some more tasks.
        ray.get([f.remote() for _ in range(10)])

        while len(ray.global_state.task_table()) != 0:
            ray.experimental.flush_finished_tasks_unsafe()

        # Make sure that we can call this method (but it won't do anything in
        # this test case).
        ray.experimental.flush_evicted_objects_unsafe()


if __name__ == "__main__":
    unittest.main(verbosity=2)
