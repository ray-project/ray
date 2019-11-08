# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
from concurrent.futures import ThreadPoolExecutor
import glob
import io
import json
import logging
import os
import random
import re
import setproctitle
import shutil
import six
import socket
import string
import subprocess
import sys
import tempfile
import threading
import time

import numpy as np
import pickle
import pytest

import ray
from ray import signature
import ray.ray_constants as ray_constants
import ray.tests.cluster_utils
import ray.tests.utils

from ray.tests.utils import RayTestTimeoutException

logger = logging.getLogger(__name__)


def test_simple_serialization(ray_start_regular):
    primitive_objects = [
        # Various primitive types.
        0,
        0.0,
        0.9,
        1 << 62,
        1 << 999,
        "a",
        string.printable,
        "\u262F",
        u"hello world",
        u"\xff\xfe\x9c\x001\x000\x00",
        None,
        True,
        False,
        [],
        (),
        {},
        type,
        int,
        set(),
        # Collections types.
        collections.Counter([np.random.randint(0, 10) for _ in range(100)]),
        collections.OrderedDict([("hello", 1), ("world", 2)]),
        collections.defaultdict(lambda: 0, [("hello", 1), ("world", 2)]),
        collections.defaultdict(lambda: [], [("hello", 1), ("world", 2)]),
        collections.deque([1, 2, 3, "a", "b", "c", 3.5]),
        # Numpy dtypes.
        np.int8(3),
        np.int32(4),
        np.int64(5),
        np.uint8(3),
        np.uint32(4),
        np.uint64(5),
        np.float32(1.9),
        np.float64(1.9),
    ]

    if sys.version_info < (3, 0):
        primitive_objects.append(long(0))  # noqa: E501,F821

    composite_objects = (
        [[obj]
         for obj in primitive_objects] + [(obj, )
                                          for obj in primitive_objects] + [{
                                              (): obj
                                          } for obj in primitive_objects])

    @ray.remote
    def f(x):
        return x

    # Check that we can pass arguments by value to remote functions and
    # that they are uncorrupted.
    for obj in primitive_objects + composite_objects:
        new_obj_1 = ray.get(f.remote(obj))
        new_obj_2 = ray.get(ray.put(obj))
        assert obj == new_obj_1
        assert obj == new_obj_2
        # TODO(rkn): The numpy dtypes currently come back as regular integers
        # or floats.
        if type(obj).__module__ != "numpy":
            assert type(obj) == type(new_obj_1)
            assert type(obj) == type(new_obj_2)


def test_fair_queueing(shutdown_only):
    ray.init(
        num_cpus=1, _internal_config=json.dumps({
            "fair_queueing_enabled": 1
        }))

    @ray.remote
    def h():
        return 0

    @ray.remote
    def g():
        return ray.get(h.remote())

    @ray.remote
    def f():
        return ray.get(g.remote())

    # This will never finish without fair queueing of {f, g, h}:
    # https://github.com/ray-project/ray/issues/3644
    ready, _ = ray.wait(
        [f.remote() for _ in range(1000)], timeout=60.0, num_returns=1000)
    assert len(ready) == 1000, len(ready)


def complex_serialization(use_pickle):
    def assert_equal(obj1, obj2):
        module_numpy = (type(obj1).__module__ == np.__name__
                        or type(obj2).__module__ == np.__name__)
        if module_numpy:
            empty_shape = ((hasattr(obj1, "shape") and obj1.shape == ())
                           or (hasattr(obj2, "shape") and obj2.shape == ()))
            if empty_shape:
                # This is a special case because currently
                # np.testing.assert_equal fails because we do not properly
                # handle different numerical types.
                assert obj1 == obj2, ("Objects {} and {} are "
                                      "different.".format(obj1, obj2))
            else:
                np.testing.assert_equal(obj1, obj2)
        elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
            special_keys = ["_pytype_"]
            assert (set(list(obj1.__dict__.keys()) + special_keys) == set(
                list(obj2.__dict__.keys()) + special_keys)), (
                    "Objects {} and {} are different.".format(obj1, obj2))
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
            assert len(obj1) == len(obj2), ("Objects {} and {} are tuples "
                                            "with different lengths.".format(
                                                obj1, obj2))
            for i in range(len(obj1)):
                assert_equal(obj1[i], obj2[i])
        elif (ray.serialization.is_named_tuple(type(obj1))
              or ray.serialization.is_named_tuple(type(obj2))):
            assert len(obj1) == len(obj2), (
                "Objects {} and {} are named "
                "tuples with different lengths.".format(obj1, obj2))
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
        string.printable, "\u262F", u"hello world",
        u"\xff\xfe\x9c\x001\x000\x00", None, True, False, [], (), {},
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
        {
            "obj{}".format(i): np.random.normal(size=[100, 100])
            for i in range(10)
        },
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
        },
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

    Point = collections.namedtuple("Point", ["x", "y"])
    NamedTupleExample = collections.namedtuple(
        "Example", "field1, field2, field3, field4, field5")

    CUSTOM_OBJECTS = [
        Exception("Test object."),
        CustomError(),
        Point(11, y=22),
        Foo(),
        Bar(),
        Baz(),  # Qux(), SubQux(),
        NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3]),
    ]

    # Test dataclasses in Python 3.7.
    if sys.version_info >= (3, 7):
        from dataclasses import make_dataclass

        DataClass0 = make_dataclass("DataClass0", [("number", int)])

        CUSTOM_OBJECTS.append(DataClass0(number=3))

        class CustomClass(object):
            def __init__(self, value):
                self.value = value

        DataClass1 = make_dataclass("DataClass1", [("custom", CustomClass)])

        class DataClass2(DataClass1):
            @classmethod
            def from_custom(cls, data):
                custom = CustomClass(data)
                return cls(custom)

            def __reduce__(self):
                return (self.from_custom, (self.custom.value, ))

        CUSTOM_OBJECTS.append(DataClass2(custom=CustomClass(43)))

    BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

    LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
    TUPLE_OBJECTS = [(obj, ) for obj in BASE_OBJECTS]
    # The check that type(obj).__module__ != "numpy" should be unnecessary, but
    # otherwise this seems to fail on Mac OS X on Travis.
    DICT_OBJECTS = ([{
        obj: obj
    } for obj in PRIMITIVE_OBJECTS if (
        obj.__hash__ is not None and type(obj).__module__ != "numpy")] + [{
            0: obj
        } for obj in BASE_OBJECTS] + [{
            Foo(123): Foo(456)
        }])

    RAY_TEST_OBJECTS = (
        BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS)

    @ray.remote
    def f(x):
        return x

    # Check that we can pass arguments by value to remote functions and
    # that they are uncorrupted.
    for obj in RAY_TEST_OBJECTS:
        assert_equal(obj, ray.get(f.remote(obj)))
        assert_equal(obj, ray.get(ray.put(obj)))

    # Test StringIO serialization
    s = io.StringIO(u"Hello, world!\n")
    s.seek(0)
    line = s.readline()
    s.seek(0)
    assert ray.get(ray.put(s)).readline() == line


def test_complex_serialization(ray_start_regular):
    complex_serialization(use_pickle=False)


def test_complex_serialization_with_pickle(shutdown_only):
    ray.init(use_pickle=True)
    complex_serialization(use_pickle=True)


def test_nested_functions(ray_start_regular):
    # Make sure that remote functions can use other values that are defined
    # after the remote function but before the first function invocation.
    @ray.remote
    def f():
        return g(), ray.get(h.remote())

    def g():
        return 1

    @ray.remote
    def h():
        return 2

    assert ray.get(f.remote()) == (1, 2)

    # Test a remote function that recursively calls itself.

    @ray.remote
    def factorial(n):
        if n == 0:
            return 1
        return n * ray.get(factorial.remote(n - 1))

    assert ray.get(factorial.remote(0)) == 1
    assert ray.get(factorial.remote(1)) == 1
    assert ray.get(factorial.remote(2)) == 2
    assert ray.get(factorial.remote(3)) == 6
    assert ray.get(factorial.remote(4)) == 24
    assert ray.get(factorial.remote(5)) == 120

    # Test remote functions that recursively call each other.

    @ray.remote
    def factorial_even(n):
        assert n % 2 == 0
        if n == 0:
            return 1
        return n * ray.get(factorial_odd.remote(n - 1))

    @ray.remote
    def factorial_odd(n):
        assert n % 2 == 1
        return n * ray.get(factorial_even.remote(n - 1))

    assert ray.get(factorial_even.remote(4)) == 24
    assert ray.get(factorial_odd.remote(5)) == 120


def test_ray_recursive_objects(ray_start_regular):
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

    if ray.worker.global_worker.use_pickle:
        # Serialize the recursive objects.
        for obj in recursive_objects:
            ray.put(obj)
    else:
        # Check that exceptions are thrown when we serialize the recursive
        # objects.
        for obj in recursive_objects:
            with pytest.raises(Exception):
                ray.put(obj)


def test_passing_arguments_by_value_out_of_the_box(ray_start_regular):
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


def test_putting_object_that_closes_over_object_id(ray_start_regular):
    # This test is here to prevent a regression of
    # https://github.com/ray-project/ray/issues/1317.

    class Foo(object):
        def __init__(self):
            self.val = ray.put(0)

        def method(self):
            f

    f = Foo()
    ray.put(f)


def test_put_get(shutdown_only):
    ray.init(num_cpus=0)

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


def custom_serializers():
    class Foo(object):
        def __init__(self):
            self.x = 3

    def custom_serializer(obj):
        return 3, "string1", type(obj).__name__

    def custom_deserializer(serialized_obj):
        return serialized_obj, "string2"

    ray.register_custom_serializer(
        Foo, serializer=custom_serializer, deserializer=custom_deserializer)

    assert ray.get(ray.put(Foo())) == ((3, "string1", Foo.__name__), "string2")

    class Bar(object):
        def __init__(self):
            self.x = 3

    ray.register_custom_serializer(
        Bar, serializer=custom_serializer, deserializer=custom_deserializer)

    @ray.remote
    def f():
        return Bar()

    assert ray.get(f.remote()) == ((3, "string1", Bar.__name__), "string2")


def test_custom_serializers(ray_start_regular):
    custom_serializers()


def test_custom_serializers_with_pickle(shutdown_only):
    ray.init(use_pickle=True)
    custom_serializers()

    class Foo(object):
        def __init__(self):
            self.x = 4

    # Test the pickle serialization backend without serializer.
    # NOTE: 'use_pickle' here is different from 'use_pickle' in
    # ray.init
    ray.register_custom_serializer(Foo, use_pickle=True)

    @ray.remote
    def f():
        return Foo()

    assert type(ray.get(f.remote())) == Foo


def test_serialization_final_fallback(ray_start_regular):
    pytest.importorskip("catboost")
    # This test will only run when "catboost" is installed.
    from catboost import CatBoostClassifier

    model = CatBoostClassifier(
        iterations=2,
        depth=2,
        learning_rate=1,
        loss_function="Logloss",
        logging_level="Verbose")

    reconstructed_model = ray.get(ray.put(model))
    assert set(model.get_params().items()) == set(
        reconstructed_model.get_params().items())


def test_register_class(ray_start_2_cpus):
    # Check that putting an object of a class that has not been registered
    # throws an exception.
    class TempClass(object):
        pass

    ray.get(ray.put(TempClass()))

    # Test passing custom classes into remote functions from the driver.
    @ray.remote
    def f(x):
        return x

    class Foo(object):
        def __init__(self, value=0):
            self.value = value

        def __hash__(self):
            return hash(self.value)

        def __eq__(self, other):
            return other.value == self.value

    foo = ray.get(f.remote(Foo(7)))
    assert foo == Foo(7)

    regex = re.compile(r"\d+\.\d*")
    new_regex = ray.get(f.remote(regex))
    # This seems to fail on the system Python 3 that comes with
    # Ubuntu, so it is commented out for now:
    # assert regex == new_regex
    # Instead, we do this:
    assert regex.pattern == new_regex.pattern

    class TempClass1(object):
        def __init__(self):
            self.value = 1

    # Test returning custom classes created on workers.
    @ray.remote
    def g():
        class TempClass2(object):
            def __init__(self):
                self.value = 2

        return TempClass1(), TempClass2()

    object_1, object_2 = ray.get(g.remote())
    assert object_1.value == 1
    assert object_2.value == 2

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


def test_keyword_args(ray_start_regular):
    @ray.remote
    def keyword_fct1(a, b="hello"):
        return "{} {}".format(a, b)

    @ray.remote
    def keyword_fct2(a="hello", b="world"):
        return "{} {}".format(a, b)

    @ray.remote
    def keyword_fct3(a, b, c="hello", d="world"):
        return "{} {} {} {}".format(a, b, c, d)

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


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_args_starkwargs(ray_start_regular):
    def starkwargs(a, b, **kwargs):
        return a, b, kwargs

    class TestActor(object):
        def starkwargs(self, a, b, **kwargs):
            return a, b, kwargs

    def test_function(fn, remote_fn):
        assert fn(1, 2, x=3) == ray.get(remote_fn.remote(1, 2, x=3))
        with pytest.raises(TypeError):
            remote_fn.remote(3)

    remote_test_function = ray.remote(test_function)

    remote_starkwargs = ray.remote(starkwargs)
    test_function(starkwargs, remote_starkwargs)
    ray.get(remote_test_function.remote(starkwargs, remote_starkwargs))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.starkwargs
    local_actor = TestActor()
    local_method = local_actor.starkwargs
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_args_named_and_star(ray_start_regular):
    def hello(a, x="hello", **kwargs):
        return a, x, kwargs

    class TestActor(object):
        def hello(self, a, x="hello", **kwargs):
            return a, x, kwargs

    def test_function(fn, remote_fn):
        assert fn(1, x=2, y=3) == ray.get(remote_fn.remote(1, x=2, y=3))
        assert fn(1, 2, y=3) == ray.get(remote_fn.remote(1, 2, y=3))
        assert fn(1, y=3) == ray.get(remote_fn.remote(1, y=3))

        assert fn(1, ) == ray.get(remote_fn.remote(1, ))
        assert fn(1) == ray.get(remote_fn.remote(1))

        with pytest.raises(TypeError):
            remote_fn.remote(1, 2, x=3)

    remote_test_function = ray.remote(test_function)

    remote_hello = ray.remote(hello)
    test_function(hello, remote_hello)
    ray.get(remote_test_function.remote(hello, remote_hello))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.hello
    local_actor = TestActor()
    local_method = local_actor.hello
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_args_stars_after(ray_start_regular):
    def star_args_after(a="hello", b="heo", *args, **kwargs):
        return a, b, args, kwargs

    class TestActor(object):
        def star_args_after(self, a="hello", b="heo", *args, **kwargs):
            return a, b, args, kwargs

    def test_function(fn, remote_fn):
        assert fn("hi", "hello", 2) == ray.get(
            remote_fn.remote("hi", "hello", 2))
        assert fn(
            "hi", "hello", 2, hi="hi") == ray.get(
                remote_fn.remote("hi", "hello", 2, hi="hi"))
        assert fn(hi="hi") == ray.get(remote_fn.remote(hi="hi"))

    remote_test_function = ray.remote(test_function)

    remote_star_args_after = ray.remote(star_args_after)
    test_function(star_args_after, remote_star_args_after)
    ray.get(
        remote_test_function.remote(star_args_after, remote_star_args_after))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.star_args_after
    local_actor = TestActor()
    local_method = local_actor.star_args_after
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


def test_variable_number_of_args(shutdown_only):
    @ray.remote
    def varargs_fct1(*a):
        return " ".join(map(str, a))

    @ray.remote
    def varargs_fct2(a, *b):
        return " ".join(map(str, b))

    ray.init(num_cpus=1)

    x = varargs_fct1.remote(0, 1, 2)
    assert ray.get(x) == "0 1 2"
    x = varargs_fct2.remote(0, 1, 2)
    assert ray.get(x) == "1 2"

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

        self.ray_start()

        ray.get(no_op.remote())


def test_defining_remote_functions(shutdown_only):
    ray.init(num_cpus=3)

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
            logger.info("Still using old definition of f, trying again.")

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

    assert np.alltrue(ray.get(h.remote()) == np.zeros([3, 5]))

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


def test_submit_api(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1, resources={"Custom": 1})

    @ray.remote
    def f(n):
        return list(range(n))

    @ray.remote
    def g():
        return ray.get_gpu_ids()

    assert f._remote([0], num_return_vals=0) is None
    id1 = f._remote(args=[1], num_return_vals=1)
    assert ray.get(id1) == [0]
    id1, id2 = f._remote(args=[2], num_return_vals=2)
    assert ray.get([id1, id2]) == [0, 1]
    id1, id2, id3 = f._remote(args=[3], num_return_vals=3)
    assert ray.get([id1, id2, id3]) == [0, 1, 2]
    assert ray.get(
        g._remote(args=[], num_cpus=1, num_gpus=1,
                  resources={"Custom": 1})) == [0]
    infeasible_id = g._remote(args=[], resources={"NonexistentCustom": 1})
    assert ray.get(g._remote()) == []
    ready_ids, remaining_ids = ray.wait([infeasible_id], timeout=0.05)
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

    @ray.remote
    class Actor2(object):
        def __init__(self):
            pass

        def method(self):
            pass

    a = Actor._remote(
        args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

    a2 = Actor2._remote()
    ray.get(a2.method._remote())

    id1, id2, id3, id4 = a.method._remote(
        args=["test"], kwargs={"b": 2}, num_return_vals=4)
    assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]


def test_many_fractional_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=2, resources={"Custom": 2})

    @ray.remote
    def g():
        return 1

    @ray.remote
    def f(block, accepted_resources):
        true_resources = {
            resource: value[0][1]
            for resource, value in ray.get_resource_ids().items()
        }
        if block:
            ray.get(g.remote())
        return true_resources == accepted_resources

    # Check that the resource are assigned correctly.
    result_ids = []
    for rand1, rand2, rand3 in np.random.uniform(size=(100, 3)):
        resource_set = {"CPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_cpus=rand1))

        resource_set = {"CPU": 1, "GPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_gpus=rand1))

        resource_set = {"CPU": 1, "Custom": int(rand1 * 10000) / 10000}
        result_ids.append(
            f._remote([False, resource_set], resources={"Custom": rand1}))

        resource_set = {
            "CPU": int(rand1 * 10000) / 10000,
            "GPU": int(rand2 * 10000) / 10000,
            "Custom": int(rand3 * 10000) / 10000
        }
        result_ids.append(
            f._remote(
                [False, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3}))
        result_ids.append(
            f._remote(
                [True, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3}))
    assert all(ray.get(result_ids))

    # Check that the available resources at the end are the same as the
    # beginning.
    stop_time = time.time() + 10
    correct_available_resources = False
    while time.time() < stop_time:
        if (ray.available_resources()["CPU"] == 2.0
                and ray.available_resources()["GPU"] == 2.0
                and ray.available_resources()["Custom"] == 2.0):
            correct_available_resources = True
            break
    if not correct_available_resources:
        assert False, "Did not get correct available resources."


def test_get_multiple(ray_start_regular):
    object_ids = [ray.put(i) for i in range(10)]
    assert ray.get(object_ids) == list(range(10))

    # Get a random choice of object IDs with duplicates.
    indices = list(np.random.choice(range(10), 5))
    indices += indices
    results = ray.get([object_ids[i] for i in indices])
    assert results == indices


def test_get_multiple_experimental(ray_start_regular):
    object_ids = [ray.put(i) for i in range(10)]

    object_ids_tuple = tuple(object_ids)
    assert ray.experimental.get(object_ids_tuple) == list(range(10))

    object_ids_nparray = np.array(object_ids)
    assert ray.experimental.get(object_ids_nparray) == list(range(10))


def test_get_dict(ray_start_regular):
    d = {str(i): ray.put(i) for i in range(5)}
    for i in range(5, 10):
        d[str(i)] = i
    result = ray.experimental.get(d)
    expected = {str(i): i for i in range(10)}
    assert result == expected


def test_direct_actor_enabled(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def f(self, x):
            return x * 2

    a = Actor._remote(is_direct_call=True)
    obj_id = a.f.remote(1)
    # it is not stored in plasma
    assert not ray.worker.global_worker.core_worker.object_exists(obj_id)
    assert ray.get(obj_id) == 2


def test_direct_actor_large_objects(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def f(self):
            time.sleep(1)
            return np.zeros(10000000)

    a = Actor._remote(is_direct_call=True)
    obj_id = a.f.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(obj_id)
    done, _ = ray.wait([obj_id])
    assert len(done) == 1
    assert ray.worker.global_worker.core_worker.object_exists(obj_id)
    assert isinstance(ray.get(obj_id), np.ndarray)


def test_direct_actor_errors(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def f(self, x):
            return x * 2

    @ray.remote
    def f(x):
        return 1

    a = Actor._remote(is_direct_call=True)

    # cannot pass returns to other methods directly
    with pytest.raises(Exception):
        ray.get(f.remote(a.f.remote(2)))

    # cannot pass returns to other methods even in a list
    with pytest.raises(Exception):
        ray.get(f.remote([a.f.remote(2)]))


def test_direct_actor_pass_by_ref(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def f(self, x):
            return x * 2

    @ray.remote
    def f(x):
        return x

    @ray.remote
    def error():
        sys.exit(0)

    a = Actor._remote(is_direct_call=True)
    assert ray.get(a.f.remote(f.remote(1))) == 2

    fut = [a.f.remote(f.remote(i)) for i in range(100)]
    assert ray.get(fut) == [i * 2 for i in range(100)]

    # propagates errors for pass by ref
    with pytest.raises(Exception):
        ray.get(a.f.remote(error.remote()))


def test_direct_actor_pass_by_ref_order_optimization(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def f(self, x):
            pass

    a = Actor._remote(is_direct_call=True)

    @ray.remote
    def fast_value():
        print("fast value")
        pass

    @ray.remote
    def slow_value():
        print("start sleep")
        time.sleep(30)

    @ray.remote
    def runner(f):
        print("runner", a, f)
        return ray.get(a.f.remote(f.remote()))

    runner.remote(slow_value)
    time.sleep(1)
    x2 = runner.remote(fast_value)
    start = time.time()
    ray.get(x2)
    delta = time.time() - start
    assert delta < 10, "did not skip slow value"


def test_direct_actor_recursive(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self, delegate=None):
            self.delegate = delegate

        def f(self, x):
            if self.delegate:
                return ray.get(self.delegate.f.remote(x))
            return x * 2

    a = Actor._remote(is_direct_call=True)
    b = Actor._remote(args=[a], is_direct_call=False)
    c = Actor._remote(args=[b], is_direct_call=True)

    result = ray.get([c.f.remote(i) for i in range(100)])
    assert result == [x * 2 for x in range(100)]

    result, _ = ray.wait([c.f.remote(i) for i in range(100)], num_returns=100)
    result = ray.get(result)
    assert result == [x * 2 for x in range(100)]


def test_direct_actor_concurrent(ray_start_regular):
    @ray.remote
    class Batcher(object):
        def __init__(self):
            self.batch = []
            self.event = threading.Event()

        def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= 3:
                self.event.set()
            else:
                self.event.wait()
            return sorted(self.batch)

    a = Batcher.options(is_direct_call=True, max_concurrency=3).remote()
    x1 = a.add.remote(1)
    x2 = a.add.remote(2)
    x3 = a.add.remote(3)
    r1 = ray.get(x1)
    r2 = ray.get(x2)
    r3 = ray.get(x3)
    assert r1 == [1, 2, 3]
    assert r1 == r2 == r3


def test_wait(ray_start_regular):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    ready_ids, remaining_ids = ray.wait(objectids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(objectids, num_returns=4)
    assert set(ready_ids) == set(objectids)
    assert remaining_ids == []

    objectids = [f.remote(0.5), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=1.75, num_returns=4)
    assert time.time() - start_time < 2
    assert len(ready_ids) == 3
    assert len(remaining_ids) == 1
    ray.wait(objectids)
    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=5.0)
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


def test_wait_iterables(ray_start_regular):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return 1

    objectids = (f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5))
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


def test_multiple_waits_and_gets(shutdown_only):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_cpus=3)

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


def test_caching_functions_to_run(shutdown_only):
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

    ray.init(num_cpus=1)

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


def test_running_function_on_all_workers(ray_start_regular):
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


def test_profiling_api(ray_start_2_cpus):
    @ray.remote
    def f():
        with ray.profile("custom_event", extra_data={"name": "custom name"}):
            pass

    ray.put(1)
    object_id = f.remote()
    ray.wait([object_id])
    ray.get(object_id)

    # Wait until all of the profiling information appears in the profile
    # table.
    timeout_seconds = 20
    start_time = time.time()
    while True:
        profile_data = ray.timeline()
        event_types = {event["cat"] for event in profile_data}
        expected_types = [
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

        if time.time() - start_time > timeout_seconds:
            raise RayTestTimeoutException(
                "Timed out while waiting for information in "
                "profile table. Missing events: {}.".format(
                    set(expected_types) - set(event_types)))

        # The profiling information only flushes once every second.
        time.sleep(1.1)


def test_wait_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    cluster.add_node(num_cpus=1, resources={"RemoteResource": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"RemoteResource": 1})
    def f():
        return

    # Make sure we have enough workers on the remote nodes to execute some
    # tasks.
    tasks = [f.remote() for _ in range(10)]
    start = time.time()
    ray.get(tasks)
    end = time.time()

    # Submit some more tasks that can only be executed on the remote nodes.
    tasks = [f.remote() for _ in range(10)]
    # Sleep for a bit to let the tasks finish.
    time.sleep((end - start) * 2)
    _, unready = ray.wait(tasks, num_returns=len(tasks), timeout=0)
    # All remote tasks should have finished.
    assert len(unready) == 0


def test_object_transfer_dump(ray_start_cluster):
    cluster = ray_start_cluster

    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(resources={str(i): 1}, object_store_memory=10**9)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return

    # These objects will live on different nodes.
    object_ids = [
        f._remote(args=[1], resources={str(i): 1}) for i in range(num_nodes)
    ]

    # Broadcast each object from each machine to each other machine.
    for object_id in object_ids:
        ray.get([
            f._remote(args=[object_id], resources={str(i): 1})
            for i in range(num_nodes)
        ])

    # The profiling information only flushes once every second.
    time.sleep(1.1)

    transfer_dump = ray.object_transfer_timeline()
    # Make sure the transfer dump can be serialized with JSON.
    json.loads(json.dumps(transfer_dump))
    assert len(transfer_dump) >= num_nodes**2
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_receive"
    }) == num_nodes
    assert len({
        event["pid"]
        for event in transfer_dump if event["name"] == "transfer_send"
    }) == num_nodes


def test_identical_function_names(ray_start_regular):
    # Define a bunch of remote functions and make sure that we don't
    # accidentally call an older version.

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


def test_illegal_api_calls(ray_start_regular):

    # Verify that we cannot call put on an ObjectID.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.put(x)
    # Verify that we cannot call get on a regular value.
    with pytest.raises(Exception):
        ray.get(3)


# TODO(hchen): This test currently doesn't work in Python 2. This is likely
# because plasma client isn't thread-safe. This needs to be fixed from the
# Arrow side. See #4107 for relevant discussions.
@pytest.mark.skipif(six.PY2, reason="Doesn't work in Python 2.")
def test_multithreading(ray_start_2_cpus):
    # This test requires at least 2 CPUs to finish since the worker does not
    # release resources when joining the threads.

    def run_test_in_multi_threads(test_case, num_threads=10, num_repeats=25):
        """A helper function that runs test cases in multiple threads."""

        def wrapper():
            for _ in range(num_repeats):
                test_case()
                time.sleep(random.randint(0, 10) / 1000.0)
            return "ok"

        executor = ThreadPoolExecutor(max_workers=num_threads)
        futures = [executor.submit(wrapper) for _ in range(num_threads)]
        for future in futures:
            assert future.result() == "ok"

    @ray.remote
    def echo(value, delay_ms=0):
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        return value

    def test_api_in_multi_threads():
        """Test using Ray api in multiple threads."""

        @ray.remote
        class Echo(object):
            def echo(self, value):
                return value

        # Test calling remote functions in multiple threads.
        def test_remote_call():
            value = random.randint(0, 1000000)
            result = ray.get(echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_remote_call)

        # Test multiple threads calling one actor.
        actor = Echo.remote()

        def test_call_actor():
            value = random.randint(0, 1000000)
            result = ray.get(actor.echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_call_actor)

        # Test put and get.
        def test_put_and_get():
            value = random.randint(0, 1000000)
            result = ray.get(ray.put(value))
            assert value == result

        run_test_in_multi_threads(test_put_and_get)

        # Test multiple threads waiting for objects.
        num_wait_objects = 10
        objects = [
            echo.remote(i, delay_ms=10) for i in range(num_wait_objects)
        ]

        def test_wait():
            ready, _ = ray.wait(
                objects,
                num_returns=len(objects),
                timeout=1000.0,
            )
            assert len(ready) == num_wait_objects
            assert ray.get(ready) == list(range(num_wait_objects))

        run_test_in_multi_threads(test_wait, num_repeats=1)

    # Run tests in a driver.
    test_api_in_multi_threads()

    # Run tests in a worker.
    @ray.remote
    def run_tests_in_worker():
        test_api_in_multi_threads()
        return "ok"

    assert ray.get(run_tests_in_worker.remote()) == "ok"

    # Test actor that runs background threads.
    @ray.remote
    class MultithreadedActor(object):
        def __init__(self):
            self.lock = threading.Lock()
            self.thread_results = []

        def background_thread(self, wait_objects):
            try:
                # Test wait
                ready, _ = ray.wait(
                    wait_objects,
                    num_returns=len(wait_objects),
                    timeout=1000.0,
                )
                assert len(ready) == len(wait_objects)
                for _ in range(20):
                    num = 10
                    # Test remote call
                    results = [echo.remote(i) for i in range(num)]
                    assert ray.get(results) == list(range(num))
                    # Test put and get
                    objects = [ray.put(i) for i in range(num)]
                    assert ray.get(objects) == list(range(num))
                    time.sleep(random.randint(0, 10) / 1000.0)
            except Exception as e:
                with self.lock:
                    self.thread_results.append(e)
            else:
                with self.lock:
                    self.thread_results.append("ok")

        def spawn(self):
            wait_objects = [echo.remote(i, delay_ms=10) for i in range(10)]
            self.threads = [
                threading.Thread(
                    target=self.background_thread, args=(wait_objects, ))
                for _ in range(20)
            ]
            [thread.start() for thread in self.threads]

        def join(self):
            [thread.join() for thread in self.threads]
            assert self.thread_results == ["ok"] * len(self.threads)
            return "ok"

    actor = MultithreadedActor.remote()
    actor.spawn.remote()
    ray.get(actor.join.remote()) == "ok"


def test_free_objects_multi_node(ray_start_cluster):
    # This test will do following:
    # 1. Create 3 raylets that each hold an actor.
    # 2. Each actor creates an object which is the deletion target.
    # 3. Wait 0.1 second for the objects to be deleted.
    # 4. Check that the deletion targets have been deleted.
    # Caution: if remote functions are used instead of actor methods,
    # one raylet may create more than one worker to execute the
    # tasks, so the flushing operations may be executed in different
    # workers and the plasma client holding the deletion target
    # may not be flushed.
    cluster = ray_start_cluster
    config = json.dumps({"object_manager_repeated_push_delay_ms": 1000})
    for i in range(3):
        cluster.add_node(
            num_cpus=1,
            resources={"Custom{}".format(i): 1},
            _internal_config=config)
    ray.init(address=cluster.address)

    class RawActor(object):
        def get(self):
            return ray.worker.global_worker.node.unique_id

    ActorOnNode0 = ray.remote(resources={"Custom0": 1})(RawActor)
    ActorOnNode1 = ray.remote(resources={"Custom1": 1})(RawActor)
    ActorOnNode2 = ray.remote(resources={"Custom2": 1})(RawActor)

    def create(actors):
        a = actors[0].get.remote()
        b = actors[1].get.remote()
        c = actors[2].get.remote()
        (l1, l2) = ray.wait([a, b, c], num_returns=3)
        assert len(l1) == 3
        assert len(l2) == 0
        return (a, b, c)

    def run_one_test(actors, local_only, delete_creating_tasks):
        (a, b, c) = create(actors)
        # The three objects should be generated on different object stores.
        assert ray.get(a) != ray.get(b)
        assert ray.get(a) != ray.get(c)
        assert ray.get(c) != ray.get(b)
        ray.internal.free(
            [a, b, c],
            local_only=local_only,
            delete_creating_tasks=delete_creating_tasks)
        # Wait for the objects to be deleted.
        time.sleep(0.1)
        return (a, b, c)

    actors = [
        ActorOnNode0.remote(),
        ActorOnNode1.remote(),
        ActorOnNode2.remote()
    ]
    # Case 1: run this local_only=False. All 3 objects will be deleted.
    (a, b, c) = run_one_test(actors, False, False)
    (l1, l2) = ray.wait([a, b, c], timeout=0.01, num_returns=1)
    # All the objects are deleted.
    assert len(l1) == 0
    assert len(l2) == 3
    # Case 2: run this local_only=True. Only 1 object will be deleted.
    (a, b, c) = run_one_test(actors, True, False)
    (l1, l2) = ray.wait([a, b, c], timeout=0.01, num_returns=3)
    # One object is deleted and 2 objects are not.
    assert len(l1) == 2
    assert len(l2) == 1
    # The deleted object will have the same store with the driver.
    local_return = ray.worker.global_worker.node.unique_id
    for object_id in l1:
        assert ray.get(object_id) != local_return

    # Case3: These cases test the deleting creating tasks for the object.
    (a, b, c) = run_one_test(actors, False, False)
    task_table = ray.tasks()
    for obj in [a, b, c]:
        assert ray._raylet.compute_task_id(obj).hex() in task_table

    (a, b, c) = run_one_test(actors, False, True)
    task_table = ray.tasks()
    for obj in [a, b, c]:
        assert ray._raylet.compute_task_id(obj).hex() not in task_table


def test_local_mode(shutdown_only):
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
    # Remote functions should return ObjectIDs.
    assert isinstance(xref, ray.ObjectID)
    assert np.alltrue(ray.get(xref) == np.ones([3, 4, 5]))
    y = np.random.normal(size=[11, 12])
    # Check that ray.get(ray.put) is the identity.
    assert np.alltrue(y == ray.get(ray.put(y)))

    # Make sure objects are immutable, this example is why we need to copy
    # arguments before passing them into remote functions in python mode
    aref = local_mode_f.remote()
    assert np.alltrue(ray.get(aref) == np.array([0, 0]))
    bref = local_mode_g.remote(ray.get(aref))
    # Make sure local_mode_g does not mutate aref.
    assert np.alltrue(ray.get(aref) == np.array([0, 0]))
    assert np.alltrue(ray.get(bref) == np.array([1, 0]))

    # wait should return the first num_returns values passed in as the
    # first list and the remaining values as the second list
    num_returns = 5
    object_ids = [ray.put(i) for i in range(20)]
    ready, remaining = ray.wait(
        object_ids, num_returns=num_returns, timeout=None)
    assert ready == object_ids[:num_returns]
    assert remaining == object_ids[num_returns:]

    # Check that ray.put() and ray.internal.free() work in local mode.

    v1 = np.ones(10)
    v2 = np.zeros(10)

    k1 = ray.put(v1)
    assert np.alltrue(v1 == ray.get(k1))
    k2 = ray.put(v2)
    assert np.alltrue(v2 == ray.get(k2))

    ray.internal.free([k1, k2])
    with pytest.raises(Exception):
        ray.get(k1)
    with pytest.raises(Exception):
        ray.get(k2)

    # Should fail silently.
    ray.internal.free([k1, k2])

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

        @ray.method(num_return_vals=3)
        def returns_multiple(self):
            return 1, 2, 3

    test_actor = LocalModeTestClass.remote(np.arange(10))
    obj = test_actor.get_array.remote()
    assert isinstance(obj, ray.ObjectID)
    assert np.alltrue(ray.get(obj) == np.arange(10))

    test_array = np.arange(10)
    # Remote actor functions should not mutate arguments
    test_actor.modify_and_set_array.remote(test_array)
    assert np.alltrue(test_array == np.arange(10))
    # Remote actor functions should keep state
    test_array[0] = -1
    assert np.alltrue(test_array == ray.get(test_actor.get_array.remote()))

    # Check that actor handles work in local mode.

    @ray.remote
    def use_actor_handle(handle):
        array = np.ones(10)
        handle.set_array.remote(array)
        assert np.alltrue(array == ray.get(handle.get_array.remote()))

    ray.get(use_actor_handle.remote(test_actor))

    # Check that exceptions are deferred until ray.get().

    exception_str = "test_basic remote task exception"

    @ray.remote
    def throws():
        raise Exception(exception_str)

    obj = throws.remote()
    with pytest.raises(Exception, match=exception_str):
        ray.get(obj)

    # Check that multiple return values are handled properly.

    @ray.remote(num_return_vals=3)
    def returns_multiple():
        return 1, 2, 3

    obj1, obj2, obj3 = returns_multiple.remote()
    assert ray.get(obj1) == 1
    assert ray.get(obj2) == 2
    assert ray.get(obj3) == 3
    assert ray.get([obj1, obj2, obj3]) == [1, 2, 3]

    obj1, obj2, obj3 = test_actor.returns_multiple.remote()
    assert ray.get(obj1) == 1
    assert ray.get(obj2) == 2
    assert ray.get(obj3) == 3
    assert ray.get([obj1, obj2, obj3]) == [1, 2, 3]

    @ray.remote(num_return_vals=2)
    def returns_multiple_throws():
        raise Exception(exception_str)

    obj1, obj2 = returns_multiple_throws.remote()
    with pytest.raises(Exception, match=exception_str):
        ray.get(obj)
        ray.get(obj1)
    with pytest.raises(Exception, match=exception_str):
        ray.get(obj2)

    # Check that Actors are not overwritten by remote calls from different
    # classes.
    @ray.remote
    class RemoteActor1(object):
        def __init__(self):
            pass

        def function1(self):
            return 0

    @ray.remote
    class RemoteActor2(object):
        def __init__(self):
            pass

        def function2(self):
            return 1

    actor1 = RemoteActor1.remote()
    _ = RemoteActor2.remote()
    assert ray.get(actor1.function1.remote()) == 0


def test_resource_constraints(shutdown_only):
    num_workers = 20
    ray.init(num_cpus=10, num_gpus=2)

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

    time_buffer = 2

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


def test_multi_resource_constraints(shutdown_only):
    num_workers = 20
    ray.init(num_cpus=10, num_gpus=10)

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

    time_buffer = 2

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


def test_gpu_ids(shutdown_only):
    num_gpus = 10
    ray.init(num_cpus=10, num_gpus=num_gpus)

    def get_gpu_ids(num_gpus_per_worker):
        time.sleep(0.1)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == num_gpus_per_worker
        assert (os.environ["CUDA_VISIBLE_DEVICES"] == ",".join(
            [str(i) for i in gpu_ids]))
        for gpu_id in gpu_ids:
            assert gpu_id in range(num_gpus)
        return gpu_ids

    f0 = ray.remote(num_gpus=0)(lambda: get_gpu_ids(0))
    f1 = ray.remote(num_gpus=1)(lambda: get_gpu_ids(1))
    f2 = ray.remote(num_gpus=2)(lambda: get_gpu_ids(2))
    f4 = ray.remote(num_gpus=4)(lambda: get_gpu_ids(4))
    f5 = ray.remote(num_gpus=5)(lambda: get_gpu_ids(5))

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
            raise RayTestTimeoutException(
                "Timed out while waiting for workers to start "
                "up.")

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    assert list_of_ids == 10 * [[]]

    list_of_ids = ray.get([f1.remote() for _ in range(10)])
    set_of_ids = {tuple(gpu_ids) for gpu_ids in list_of_ids}
    assert set_of_ids == {(i, ) for i in range(10)}

    list_of_ids = ray.get([f2.remote(), f4.remote(), f4.remote()])
    all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
    assert set(all_ids) == set(range(10))

    # There are only 10 GPUs, and each task uses 5 GPUs, so there should only
    # be 2 tasks scheduled at a given time.
    t1 = time.time()
    ray.get([f5.remote() for _ in range(20)])
    assert time.time() - t1 >= 10 * 0.1

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


def test_zero_cpus(shutdown_only):
    ray.init(num_cpus=0)

    # We should be able to execute a task that requires 0 CPU resources.
    @ray.remote(num_cpus=0)
    def f():
        return 1

    ray.get(f.remote())

    # We should be able to create an actor that requires 0 CPU resources.
    @ray.remote(num_cpus=0)
    class Actor(object):
        def method(self):
            pass

    a = Actor.remote()
    x = a.method.remote()
    ray.get(x)


def test_zero_cpus_actor(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    node_id = ray.worker.global_worker.node.unique_id

    @ray.remote
    class Foo(object):
        def method(self):
            return ray.worker.global_worker.node.unique_id

    # Make sure tasks and actors run on the remote raylet.
    a = Foo.remote()
    assert ray.get(a.method.remote()) != node_id


def test_fractional_resources(shutdown_only):
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
    f1 = Foo2._remote([], {}, resources={"Custom": 0.7})
    ray.get(f1.method.remote())
    # Make sure that we cannot create an actor that requires 0.7 of the
    # custom resource. TODO(rkn): Re-enable this once ray.wait is
    # implemented.
    f2 = Foo2._remote([], {}, resources={"Custom": 0.7})
    ready, _ = ray.wait([f2.method.remote()], timeout=0.5)
    assert len(ready) == 0
    # Make sure we can start an actor that requries only 0.3 of the custom
    # resource.
    f3 = Foo2._remote([], {}, resources={"Custom": 0.3})
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
        Foo2._remote([], {}, resources={"Custom": 1.5})


def test_multiple_raylets(ray_start_cluster):
    # This test will define a bunch of tasks that can only be assigned to
    # specific raylets, and we will check that they are assigned
    # to the correct raylets.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=11, num_gpus=0)
    cluster.add_node(num_cpus=5, num_gpus=5)
    cluster.add_node(num_cpus=10, num_gpus=1)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    # Define a bunch of remote functions that all return the socket name of
    # the plasma store. Since there is a one-to-one correspondence between
    # plasma stores and raylets (at least right now), this can be
    # used to identify which raylet the task was assigned to.

    # This must be run on the zeroth raylet.
    @ray.remote(num_cpus=11)
    def run_on_0():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the first raylet.
    @ray.remote(num_gpus=2)
    def run_on_1():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the second raylet.
    @ray.remote(num_cpus=6, num_gpus=1)
    def run_on_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This can be run anywhere.
    @ray.remote(num_cpus=0, num_gpus=0)
    def run_on_0_1_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the first or second raylet.
    @ray.remote(num_gpus=1)
    def run_on_1_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

    # This must be run on the zeroth or second raylet.
    @ray.remote(num_cpus=8)
    def run_on_0_2():
        return ray.worker.global_worker.node.plasma_store_socket_name

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

    client_table = ray.nodes()
    store_names = []
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"].get("GPU", 0) == 0
    ]
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"].get("GPU", 0) == 5
    ]
    store_names += [
        client["ObjectStoreSocketName"] for client in client_table
        if client["Resources"].get("GPU", 0) == 1
    ]
    assert len(store_names) == 3

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


def test_custom_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3, resources={"CustomResource": 0})
    cluster.add_node(num_cpus=3, resources={"CustomResource": 1})
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource": 1})
    def g():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource": 1})
    def h():
        ray.get([f.remote() for _ in range(5)])
        return ray.worker.global_worker.node.unique_id

    # The f tasks should be scheduled on both raylets.
    assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2

    node_id = ray.worker.global_worker.node.unique_id

    # The g tasks should be scheduled only on the second raylet.
    raylet_ids = set(ray.get([g.remote() for _ in range(50)]))
    assert len(raylet_ids) == 1
    assert list(raylet_ids)[0] != node_id

    # Make sure that resource bookkeeping works when a task that uses a
    # custom resources gets blocked.
    ray.get([h.remote() for _ in range(5)])


def test_node_id_resource(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=3)
    cluster.add_node(num_cpus=3)
    ray.init(address=cluster.address)

    local_node = ray.state.current_node_id()

    # Note that these will have the same IP in the test cluster
    assert len(ray.state.node_ids()) == 2
    assert local_node in ray.state.node_ids()

    @ray.remote(resources={local_node: 1})
    def f():
        return ray.state.current_node_id()

    # Check the node id resource is automatically usable for scheduling.
    assert ray.get(f.remote()) == ray.state.current_node_id()


def test_two_custom_resources(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 1,
            "CustomResource2": 2
        })
    cluster.add_node(
        num_cpus=3, resources={
            "CustomResource1": 3,
            "CustomResource2": 4
        })
    ray.init(address=cluster.address)

    @ray.remote(resources={"CustomResource1": 1})
    def f():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource2": 1})
    def g():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource1": 1, "CustomResource2": 3})
    def h():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource1": 4})
    def j():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource3": 1})
    def k():
        time.sleep(0.001)
        return ray.worker.global_worker.node.unique_id

    # The f and g tasks should be scheduled on both raylets.
    assert len(set(ray.get([f.remote() for _ in range(50)]))) == 2
    assert len(set(ray.get([g.remote() for _ in range(50)]))) == 2

    node_id = ray.worker.global_worker.node.unique_id

    # The h tasks should be scheduled only on the second raylet.
    raylet_ids = set(ray.get([h.remote() for _ in range(50)]))
    assert len(raylet_ids) == 1
    assert list(raylet_ids)[0] != node_id

    # Make sure that tasks with unsatisfied custom resource requirements do
    # not get scheduled.
    ready_ids, remaining_ids = ray.wait([j.remote(), k.remote()], timeout=0.5)
    assert ready_ids == []


def test_many_custom_resources(shutdown_only):
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


# TODO: 5 retry attempts may be too little for Travis and we may need to
# increase it if this test begins to be flaky on Travis.
def test_zero_capacity_deletion_semantics(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1, resources={"test_resource": 1})

    def test():
        resources = ray.available_resources()
        MAX_RETRY_ATTEMPTS = 5
        retry_count = 0

        del resources["memory"]
        del resources["object_store_memory"]
        for key in list(resources.keys()):
            if key.startswith("node:"):
                del resources[key]

        while resources and retry_count < MAX_RETRY_ATTEMPTS:
            time.sleep(0.1)
            resources = ray.available_resources()
            retry_count += 1

        if retry_count >= MAX_RETRY_ATTEMPTS:
            raise RuntimeError(
                "Resources were available even after five retries.", resources)

        return resources

    function = ray.remote(
        num_cpus=2, num_gpus=1, resources={"test_resource": 1})(test)
    cluster_resources = ray.get(function.remote())

    # All cluster resources should be utilized and
    # cluster_resources must be empty
    assert cluster_resources == {}


@pytest.fixture
def save_gpu_ids_shutdown_only():
    # Record the curent value of this environment variable so that we can
    # reset it after the test.
    original_gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    yield None

    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Reset the environment variable.
    if original_gpu_ids is not None:
        os.environ["CUDA_VISIBLE_DEVICES"] = original_gpu_ids
    else:
        del os.environ["CUDA_VISIBLE_DEVICES"]


def test_specific_gpus(save_gpu_ids_shutdown_only):
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


def test_blocking_tasks(ray_start_regular):
    @ray.remote
    def f(i, j):
        return (i, j)

    @ray.remote
    def g(i):
        # Each instance of g submits and blocks on the result of another
        # remote task.
        object_ids = [f.remote(i, j) for j in range(2)]
        return ray.get(object_ids)

    @ray.remote
    def h(i):
        # Each instance of g submits and blocks on the result of another
        # remote task using ray.wait.
        object_ids = [f.remote(i, j) for j in range(2)]
        return ray.wait(object_ids, num_returns=len(object_ids))

    ray.get([h.remote(i) for i in range(4)])

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


def test_max_call_tasks(ray_start_regular):
    @ray.remote(max_calls=1)
    def f():
        return os.getpid()

    pid = ray.get(f.remote())
    ray.tests.utils.wait_for_pid_to_exit(pid)

    @ray.remote(max_calls=2)
    def f():
        return os.getpid()

    pid1 = ray.get(f.remote())
    pid2 = ray.get(f.remote())
    assert pid1 == pid2
    ray.tests.utils.wait_for_pid_to_exit(pid1)


def attempt_to_load_balance(remote_function,
                            args,
                            total_tasks,
                            num_nodes,
                            minimum_count,
                            num_attempts=100):
    attempts = 0
    while attempts < num_attempts:
        locations = ray.get(
            [remote_function.remote(*args) for _ in range(total_tasks)])
        names = set(locations)
        counts = [locations.count(name) for name in names]
        logger.info("Counts are {}.".format(counts))
        if (len(names) == num_nodes
                and all(count >= minimum_count for count in counts)):
            break
        attempts += 1
    assert attempts < num_attempts


def test_load_balancing(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets
    # in a roughly equal manner.
    cluster = ray_start_cluster
    num_nodes = 3
    num_cpus = 7
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=num_cpus)
    ray.init(address=cluster.address)

    @ray.remote
    def f():
        time.sleep(0.01)
        return ray.worker.global_worker.node.unique_id

    attempt_to_load_balance(f, [], 100, num_nodes, 10)
    attempt_to_load_balance(f, [], 1000, num_nodes, 100)


def test_load_balancing_with_dependencies(ray_start_cluster):
    # This test ensures that tasks are being assigned to all raylets in a
    # roughly equal manner even when the tasks have dependencies.
    cluster = ray_start_cluster
    num_nodes = 3
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        time.sleep(0.010)
        return ray.worker.global_worker.node.unique_id

    # This object will be local to one of the raylets. Make sure
    # this doesn't prevent tasks from being scheduled on other raylets.
    x = ray.put(np.zeros(1000000))

    attempt_to_load_balance(f, [x], 100, num_nodes, 25)


def wait_for_num_tasks(num_tasks, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.tasks()) >= num_tasks:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


def wait_for_num_objects(num_objects, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(ray.objects()) >= num_objects:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out while waiting for global state.")


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_global_state_api(shutdown_only):

    error_message = ("The ray global state API cannot be used "
                     "before ray.init has been called.")

    with pytest.raises(Exception, match=error_message):
        ray.objects()

    with pytest.raises(Exception, match=error_message):
        ray.tasks()

    with pytest.raises(Exception, match=error_message):
        ray.nodes()

    with pytest.raises(Exception, match=error_message):
        ray.jobs()

    ray.init(
        num_cpus=5,
        num_gpus=3,
        resources={"CustomResource": 1},
        include_webui=False)

    assert ray.cluster_resources()["CPU"] == 5
    assert ray.cluster_resources()["GPU"] == 3
    assert ray.cluster_resources()["CustomResource"] == 1

    assert ray.objects() == {}

    job_id = ray.utils.compute_job_id_from_driver(
        ray.WorkerID(ray.worker.global_worker.worker_id))
    driver_task_id = ray.worker.global_worker.current_task_id.hex()

    # One task is put in the task table which corresponds to this driver.
    wait_for_num_tasks(1)
    task_table = ray.tasks()
    assert len(task_table) == 1
    assert driver_task_id == list(task_table.keys())[0]
    task_spec = task_table[driver_task_id]["TaskSpec"]
    nil_unique_id_hex = ray.UniqueID.nil().hex()
    nil_actor_id_hex = ray.ActorID.nil().hex()

    assert task_spec["TaskID"] == driver_task_id
    assert task_spec["ActorID"] == nil_actor_id_hex
    assert task_spec["Args"] == []
    assert task_spec["JobID"] == job_id.hex()
    assert task_spec["FunctionID"] == nil_unique_id_hex
    assert task_spec["ReturnObjectIDs"] == []

    client_table = ray.nodes()
    node_ip_address = ray.worker.global_worker.node_ip_address

    assert len(client_table) == 1
    assert client_table[0]["NodeManagerAddress"] == node_ip_address

    @ray.remote
    def f(*xs):
        return 1

    x_id = ray.put(1)
    result_id = f.remote(1, "hi", x_id)

    # Wait for one additional task to complete.
    wait_for_num_tasks(1 + 1)
    task_table = ray.tasks()
    assert len(task_table) == 1 + 1
    task_id_set = set(task_table.keys())
    task_id_set.remove(driver_task_id)
    task_id = list(task_id_set)[0]

    task_spec = task_table[task_id]["TaskSpec"]
    assert task_spec["ActorID"] == nil_actor_id_hex
    assert task_spec["Args"] == [
        signature.DUMMY_TYPE, 1, signature.DUMMY_TYPE, "hi",
        signature.DUMMY_TYPE, x_id
    ]
    assert task_spec["JobID"] == job_id.hex()
    assert task_spec["ReturnObjectIDs"] == [result_id]

    assert task_table[task_id] == ray.tasks(task_id)

    # Wait for two objects, one for the x_id and one for result_id.
    wait_for_num_objects(2)

    def wait_for_object_table():
        timeout = 10
        start_time = time.time()
        while time.time() - start_time < timeout:
            object_table = ray.objects()
            tables_ready = (object_table[x_id]["ManagerIDs"] is not None and
                            object_table[result_id]["ManagerIDs"] is not None)
            if tables_ready:
                return
            time.sleep(0.1)
        raise RayTestTimeoutException(
            "Timed out while waiting for object table to "
            "update.")

    object_table = ray.objects()
    assert len(object_table) == 2

    assert object_table[x_id] == ray.objects(x_id)
    object_table_entry = ray.objects(result_id)
    assert object_table[result_id] == object_table_entry

    job_table = ray.jobs()
    print(job_table)

    assert len(job_table) == 1
    assert job_table[0]["JobID"] == job_id.hex()
    assert job_table[0]["NodeManagerAddress"] == node_ip_address


# TODO(rkn): Pytest actually has tools for capturing stdout and stderr, so we
# should use those, but they seem to conflict with Ray's use of faulthandler.
class CaptureOutputAndError(object):
    """Capture stdout and stderr of some span.

    This can be used as follows.

        captured = {}
        with CaptureOutputAndError(captured):
            # Do stuff.
        # Access captured["out"] and captured["err"].
    """

    def __init__(self, captured_output_and_error):
        if sys.version_info >= (3, 0):
            import io
            self.output_buffer = io.StringIO()
            self.error_buffer = io.StringIO()
        else:
            import cStringIO
            self.output_buffer = cStringIO.StringIO()
            self.error_buffer = cStringIO.StringIO()
        self.captured_output_and_error = captured_output_and_error

    def __enter__(self):
        sys.stdout.flush()
        sys.stderr.flush()
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.output_buffer
        sys.stderr = self.error_buffer

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        self.captured_output_and_error["out"] = self.output_buffer.getvalue()
        self.captured_output_and_error["err"] = self.error_buffer.getvalue()


def test_logging_to_driver(shutdown_only):
    ray.init(num_cpus=1, log_to_driver=True)

    @ray.remote
    def f():
        # It's important to make sure that these print statements occur even
        # without calling sys.stdout.flush() and sys.stderr.flush().
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    output_lines = captured["out"]
    for i in range(200):
        assert str(i) in output_lines

    # TODO(rkn): Check that no additional logs appear beyond what we expect
    # and that there are no duplicate logs. Once we address the issue
    # described in https://github.com/ray-project/ray/pull/5462, we should
    # also check that nothing is logged to stderr.


def test_not_logging_to_driver(shutdown_only):
    ray.init(num_cpus=1, log_to_driver=False)

    @ray.remote
    def f():
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

    captured = {}
    with CaptureOutputAndError(captured):
        ray.get(f.remote())
        time.sleep(1)

    output_lines = captured["out"]
    assert len(output_lines) == 0

    # TODO(rkn): Check that no additional logs appear beyond what we expect
    # and that there are no duplicate logs. Once we address the issue
    # described in https://github.com/ray-project/ray/pull/5462, we should
    # also check that nothing is logged to stderr.


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="New GCS API doesn't have a Python API yet.")
def test_workers(shutdown_only):
    num_workers = 3
    ray.init(num_cpus=num_workers)

    @ray.remote
    def f():
        return id(ray.worker.global_worker), os.getpid()

    # Wait until all of the workers have started.
    worker_ids = set()
    while len(worker_ids) != num_workers:
        worker_ids = set(ray.get([f.remote() for _ in range(10)]))


def test_specific_job_id():
    dummy_driver_id = ray.JobID.from_int(1)
    ray.init(num_cpus=1, job_id=dummy_driver_id, include_webui=False)

    # in driver
    assert dummy_driver_id == ray._get_runtime_context().current_driver_id

    # in worker
    @ray.remote
    def f():
        return ray._get_runtime_context().current_driver_id

    assert dummy_driver_id == ray.get(f.remote())

    ray.shutdown()


def test_object_id_properties():
    id_bytes = b"00112233445566778899"
    object_id = ray.ObjectID(id_bytes)
    assert object_id.binary() == id_bytes
    object_id = ray.ObjectID.nil()
    assert object_id.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length 20.*"):
        ray.ObjectID(b"0123456789")
    object_id = ray.ObjectID.from_random()
    assert not object_id.is_nil()
    assert object_id.binary() != id_bytes
    id_dumps = pickle.dumps(object_id)
    id_from_dumps = pickle.loads(id_dumps)
    assert id_from_dumps == object_id


@pytest.fixture
def shutdown_only_with_initialization_check():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    assert not ray.is_initialized()


def test_initialized(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0)
    assert ray.is_initialized()


def test_initialized_local_mode(shutdown_only_with_initialization_check):
    assert not ray.is_initialized()
    ray.init(num_cpus=0, local_mode=True)
    assert ray.is_initialized()


def test_wait_reconstruction(shutdown_only):
    ray.init(num_cpus=1, object_store_memory=int(10**8))

    @ray.remote
    def f():
        return np.zeros(6 * 10**7, dtype=np.uint8)

    x_id = f.remote()
    ray.wait([x_id])
    ray.wait([f.remote()])
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ready_ids, _ = ray.wait([x_id])
    assert len(ready_ids) == 1


def test_ray_setproctitle(ray_start_2_cpus):
    @ray.remote
    class UniqueName(object):
        def __init__(self):
            assert setproctitle.getproctitle() == "ray_UniqueName:__init__()"

        def f(self):
            assert setproctitle.getproctitle() == "ray_UniqueName:f()"

    @ray.remote
    def unique_1():
        assert setproctitle.getproctitle(
        ) == "ray_worker:ray.tests.test_basic.unique_1()"

    actor = UniqueName.remote()
    ray.get(actor.f.remote())
    ray.get(unique_1.remote())


def test_duplicate_error_messages(shutdown_only):
    ray.init(num_cpus=0)

    driver_id = ray.WorkerID.nil()
    error_data = ray.gcs_utils.construct_error_message(driver_id, "test",
                                                       "message", 0)

    # Push the same message to the GCS twice (they are the same because we
    # do not include a timestamp).

    r = ray.worker.global_worker.redis_client

    r.execute_command("RAY.TABLE_APPEND",
                      ray.gcs_utils.TablePrefix.Value("ERROR_INFO"),
                      ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB"),
                      driver_id.binary(), error_data)

    # Before https://github.com/ray-project/ray/pull/3316 this would
    # give an error
    r.execute_command("RAY.TABLE_APPEND",
                      ray.gcs_utils.TablePrefix.Value("ERROR_INFO"),
                      ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB"),
                      driver_id.binary(), error_data)


@pytest.mark.skipif(
    os.getenv("TRAVIS") is None,
    reason="This test should only be run on Travis.")
def test_ray_stack(ray_start_2_cpus):
    def unique_name_1():
        time.sleep(1000)

    @ray.remote
    def unique_name_2():
        time.sleep(1000)

    @ray.remote
    def unique_name_3():
        unique_name_1()

    unique_name_2.remote()
    unique_name_3.remote()

    success = False
    start_time = time.time()
    while time.time() - start_time < 30:
        # Attempt to parse the "ray stack" call.
        output = ray.utils.decode(subprocess.check_output(["ray", "stack"]))
        if ("unique_name_1" in output and "unique_name_2" in output
                and "unique_name_3" in output):
            success = True
            break

    if not success:
        raise Exception("Failed to find necessary information with "
                        "'ray stack'")


def test_pandas_parquet_serialization():
    # Only test this if pandas is installed
    pytest.importorskip("pandas")

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    tempdir = tempfile.mkdtemp()
    filename = os.path.join(tempdir, "parquet-test")
    pd.DataFrame({"col1": [0, 1], "col2": [0, 1]}).to_parquet(filename)
    with open(os.path.join(tempdir, "parquet-compression"), "wb") as f:
        table = pa.Table.from_arrays([pa.array([1, 2, 3])], ["hello"])
        pq.write_table(table, f, compression="lz4")
    # Clean up
    shutil.rmtree(tempdir)


def test_socket_dir_not_existing(shutdown_only):
    random_name = ray.ObjectID.from_random().hex()
    temp_raylet_socket_dir = "/tmp/ray/tests/{}".format(random_name)
    temp_raylet_socket_name = os.path.join(temp_raylet_socket_dir,
                                           "raylet_socket")
    ray.init(num_cpus=1, raylet_socket_name=temp_raylet_socket_name)


def test_raylet_is_robust_to_random_messages(ray_start_regular):
    node_manager_address = None
    node_manager_port = None
    for client in ray.nodes():
        if "NodeManagerAddress" in client:
            node_manager_address = client["NodeManagerAddress"]
            node_manager_port = client["NodeManagerPort"]
    assert node_manager_address
    assert node_manager_port
    # Try to bring down the node manager:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((node_manager_address, node_manager_port))
    s.send(1000 * b"asdf")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


def test_non_ascii_comment(ray_start_regular):
    @ray.remote
    def f():
        # 日本語 Japanese comment
        return 1

    assert ray.get(f.remote()) == 1


@ray.remote
def echo(x):
    return x


@ray.remote
class WithConstructor(object):
    def __init__(self, data):
        self.data = data

    def get_data(self):
        return self.data


@ray.remote
class WithoutConstructor(object):
    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data


class BaseClass(object):
    def __init__(self, data):
        self.data = data

    def get_data(self):
        return self.data


@ray.remote
class DerivedClass(BaseClass):
    def __init__(self, data):
        # Due to different behaviors of super in Python 2 and Python 3,
        # we use BaseClass directly here.
        BaseClass.__init__(self, data)


def test_load_code_from_local(shutdown_only):
    ray.init(load_code_from_local=True, num_cpus=4)
    message = "foo"
    # Test normal function.
    assert ray.get(echo.remote(message)) == message
    # Test actor class with constructor.
    actor = WithConstructor.remote(1)
    assert ray.get(actor.get_data.remote()) == 1
    # Test actor class without constructor.
    actor = WithoutConstructor.remote()
    actor.set_data.remote(1)
    assert ray.get(actor.get_data.remote()) == 1
    # Test derived actor class.
    actor = DerivedClass.remote(1)
    assert ray.get(actor.get_data.remote()) == 1
    # Test using ray.remote decorator on raw classes.
    base_actor_class = ray.remote(num_cpus=1)(BaseClass)
    base_actor = base_actor_class.remote(message)
    assert ray.get(base_actor.get_data.remote()) == message


def test_shutdown_disconnect_global_state():
    ray.init(num_cpus=0)
    ray.shutdown()

    with pytest.raises(Exception) as e:
        ray.objects()
    assert str(e.value).endswith("ray.init has been called.")


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True)
def test_put_pins_object(ray_start_object_store_memory):
    x_id = ray.put("HI")
    x_copy = ray.ObjectID(x_id.binary())
    assert ray.get(x_copy) == "HI"

    # x cannot be evicted since x_id pins it
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert ray.get(x_id) == "HI"
    assert ray.get(x_copy) == "HI"

    # now it can be evicted since x_id pins it but x_copy does not
    del x_id
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(x_copy)

    # weakref put
    y_id = ray.put("HI", weakref=True)
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    with pytest.raises(ray.exceptions.UnreconstructableError):
        ray.get(y_id)

    @ray.remote
    def check_no_buffer_ref(x):
        assert x[0].get_buffer_ref() is None

    z_id = ray.put("HI")
    assert z_id.get_buffer_ref() is not None
    ray.get(check_no_buffer_ref.remote([z_id]))


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True)
def test_redis_lru_with_set(ray_start_object_store_memory):
    x = np.zeros(8 * 10**7, dtype=np.uint8)
    x_id = ray.put(x, weakref=True)

    # Remove the object from the object table to simulate Redis LRU eviction.
    removed = False
    start_time = time.time()
    while time.time() < start_time + 10:
        if ray.state.state.redis_clients[0].delete(b"OBJECT" +
                                                   x_id.binary()) == 1:
            removed = True
            break
    assert removed

    # Now evict the object from the object store.
    ray.put(x)  # This should not crash.


def test_decorated_function(ray_start_regular):
    def function_invocation_decorator(f):
        def new_f(args, kwargs):
            # Reverse the arguments.
            return f(args[::-1], {"d": 5}), kwargs

        return new_f

    def f(a, b, c, d=None):
        return a, b, c, d

    f.__ray_invocation_decorator__ = function_invocation_decorator
    f = ray.remote(f)

    result_id, kwargs = f.remote(1, 2, 3, d=4)
    assert kwargs == {"d": 4}
    assert ray.get(result_id) == (3, 2, 1, 5)


def test_get_postprocess(ray_start_regular):
    def get_postprocessor(object_ids, values):
        return [value for value in values if value > 0]

    ray.worker.global_worker._post_get_hooks.append(get_postprocessor)

    assert ray.get(
        [ray.put(i) for i in [0, 1, 3, 5, -1, -3, 4]]) == [1, 3, 5, 4]


def test_export_after_shutdown(ray_start_regular):
    # This test checks that we can use actor and remote function definitions
    # across multiple Ray sessions.

    @ray.remote
    def f():
        pass

    @ray.remote
    class Actor(object):
        def method(self):
            pass

    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray and use the remote function and actor again.
    ray.init(num_cpus=1)
    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray again and make sure that these definitions can be exported from
    # workers.
    ray.init(num_cpus=2)

    @ray.remote
    def export_definitions_from_worker(remote_function, actor_class):
        ray.get(remote_function.remote())
        actor_handle = actor_class.remote()
        ray.get(actor_handle.method.remote())

    ray.get(export_definitions_from_worker.remote(f, Actor))


def test_invalid_unicode_in_worker_log(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    # Wait till first worker log file is created.
    while True:
        log_file_paths = glob.glob("{}/worker*.out".format(logs_dir))
        if len(log_file_paths) == 0:
            time.sleep(0.2)
        else:
            break

    with open(log_file_paths[0], "wb") as f:
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.flush()

    # Wait till the log monitor reads the file.
    time.sleep(1.0)

    # Make sure that nothing has died.
    assert ray.services.remaining_processes_alive()


@pytest.mark.skip(reason="This test is too expensive to run.")
def test_move_log_files_to_old(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    @ray.remote
    class Actor(object):
        def f(self):
            print("function f finished")

    # First create a temporary actor.
    actors = [
        Actor.remote() for i in range(ray_constants.LOG_MONITOR_MAX_OPEN_FILES)
    ]
    ray.get([a.f.remote() for a in actors])

    # Make sure no log files are in the "old" directory before the actors
    # are killed.
    assert len(glob.glob("{}/old/worker*.out".format(logs_dir))) == 0

    # Now kill the actors so the files get moved to logs/old/.
    [a.__ray_terminate__.remote() for a in actors]

    while True:
        log_file_paths = glob.glob("{}/old/worker*.out".format(logs_dir))
        if len(log_file_paths) > 0:
            with open(log_file_paths[0], "r") as f:
                assert "function f finished\n" in f.readlines()
            break

    # Make sure that nothing has died.
    assert ray.services.remaining_processes_alive()
