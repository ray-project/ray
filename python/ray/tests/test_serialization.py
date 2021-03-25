# coding: utf-8
import collections
import io
import logging
import re
import string
import sys
import weakref

import numpy as np
from numpy import log
import pytest

import ray
import ray.cluster_utils
import ray.test_utils

logger = logging.getLogger(__name__)


def is_named_tuple(cls):
    """Return True if cls is a namedtuple and False otherwise."""
    b = cls.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(cls, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_simple_serialization(ray_start_regular):
    primitive_objects = [
        # Various primitive types.
        0,
        0.0,
        0.9,
        1 << 62,
        1 << 999,
        b"",
        b"a",
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


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_complex_serialization(ray_start_regular):
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
        elif (is_named_tuple(type(obj1)) or is_named_tuple(type(obj2))):
            assert len(obj1) == len(obj2), (
                "Objects {} and {} are named "
                "tuples with different lengths.".format(obj1, obj2))
            for i in range(len(obj1)):
                assert_equal(obj1[i], obj2[i])
        else:
            assert obj1 == obj2, "Objects {} and {} are different.".format(
                obj1, obj2)

    long_extras = [0, np.array([["hi", u"hi"], [1.3, 1]])]

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

    class Foo:
        def __init__(self, value=0):
            self.value = value

        def __hash__(self):
            return hash(self.value)

        def __eq__(self, other):
            return other.value == self.value

    class Bar:
        def __init__(self):
            for i, val in enumerate(PRIMITIVE_OBJECTS + COMPLEX_OBJECTS):
                setattr(self, "field{}".format(i), val)

    class Baz:
        def __init__(self):
            self.foo = Foo()
            self.bar = Bar()

        def method(self, arg):
            pass

    class Qux:
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

        class CustomClass:
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


def test_numpy_serialization(ray_start_regular):
    array = np.zeros(314)
    from ray.cloudpickle import dumps
    buffers = []
    inband = dumps(array, protocol=5, buffer_callback=buffers.append)
    assert len(inband) < array.nbytes
    assert len(buffers) == 1


def test_numpy_subclass_serialization_pickle(ray_start_regular):
    class MyNumpyConstant(np.ndarray):
        def __init__(self, value):
            super().__init__()
            self.constant = value

        def __str__(self):
            print(self.constant)

    constant = MyNumpyConstant(123)
    repr_orig = repr(constant)
    repr_ser = repr(ray.get(ray.put(constant)))
    assert repr_orig == repr_ser


def test_inspect_serialization(enable_pickle_debug):
    import threading
    from ray.cloudpickle import dumps_debug

    lock = threading.Lock()

    with pytest.raises(TypeError):
        dumps_debug(lock)

    def test_func():
        print(lock)

    with pytest.raises(TypeError):
        dumps_debug(test_func)

    class test_class:
        def test(self):
            self.lock = lock

    from ray.util.check_serialize import inspect_serializability
    results = inspect_serializability(lock)
    assert list(results[1])[0].obj == lock, results

    results = inspect_serializability(test_func)
    assert list(results[1])[0].obj == lock, results

    results = inspect_serializability(test_class)
    assert list(results[1])[0].obj == lock, results


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
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
    class TempClass:
        pass

    ray.get(ray.put(TempClass()))

    # Test passing custom classes into remote functions from the driver.
    @ray.remote
    def f(x):
        return x

    class Foo:
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

    class TempClass1:
        def __init__(self):
            self.value = 1

    # Test returning custom classes created on workers.
    @ray.remote
    def g():
        class TempClass2:
            def __init__(self):
                self.value = 2

        return TempClass1(), TempClass2()

    object_1, object_2 = ray.get(g.remote())
    assert object_1.value == 1
    assert object_2.value == 2

    # Test exporting custom class definitions from one worker to another
    # when the worker is blocked in a get.
    class NewTempClass:
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
    @ray.remote(num_returns=3)
    def j():
        class Class0:
            def method0(self):
                pass

        c0 = Class0()

        class Class0:
            def method1(self):
                pass

        c1 = Class0()

        class Class0:
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
        class Class0:
            def method0(self):
                pass

        c0 = Class0()

        class Class0:
            def method1(self):
                pass

        c1 = Class0()

        class Class0:
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


def test_deserialized_from_buffer_immutable(ray_start_shared_local_modes):
    x = np.full((2, 2), 1.)
    o = ray.put(x)
    y = ray.get(o)
    with pytest.raises(
            ValueError, match="assignment destination is read-only"):
        y[0, 0] = 9.


def test_reducer_override_no_reference_cycle(ray_start_shared_local_modes):
    # bpo-39492: reducer_override used to induce a spurious reference cycle
    # inside the Pickler object, that could prevent all serialized objects
    # from being garbage-collected without explicity invoking gc.collect.

    # test a dynamic function
    def f():
        return 4669201609102990671853203821578

    wr = weakref.ref(f)

    bio = io.BytesIO()
    from ray.cloudpickle import CloudPickler, loads, dumps
    p = CloudPickler(bio, protocol=5)
    p.dump(f)
    new_f = loads(bio.getvalue())
    assert new_f() == 4669201609102990671853203821578

    del p
    del f

    assert wr() is None

    # test a dynamic class
    class ShortlivedObject:
        def __del__(self):
            print("Went out of scope!")

    obj = ShortlivedObject()
    new_obj = weakref.ref(obj)

    dumps(obj)
    del obj
    assert new_obj() is None


def test_buffer_alignment(ray_start_shared_local_modes):
    # Deserialized large numpy arrays should be 64-byte aligned.
    x = np.random.normal(size=(10, 20, 30))
    y = ray.get(ray.put(x))
    assert y.ctypes.data % 64 == 0

    # Unlike PyArrow, Ray aligns small numpy arrays to 8
    # bytes to be memory efficient.
    xs = [np.random.normal(size=i) for i in range(100)]
    ys = ray.get(ray.put(xs))
    for y in ys:
        assert y.ctypes.data % 8 == 0

    xs = [np.random.normal(size=i * (1, )) for i in range(20)]
    ys = ray.get(ray.put(xs))
    for y in ys:
        assert y.ctypes.data % 8 == 0

    xs = [np.random.normal(size=i * (5, )) for i in range(1, 8)]
    xs = [xs[i][(i + 1) * (slice(1, 3), )] for i in range(len(xs))]
    ys = ray.get(ray.put(xs))
    for y in ys:
        assert y.ctypes.data % 8 == 0


def test_custom_serializer(ray_start_shared_local_modes):
    import threading

    class A:
        def __init__(self, x):
            self.x = x
            self.lock = threading.Lock()

    def custom_serializer(a):
        return a.x

    def custom_deserializer(x):
        return A(x)

    ray.util.register_serializer(
        A, serializer=custom_serializer, deserializer=custom_deserializer)
    ray.get(ray.put(A(1)))

    ray.util.deregister_serializer(A)
    with pytest.raises(Exception):
        ray.get(ray.put(A(1)))

    # deregister again takes no effects
    ray.util.deregister_serializer(A)


def test_numpy_ufunc(ray_start_shared_local_modes):
    @ray.remote
    def f():
        # add reference to the numpy ufunc
        log

    ray.get(f.remote())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
