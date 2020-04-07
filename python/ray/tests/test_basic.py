# coding: utf-8
import collections
import io
import json
import logging
import os
import pickle
import re
import string
import sys
import threading
import time
import weakref

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.test_utils
from ray.exceptions import RayTimeoutError

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


# https://github.com/ray-project/ray/issues/6662
def test_ignore_http_proxy(shutdown_only):
    ray.init(num_cpus=1)
    os.environ["http_proxy"] = "http://example.com"
    os.environ["https_proxy"] = "http://example.com"

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


# https://github.com/ray-project/ray/issues/7263
def test_grpc_message_size(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def bar(*a):
        return

    # 50KiB, not enough to spill to plasma, but will be inlined.
    def f():
        return np.zeros(50000, dtype=np.uint8)

    # Executes a 10MiB task spec
    ray.get(bar.remote(*[f() for _ in range(200)]))


# https://github.com/ray-project/ray/issues/7287
def test_omp_threads_set(shutdown_only):
    ray.init(num_cpus=1)
    # Should have been auto set by ray init.
    assert os.environ["OMP_NUM_THREADS"] == "1"


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
    class Actor:
        def __init__(self, x, y=0):
            self.x = x
            self.y = y

        def method(self, a, b=0):
            return self.x, self.y, a, b

        def gpu_ids(self):
            return ray.get_gpu_ids()

    @ray.remote
    class Actor2:
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


def test_background_tasks_with_max_calls(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def g():
        time.sleep(.1)
        return 0

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return [g.remote()]

    nested = ray.get([f.remote() for _ in range(10)])

    # Should still be able to retrieve these objects, since f's workers will
    # wait for g to finish before exiting.
    ray.get([x[0] for x in nested])

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return os.getpid(), g.remote()

    nested = ray.get([f.remote() for _ in range(10)])
    while nested:
        pid, g_id = nested.pop(0)
        ray.get(g_id)
        del g_id
        ray.test_utils.wait_for_pid_to_exit(pid)


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


def test_numpy_subclass_serialization(ray_start_regular):
    class MyNumpyConstant(np.ndarray):
        def __init__(self, value):
            super().__init__()
            self.constant = value

        def __str__(self):
            print(self.constant)

    constant = MyNumpyConstant(123)

    def explode(x):
        raise RuntimeError("Expected error.")

    ray.register_custom_serializer(
        type(constant), serializer=explode, deserializer=explode)

    try:
        ray.put(constant)
        assert False, "Should never get here!"
    except (RuntimeError, IndexError):
        print("Correct behavior, proof that customer serializer was used.")


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


def test_function_descriptor():
    python_descriptor = ray._raylet.PythonFunctionDescriptor(
        "module_name", "function_name", "class_name", "function_hash")
    python_descriptor2 = pickle.loads(pickle.dumps(python_descriptor))
    assert python_descriptor == python_descriptor2
    assert hash(python_descriptor) == hash(python_descriptor2)
    assert python_descriptor.function_id == python_descriptor2.function_id
    java_descriptor = ray._raylet.JavaFunctionDescriptor(
        "class_name", "function_name", "signature")
    java_descriptor2 = pickle.loads(pickle.dumps(java_descriptor))
    assert java_descriptor == java_descriptor2
    assert python_descriptor != java_descriptor
    assert python_descriptor != object()
    d = {python_descriptor: 123}
    assert d.get(python_descriptor2) == 123


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
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


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_ray_recursive_objects(ray_start_regular):
    class ClassA:
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

    # Serialize the recursive objects.
    for obj in recursive_objects:
        ray.put(obj)


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_reducer_override_no_reference_cycle(ray_start_regular):
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


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_deserialized_from_buffer_immutable(ray_start_regular):
    x = np.full((2, 2), 1.)
    o = ray.put(x)
    y = ray.get(o)
    with pytest.raises(
            ValueError, match="assignment destination is read-only"):
        y[0, 0] = 9.


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
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

    class Foo:
        def __init__(self):
            pass

    # Make sure that we can put and get a custom type. Note that the result
    # won't be "equal" to Foo.
    ray.get(ray.put(Foo))


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_putting_object_that_closes_over_object_id(ray_start_regular):
    # This test is here to prevent a regression of
    # https://github.com/ray-project/ray/issues/1317.

    class Foo:
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


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_custom_serializers(ray_start_regular):
    class Foo:
        def __init__(self):
            self.x = 3

    def custom_serializer(obj):
        return 3, "string1", type(obj).__name__

    def custom_deserializer(serialized_obj):
        return serialized_obj, "string2"

    ray.register_custom_serializer(
        Foo, serializer=custom_serializer, deserializer=custom_deserializer)

    assert ray.get(ray.put(Foo())) == ((3, "string1", Foo.__name__), "string2")

    class Bar:
        def __init__(self):
            self.x = 3

    ray.register_custom_serializer(
        Bar, serializer=custom_serializer, deserializer=custom_deserializer)

    @ray.remote
    def f():
        return Bar()

    assert ray.get(f.remote()) == ((3, "string1", Bar.__name__), "string2")


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
    @ray.remote(num_return_vals=3)
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


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
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

    class TestActor:
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

    class TestActor:
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

    class TestActor:
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


@pytest.mark.parametrize(
    "shutdown_only", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
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


@pytest.mark.parametrize(
    "shutdown_only", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_defining_remote_functions(shutdown_only):
    ray.init(num_cpus=3)

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


@pytest.mark.parametrize(
    "shutdown_only", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_redefining_remote_functions(shutdown_only):
    ray.init(num_cpus=1)

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

    # Check that we can redefine functions even when the remote function source
    # doesn't change (see https://github.com/ray-project/ray/issues/6130).
    @ray.remote
    def g():
        return nonexistent()

    with pytest.raises(ray.exceptions.RayTaskError, match="nonexistent"):
        ray.get(g.remote())

    def nonexistent():
        return 1

    # Redefine the function and make sure it succeeds.
    @ray.remote
    def g():
        return nonexistent()

    assert ray.get(g.remote()) == 1

    # Check the same thing but when the redefined function is inside of another
    # task.
    @ray.remote
    def h(i):
        @ray.remote
        def j():
            return i

        return j.remote()

    for i in range(20):
        assert ray.get(ray.get(h.remote(i))) == i


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_get_multiple(ray_start_regular):
    object_ids = [ray.put(i) for i in range(10)]
    assert ray.get(object_ids) == list(range(10))

    # Get a random choice of object IDs with duplicates.
    indices = list(np.random.choice(range(10), 5))
    indices += indices
    results = ray.get([object_ids[i] for i in indices])
    assert results == indices


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_get_multiple_experimental(ray_start_regular):
    object_ids = [ray.put(i) for i in range(10)]

    object_ids_tuple = tuple(object_ids)
    assert ray.experimental.get(object_ids_tuple) == list(range(10))

    object_ids_nparray = np.array(object_ids)
    assert ray.experimental.get(object_ids_nparray) == list(range(10))


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_get_dict(ray_start_regular):
    d = {str(i): ray.put(i) for i in range(5)}
    for i in range(5, 10):
        d[str(i)] = i
    result = ray.experimental.get(d)
    expected = {str(i): i for i in range(10)}
    assert result == expected


def test_get_with_timeout(ray_start_regular):
    signal = ray.test_utils.SignalActor.remote()

    # Check that get() returns early if object is ready.
    start = time.time()
    ray.get(signal.wait.remote(should_wait=False), timeout=30)
    assert time.time() - start < 30

    # Check that get() raises a TimeoutError after the timeout if the object
    # is not ready yet.
    result_id = signal.wait.remote()
    with pytest.raises(RayTimeoutError):
        ray.get(result_id, timeout=0.1)

    # Check that a subsequent get() returns early.
    ray.get(signal.send.remote())
    start = time.time()
    ray.get(result_id, timeout=30)
    assert time.time() - start < 30


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
# https://github.com/ray-project/ray/issues/6329
def test_call_actors_indirect_through_tasks(ray_start_regular):
    @ray.remote
    class Counter:
        def __init__(self, value):
            self.value = int(value)

        def increase(self, delta):
            self.value += int(delta)
            return self.value

    @ray.remote
    def foo(object):
        return ray.get(object.increase.remote(1))

    @ray.remote
    def bar(object):
        return ray.get(object.increase.remote(1))

    @ray.remote
    def zoo(object):
        return ray.get(object[0].increase.remote(1))

    c = Counter.remote(0)
    for _ in range(0, 100):
        ray.get(foo.remote(c))
        ray.get(bar.remote(c))
        ray.get(zoo.remote([c]))


def test_call_matrix(shutdown_only):
    ray.init(object_store_memory=1000 * 1024 * 1024)

    @ray.remote
    class Actor:
        def small_value(self):
            return 0

        def large_value(self):
            return np.zeros(10 * 1024 * 1024)

        def echo(self, x):
            if isinstance(x, list):
                x = ray.get(x[0])
            return x

    @ray.remote
    def small_value():
        return 0

    @ray.remote
    def large_value():
        return np.zeros(10 * 1024 * 1024)

    @ray.remote
    def echo(x):
        if isinstance(x, list):
            x = ray.get(x[0])
        return x

    def check(source_actor, dest_actor, is_large, out_of_band):
        print("CHECKING", "actor" if source_actor else "task", "to", "actor"
              if dest_actor else "task", "large_object"
              if is_large else "small_object", "out_of_band"
              if out_of_band else "in_band")
        if source_actor:
            a = Actor.remote()
            if is_large:
                x_id = a.large_value.remote()
            else:
                x_id = a.small_value.remote()
        else:
            if is_large:
                x_id = large_value.remote()
            else:
                x_id = small_value.remote()
        if out_of_band:
            x_id = [x_id]
        if dest_actor:
            b = Actor.remote()
            x = ray.get(b.echo.remote(x_id))
        else:
            x = ray.get(echo.remote(x_id))
        if is_large:
            assert isinstance(x, np.ndarray)
        else:
            assert isinstance(x, int)

    for is_large in [False, True]:
        for source_actor in [False, True]:
            for dest_actor in [False, True]:
                for out_of_band in [False, True]:
                    check(source_actor, dest_actor, is_large, out_of_band)


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 1,
        "num_nodes": 1,
    }, {
        "num_cpus": 1,
        "num_nodes": 2,
    }],
    indirect=True)
def test_call_chain(ray_start_cluster):
    @ray.remote
    def g(x):
        return x + 1

    x = 0
    for _ in range(100):
        x = g.remote(x)
    assert ray.get(x) == 100


def test_inline_arg_memory_corruption(ray_start_regular):
    @ray.remote
    def f():
        return np.zeros(1000, dtype=np.uint8)

    @ray.remote
    class Actor:
        def __init__(self):
            self.z = []

        def add(self, x):
            self.z.append(x)
            for prev in self.z:
                assert np.sum(prev) == 0, ("memory corruption detected", prev)

    a = Actor.remote()
    for i in range(100):
        ray.get(a.add.remote(f.remote()))


def test_skip_plasma(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self, x):
            return x * 2

    a = Actor.remote()
    obj_id = a.f.remote(1)
    # it is not stored in plasma
    assert not ray.worker.global_worker.core_worker.object_exists(obj_id)
    assert ray.get(obj_id) == 2


def test_actor_call_order(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    def small_value():
        time.sleep(0.01 * np.random.randint(0, 10))
        return 0

    @ray.remote
    class Actor:
        def __init__(self):
            self.count = 0

        def inc(self, count, dependency):
            assert count == self.count
            self.count += 1
            return count

    a = Actor.remote()
    assert ray.get([a.inc.remote(i, small_value.remote())
                    for i in range(100)]) == list(range(100))


def test_actor_large_objects(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self):
            time.sleep(1)
            return np.zeros(10000000)

    a = Actor.remote()
    obj_id = a.f.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(obj_id)
    done, _ = ray.wait([obj_id])
    assert len(done) == 1
    assert ray.worker.global_worker.core_worker.object_exists(obj_id)
    assert isinstance(ray.get(obj_id), np.ndarray)


def test_actor_pass_by_ref(ray_start_regular):
    @ray.remote
    class Actor:
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

    a = Actor.remote()
    assert ray.get(a.f.remote(f.remote(1))) == 2

    fut = [a.f.remote(f.remote(i)) for i in range(100)]
    assert ray.get(fut) == [i * 2 for i in range(100)]

    # propagates errors for pass by ref
    with pytest.raises(Exception):
        ray.get(a.f.remote(error.remote()))


def test_actor_pass_by_ref_order_optimization(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self, x):
            pass

    a = Actor.remote()

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


def test_actor_recursive(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self, delegate=None):
            self.delegate = delegate

        def f(self, x):
            if self.delegate:
                return ray.get(self.delegate.f.remote(x))
            return x * 2

    a = Actor.remote()
    b = Actor.remote(a)
    c = Actor.remote(b)

    result = ray.get([c.f.remote(i) for i in range(100)])
    assert result == [x * 2 for x in range(100)]

    result, _ = ray.wait([c.f.remote(i) for i in range(100)], num_returns=100)
    result = ray.get(result)
    assert result == [x * 2 for x in range(100)]


def test_actor_concurrent(ray_start_regular):
    @ray.remote
    class Batcher:
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

    a = Batcher.options(max_concurrency=3).remote()
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
        return

    object_ids = [f.remote(0), f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_ids)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(object_ids, num_returns=4)
    assert set(ready_ids) == set(object_ids)
    assert remaining_ids == []

    object_ids = [f.remote(0), f.remote(5)]
    ready_ids, remaining_ids = ray.wait(object_ids, timeout=0.5, num_returns=2)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 1

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


def test_duplicate_args(ray_start_regular):
    @ray.remote
    def f(arg1,
          arg2,
          arg1_duplicate,
          kwarg1=None,
          kwarg2=None,
          kwarg1_duplicate=None):
        assert arg1 == kwarg1
        assert arg1 != arg2
        assert arg1 == arg1_duplicate
        assert kwarg1 != kwarg2
        assert kwarg1 == kwarg1_duplicate

    # Test by-value arguments.
    arg1 = [1]
    arg2 = [2]
    ray.get(
        f.remote(
            arg1, arg2, arg1, kwarg1=arg1, kwarg2=arg2, kwarg1_duplicate=arg1))

    # Test by-reference arguments.
    arg1 = ray.put([1])
    arg2 = ray.put([2])
    ray.get(
        f.remote(
            arg1, arg2, arg1, kwarg1=arg1, kwarg2=arg2, kwarg1_duplicate=arg1))


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
