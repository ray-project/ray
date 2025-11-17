import collections
import io
import os
import string
import sys
from dataclasses import make_dataclass

import numpy as np
import pytest
import torch

import ray
from ray._common.test_utils import is_named_tuple

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@pytest.fixture(scope="session")
def ray_start_cluster_with_zero_copy_tensors():
    """Start a Ray cluster with zero-copy PyTorch tensors enabled."""
    # Save the original environment variable value (if any) for restoration later
    original_value = os.environ.get("RAY_ENABLE_ZERO_COPY_TORCH_TENSORS")

    # Enable zero-copy sharing of PyTorch tensors in Ray
    os.environ["RAY_ENABLE_ZERO_COPY_TORCH_TENSORS"] = "1"

    # Initialize Ray with the required environment variable.
    ray.init(runtime_env={"env_vars": {"RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "1"}})

    # Yield control to the test session
    yield

    # Shutdown Ray after tests complete
    ray.shutdown()

    # Restore the original environment variable state
    if original_value is None:
        os.environ.pop("RAY_ENABLE_ZERO_COPY_TORCH_TENSORS", None)
    else:
        os.environ["RAY_ENABLE_ZERO_COPY_TORCH_TENSORS"] = original_value


def test_simple_non_tensor_serialization(ray_start_cluster_with_zero_copy_tensors):
    os.environ["RAY_ENABLE_ZERO_COPY_TORCH_TENSORS"] = "1"
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
        "hello world",
        "\xff\xfe\x9c\x001\x000\x00",
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
        [[obj] for obj in primitive_objects]
        + [(obj,) for obj in primitive_objects]
        + [{(): obj} for obj in primitive_objects]
    )

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
        if type(obj).__module__ != "numpy":
            assert type(obj) is type(new_obj_1)
            assert type(obj) is type(new_obj_2)


def test_complex_non_tensor_serialization(ray_start_cluster_with_zero_copy_tensors):
    def assert_equal(obj1, obj2):
        module_numpy = (
            type(obj1).__module__ == np.__name__ or type(obj2).__module__ == np.__name__
        )
        if module_numpy:
            empty_shape = (hasattr(obj1, "shape") and obj1.shape == ()) or (
                hasattr(obj2, "shape") and obj2.shape == ()
            )
            if empty_shape:
                # This is a special case because currently
                # np.testing.assert_equal fails because we do not properly
                # handle different numerical types.
                assert obj1 == obj2, "Objects {} and {} are different.".format(
                    obj1, obj2
                )
            else:
                np.testing.assert_equal(obj1, obj2)
        elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
            special_keys = ["_pytype_"]
            assert set(list(obj1.__dict__.keys()) + special_keys) == set(
                list(obj2.__dict__.keys()) + special_keys
            ), "Objects {} and {} are different.".format(obj1, obj2)
            for key in obj1.__dict__.keys():
                if key not in special_keys:
                    assert_equal(obj1.__dict__[key], obj2.__dict__[key])
        elif type(obj1) is dict or type(obj2) is dict:
            assert_equal(obj1.keys(), obj2.keys())
            for key in obj1.keys():
                assert_equal(obj1[key], obj2[key])
        elif type(obj1) is list or type(obj2) is list:
            assert len(obj1) == len(
                obj2
            ), "Objects {} and {} are lists with different lengths.".format(obj1, obj2)
            for i in range(len(obj1)):
                assert_equal(obj1[i], obj2[i])
        elif type(obj1) is tuple or type(obj2) is tuple:
            assert len(obj1) == len(
                obj2
            ), "Objects {} and {} are tuples with different lengths.".format(obj1, obj2)
            for i in range(len(obj1)):
                assert_equal(obj1[i], obj2[i])
        elif is_named_tuple(type(obj1)) or is_named_tuple(type(obj2)):
            assert len(obj1) == len(
                obj2
            ), "Objects {} and {} are named tuples with different lengths.".format(
                obj1, obj2
            )
            for i in range(len(obj1)):
                assert_equal(obj1[i], obj2[i])
        else:
            assert obj1 == obj2, "Objects {} and {} are different.".format(obj1, obj2)

    long_extras = [0, np.array([["hi", "hi"], [1.3, 1]])]

    PRIMITIVE_OBJECTS = [
        0,
        0.0,
        0.9,
        1 << 62,
        1 << 100,
        1 << 999,
        [1 << 100, [1 << 100]],
        "a",
        string.printable,
        "\u262F",
        "hello world",
        "\xff\xfe\x9c\x001\x000\x00",
        None,
        True,
        False,
        [],
        (),
        {},
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
        np.array(["hi", 3], dtype=object),
    ] + long_extras

    COMPLEX_OBJECTS = [
        [[[[[[[[[[[[]]]]]]]]]]]],
        {"obj{}".format(i): np.random.normal(size=[100, 100]) for i in range(10)},
        {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {}}}}}}}}}}}}},
        ((((((((((),),),),),),),),),),
        {"a": {"b": {"c": {"d": {}}}}},
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
        "Example", "field1, field2, field3, field4, field5"
    )

    CUSTOM_OBJECTS = [
        Exception("Test object."),
        CustomError(),
        Point(11, y=22),
        Foo(),
        Bar(),
        Baz(),  # Qux(), SubQux(),
        NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3]),
    ]

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
            return (self.from_custom, (self.custom.value,))

    CUSTOM_OBJECTS.append(DataClass2(custom=CustomClass(43)))

    BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

    LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
    TUPLE_OBJECTS = [(obj,) for obj in BASE_OBJECTS]
    # The check that type(obj).__module__ != "numpy" should be unnecessary, but
    # otherwise this seems to fail on Mac OS X on Travis.
    DICT_OBJECTS = (
        [
            {obj: obj}
            for obj in PRIMITIVE_OBJECTS
            if (obj.__hash__ is not None and type(obj).__module__ != "numpy")
        ]
        + [{0: obj} for obj in BASE_OBJECTS]
        + [{Foo(123): Foo(456)}]
    )

    RAY_TEST_OBJECTS = BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS

    @ray.remote
    def f(x):
        return x

    # Check that we can pass arguments by value to remote functions and
    # that they are uncorrupted.
    for obj in RAY_TEST_OBJECTS:
        assert_equal(obj, ray.get(f.remote(obj)))
        assert_equal(obj, ray.get(ray.put(obj)))

    # Test StringIO serialization
    s = io.StringIO("Hello, world!\n")
    s.seek(0)
    line = s.readline()
    s.seek(0)
    assert ray.get(ray.put(s)).readline() == line


def assert_tensors_equivalent(obj1, obj2):
    """
    Recursively compare objects with special handling for torch.Tensor.

    Tensors are considered equivalent if:
      - Same dtype and shape
      - Same device type (e.g., both 'cpu' or both 'cuda'), index ignored
      - Values are equal (or close for floats)
    """
    if isinstance(obj1, torch.Tensor) and isinstance(obj2, torch.Tensor):
        # 1. dtype
        assert obj1.dtype == obj2.dtype, f"dtype mismatch: {obj1.dtype} vs {obj2.dtype}"
        # 2. shape
        assert obj1.shape == obj2.shape, f"shape mismatch: {obj1.shape} vs {obj2.shape}"
        # 3. device type must match (cpu/cpu or cuda/cuda), ignore index
        assert (
            obj1.device.type == obj2.device.type
        ), f"Device type mismatch: {obj1.device} vs {obj2.device}"

        # 4. Compare values safely on CPU
        t1_cpu = obj1.cpu()
        t2_cpu = obj2.cpu()
        if obj1.dtype.is_floating_point or obj1.dtype.is_complex:
            assert torch.allclose(
                t1_cpu, t2_cpu, atol=1e-6, rtol=1e-5
            ), "Floating-point tensors not close"
        else:
            assert torch.equal(t1_cpu, t2_cpu), "Integer/bool tensors not equal"
        return

    # Non-tensor recursive comparison
    if type(obj1) is not type(obj2):
        raise AssertionError(f"Type mismatch: {type(obj1)} vs {type(obj2)}")

    if is_named_tuple(type(obj1)):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif isinstance(obj1, collections.Counter):
        assert obj1 == obj2
    elif isinstance(obj1, collections.OrderedDict):
        assert list(obj1.keys()) == list(obj2.keys())
        for k in obj1:
            assert_tensors_equivalent(obj1[k], obj2[k])
    elif isinstance(obj1, collections.defaultdict):
        assert list(obj1.keys()) == list(obj2.keys())
        for k in obj1:
            assert_tensors_equivalent(obj1[k], obj2[k])
    elif isinstance(obj1, collections.deque):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif isinstance(obj1, dict):
        assert obj1.keys() == obj2.keys()
        for k in obj1:
            assert_tensors_equivalent(obj1[k], obj2[k])
    elif isinstance(obj1, (list, tuple)):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
        # Skip internal Ray attributes
        keys1 = {
            k
            for k in obj1.__dict__.keys()
            if not k.startswith("_ray_") and k != "_pytype_"
        }
        keys2 = {
            k
            for k in obj2.__dict__.keys()
            if not k.startswith("_ray_") and k != "_pytype_"
        }
        assert keys1 == keys2, f"Object attribute keys differ: {keys1} vs {keys2}"
        for k in keys1:
            assert_tensors_equivalent(obj1.__dict__[k], obj2.__dict__[k])
    else:
        assert obj1 == obj2, f"Non-tensor values differ: {obj1} vs {obj2}"


def test_cpu_tensor_serialization(ray_start_cluster_with_zero_copy_tensors):
    PRIMITIVE_OBJECTS = [
        # Existing ones
        torch.tensor(42),
        torch.tensor(3.14159),
        torch.tensor(True),
        torch.tensor([1, 2, 3]),
        torch.tensor([1.0, 2.0, 3.0]),
        torch.tensor([True, False]),
        torch.randn(3, 4).cpu(),
        torch.randint(0, 10, (2, 3)).cpu(),
        torch.zeros(5, dtype=torch.int64).cpu(),
        torch.ones(2, 2, dtype=torch.float32).cpu(),
        torch.tensor([]).cpu(),
        torch.tensor((), dtype=torch.float32).cpu(),
        torch.zeros(0, 5),
        torch.zeros(3, 0),
        torch.zeros(2, 0, 4),
        torch.randn(1, 1, 1, 1),
        torch.randn(2, 3, 4, 5, 6),
        torch.arange(8).reshape(2, 2, 2),
        torch.tensor(99).expand(1, 3, 1),
    ]

    t1 = torch.tensor(1)
    t2 = torch.tensor(2)
    t3 = torch.tensor([3, 4])
    t4 = torch.randn(2).cpu()

    COMPLEX_OBJECTS = [
        [[[[torch.tensor(1)]]]],
        {"a": {"b": {"c": torch.tensor([1, 2, 3])}}},
        ({"x": torch.tensor(10)}, [torch.tensor(1), {"y": torch.randn(2)}]),
        [{"data": torch.tensor([5, 6])}, (torch.tensor(7),)],
        {(): {(): torch.tensor(42)}},
        collections.OrderedDict([("hello", t1), ("world", t3)]),
        collections.defaultdict(
            lambda: torch.tensor(0), [("hello", t1), ("world", t3)]
        ),
        collections.defaultdict(lambda: [], [("a", [t1, t2]), ("b", [t3])]),
        collections.deque([t1, t2, t3, "str", 42, t4]),
    ]

    class TensorBox:
        def __init__(self, t):
            self.tensor = t
            self.meta = "box"

    Point = collections.namedtuple("Point", ["x", "y"])
    TensorRecord = collections.namedtuple("TensorRecord", ["weights", "bias", "meta"])

    CUSTOM_OBJECTS = [
        Point(torch.tensor(1), torch.tensor(2)),
        TensorRecord(
            weights=torch.randn(3, 3).cpu(),
            bias=torch.zeros(3).cpu(),
            meta="linear_layer",
        ),
        Point(
            x=TensorRecord(torch.tensor([1]), torch.tensor([2]), "nested"),
            y=torch.tensor(99),
        ),
        TensorBox(torch.tensor([100, 200])),
        TensorBox({"w": torch.tensor(1.5), "ids": torch.tensor([1, 2, 3])}),
    ]

    TensorData = make_dataclass("TensorData", [("data", torch.Tensor)])
    CUSTOM_OBJECTS.append(TensorData(data=torch.tensor([7, 8, 9]).cpu()))

    BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

    LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
    TUPLE_OBJECTS = [(obj,) for obj in BASE_OBJECTS]
    DICT_OBJECTS = [{0: obj} for obj in BASE_OBJECTS] + [
        {"key": obj} for obj in BASE_OBJECTS if not isinstance(obj, dict)
    ]

    RAY_TEST_OBJECTS = BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS

    @ray.remote
    def echo(x):
        return x

    for obj in RAY_TEST_OBJECTS:
        restored1 = ray.get(ray.put(obj))
        restored2 = ray.get(echo.remote(obj))
        assert_tensors_equivalent(obj, restored1)
        assert_tensors_equivalent(obj, restored2)


@pytest.mark.skipif(not USE_GPU, reason="Skipping GPU Test")
def test_gpu_tensor_serialization(ray_start_cluster_with_zero_copy_tensors):
    if not torch.cuda.is_available():
        pytest.skip("CUDA not available")

    t1 = torch.tensor(1, device="cuda")
    t2 = torch.tensor(2.5, device="cuda")
    t3 = torch.tensor([3, 4], device="cuda")
    t4 = torch.randn(2, device="cuda")

    PRIMITIVE_GPU_OBJECTS = [
        torch.tensor(42, device="cuda"),
        torch.tensor(3.14159, device="cuda"),
        torch.tensor(True, device="cuda"),
        torch.tensor([1, 2, 3], device="cuda"),
        torch.tensor([1.0, 2.0, 3.0], device="cuda"),
        torch.tensor([True, False], device="cuda"),
        torch.randn(3, 4, device="cuda"),
        torch.randint(0, 10, (2, 3), device="cuda"),
        torch.zeros(5, dtype=torch.int64, device="cuda"),
        torch.ones(2, 2, dtype=torch.float32, device="cuda"),
        torch.tensor([], device="cuda"),
        torch.tensor((), dtype=torch.float32, device="cuda"),
        torch.zeros(0, 5, device="cuda"),
        torch.zeros(3, 0, device="cuda"),
        torch.zeros(2, 0, 4, device="cuda"),
        torch.randn(1, 1, 1, 1, device="cuda"),
        torch.randn(2, 3, 4, 5, 6, device="cuda"),
        torch.arange(8, device="cuda").reshape(2, 2, 2),
        torch.tensor(99, device="cuda").expand(1, 3, 1),
    ]

    COMPLEX_GPU_OBJECTS = [
        [[[[torch.tensor(1, device="cuda")]]]],
        {"a": {"b": {"c": torch.tensor([1, 2, 3], device="cuda")}}},
        (
            {"x": torch.tensor(10, device="cuda")},
            [torch.tensor(1, device="cuda"), {"y": torch.randn(2, device="cuda")}],
        ),
        [
            {"data": torch.tensor([5, 6], device="cuda")},
            (torch.tensor(7, device="cuda"),),
        ],
        {(): {(): torch.tensor(42, device="cuda")}},
        collections.OrderedDict([("hello", t1), ("world", t3)]),
        collections.defaultdict(
            lambda: torch.tensor(0, device="cuda"), [("hello", t1), ("world", t3)]
        ),
        collections.defaultdict(lambda: [], [("a", [t1, t2]), ("b", [t3])]),
        collections.deque([t1, t2, t3, "str", 42, t4]),
    ]

    class TensorBox:
        def __init__(self, t):
            self.tensor = t
            self.meta = "box"

    Point = collections.namedtuple("Point", ["x", "y"])
    TensorRecord = collections.namedtuple("TensorRecord", ["weights", "bias", "meta"])

    CUSTOM_GPU_OBJECTS = [
        Point(torch.tensor(1, device="cuda"), torch.tensor(2, device="cuda")),
        TensorRecord(
            weights=torch.randn(3, 3, device="cuda"),
            bias=torch.zeros(3, device="cuda"),
            meta="linear_layer",
        ),
        Point(
            x=TensorRecord(
                torch.tensor([1], device="cuda"),
                torch.tensor([2], device="cuda"),
                "nested",
            ),
            y=torch.tensor(99, device="cuda"),
        ),
        TensorBox(torch.tensor([100, 200], device="cuda")),
        TensorBox(
            {
                "w": torch.tensor(1.5, device="cuda"),
                "ids": torch.tensor([1, 2, 3], device="cuda"),
            }
        ),
    ]

    TensorData = make_dataclass("TensorData", [("data", torch.Tensor)])
    CUSTOM_GPU_OBJECTS.append(TensorData(data=torch.tensor([7, 8, 9], device="cuda")))

    BASE_GPU_OBJECTS = PRIMITIVE_GPU_OBJECTS + COMPLEX_GPU_OBJECTS + CUSTOM_GPU_OBJECTS

    LIST_GPU_OBJECTS = [[obj] for obj in BASE_GPU_OBJECTS]
    TUPLE_GPU_OBJECTS = [(obj,) for obj in BASE_GPU_OBJECTS]
    DICT_GPU_OBJECTS = [{0: obj} for obj in BASE_GPU_OBJECTS] + [
        {"key": obj} for obj in BASE_GPU_OBJECTS if not isinstance(obj, dict)
    ]

    RAY_GPU_TEST_OBJECTS = (
        BASE_GPU_OBJECTS + LIST_GPU_OBJECTS + TUPLE_GPU_OBJECTS + DICT_GPU_OBJECTS
    )

    @ray.remote
    def echo(x):
        return x

    for obj in RAY_GPU_TEST_OBJECTS:
        restored1 = ray.get(ray.put(obj))
        restored2 = ray.get(echo.remote(obj))
        assert_tensors_equivalent(obj, restored1)
        assert_tensors_equivalent(obj, restored2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
