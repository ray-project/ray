import re
import sys
import threading

import pytest
import ray

from ray.exceptions import RayTaskError, RayActorError

"""This module tests stacktrace of Ray.

There are total 3 different stacktrace types in Ray.

1. Not nested task (including actor creation) or actor task failure.
2. Chained task + actor task failure.
3. Dependency failure (upstreamed dependency raises an exception).

There are important factors.
- The root cause of the failure should be printed at the bottom.
- Ray-related code shouldn't be printed at all to the user-level stacktrace.
- It should be easy to follow stacktrace.

Each of test verifies that there's no regression by comparing the line number.
If we include unnecessary stacktrace (e.g., logs from internal files),
these tests will fail.
"""


def scrub_traceback(ex):
    assert isinstance(ex, str)
    print(ex)
    ex = ex.strip("\n")
    ex = re.sub("pid=[0-9]+,", "pid=XXX,", ex)
    ex = re.sub("ip=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+", "ip=YYY", ex)
    ex = re.sub("repr=.*\)", "repr=ZZZ)", ex)
    ex = re.sub("line .*,", "line ZZ,", ex)
    ex = re.sub('".*"', '"FILE"', ex)
    # These are used to coloring the string.
    ex = re.sub("\\x1b\[36m", "", ex)
    ex = re.sub("\\x1b\[39m", "", ex)
    # When running bazel test with pytest 6.x, the module name becomes
    # "python.ray.tests.test_traceback" instead of just "test_traceback"
    ex = re.sub(r"python\.ray\.tests\.test_traceback", "test_traceback", ex)
    # Clean up object address.
    ex = re.sub("object at .*>", "object at ADDRESS>", ex)
    return ex


def clean_noqa(ex):
    assert isinstance(ex, str)
    # noqa is required to ignore lint, so we just remove it.
    ex = re.sub(" # noqa", "", ex)
    return ex


@pytest.mark.skipif(
    sys.platform == "win32", reason="Clean stacktrace not supported on Windows"
)
def test_actor_creation_stacktrace(ray_start_regular):
    """Test the actor creation task stacktrace."""
    expected_output = """The actor died because of an error raised in its creation task, ray::A.__init__() (pid=XXX, ip=YYY, repr=ZZZ) # noqa
  File "FILE", line ZZ, in __init__
    g(3)
  File "FILE", line ZZ, in g
    raise ValueError(a)
ValueError: 3"""

    def g(a):
        raise ValueError(a)

    @ray.remote
    class A:
        def __init__(self):
            g(3)

        def ping(self):
            pass

    try:
        a = A.remote()
        ray.get(a.ping.remote())
    except RayActorError as ex:
        print(ex)
        assert clean_noqa(expected_output) == scrub_traceback(str(ex))


@pytest.mark.skipif(
    sys.platform == "win32", reason="Clean stacktrace not supported on Windows"
)
def test_task_stacktrace(ray_start_regular):
    """Test the normal task stacktrace."""
    expected_output = """ray::f() (pid=XXX, ip=YYY)
  File "FILE", line ZZ, in f
    return g(c)
  File "FILE", line ZZ, in g
    raise ValueError(a)
ValueError: 7"""

    def g(a):
        raise ValueError(a)
        # pass

    @ray.remote
    def f():
        a = 3
        b = 4
        c = a + b
        return g(c)

    try:
        ray.get(f.remote())
    except ValueError as ex:
        print(ex)
        assert clean_noqa(expected_output) == scrub_traceback(str(ex))


@pytest.mark.skipif(
    sys.platform == "win32", reason="Clean stacktrace not supported on Windows"
)
def test_actor_task_stacktrace(ray_start_regular):
    """Test the actor task stacktrace."""
    expected_output = """ray::A.f() (pid=XXX, ip=YYY, repr=ZZZ) # noqa
  File "FILE", line ZZ, in f
    return g(c)
  File "FILE", line ZZ, in g
    raise ValueError(a)
ValueError: 7"""

    def g(a):
        raise ValueError(a)

    @ray.remote
    class A:
        def f(self):
            a = 3
            b = 4
            c = a + b
            return g(c)

    a = A.remote()
    try:
        ray.get(a.f.remote())
    except ValueError as ex:
        print(ex)
        assert clean_noqa(expected_output) == scrub_traceback(str(ex))


@pytest.mark.skipif(
    sys.platform == "win32", reason="Clean stacktrace not supported on Windows"
)
def test_exception_chain(ray_start_regular):
    """Test the chained stacktrace."""
    expected_output = """ray::foo() (pid=XXX, ip=YYY) # noqa
  File "FILE", line ZZ, in foo
    return ray.get(bar.remote())
ray.exceptions.RayTaskError(ZeroDivisionError): ray::bar() (pid=XXX, ip=YYY)
  File "FILE", line ZZ, in bar
    return 1 / 0
ZeroDivisionError: division by zero"""

    @ray.remote
    def bar():
        return 1 / 0

    @ray.remote
    def foo():
        return ray.get(bar.remote())

    r = foo.remote()
    try:
        ray.get(r)
    except ZeroDivisionError as ex:
        assert isinstance(ex, RayTaskError)
        print(ex)
        assert clean_noqa(expected_output) == scrub_traceback(str(ex))


@pytest.mark.skipif(
    sys.platform == "win32", reason="Clean stacktrace not supported on Windows"
)
def test_dep_failure(ray_start_regular):
    """Test the stacktrace genereated due to dependency failures."""
    expected_output = """ray::f() (pid=XXX, ip=YYY) # noqa
  At least one of the input arguments for this task could not be computed:
ray.exceptions.RayTaskError: ray::a() (pid=XXX, ip=YYY)
  At least one of the input arguments for this task could not be computed:
ray.exceptions.RayTaskError: ray::b() (pid=XXX, ip=YYY)
  File "FILE", line ZZ, in b
    raise ValueError("FILE")
ValueError: b failed"""

    @ray.remote
    def f(a, b):
        pass

    @ray.remote
    def a(d):
        pass

    @ray.remote
    def b():
        raise ValueError("b failed")

    try:
        ray.get(f.remote(a.remote(b.remote()), b.remote()))
    except Exception as ex:
        print(ex)
        from pprint import pprint

        pprint(clean_noqa(expected_output))
        pprint(scrub_traceback(str(ex)))
        assert clean_noqa(expected_output) == scrub_traceback(str(ex))


@pytest.mark.skipif(
    sys.platform == "win32", reason="Clean stacktrace not supported on Windows"
)
def test_actor_repr_in_traceback(ray_start_regular):
    def parse_labels_from_traceback(ex):
        error_msg = str(ex)
        error_lines = error_msg.split("\n")
        traceback_line = error_lines[0]
        unformatted_labels = traceback_line.split("(")[2].split(", ")
        label_dict = {}
        for label in unformatted_labels:
            # Remove parenthesis if included.
            if label.startswith("("):
                label = label[1:]
            elif label.endswith(")"):
                label = label[:-1]
            key, value = label.split("=", 1)
            label_dict[key] = value
        return label_dict

    # Test the default repr is Actor(repr=[class_name])
    def g(a):
        raise ValueError(a)

    @ray.remote
    class A:
        def f(self):
            a = 3
            b = 4
            c = a + b
            return g(c)

        def get_repr(self):
            return repr(self)

    a = A.remote()
    try:
        ray.get(a.f.remote())
    except ValueError as ex:
        print(ex)
        label_dict = parse_labels_from_traceback(ex)
        assert label_dict["repr"] == ray.get(a.get_repr.remote())

    # Test if the repr is properly overwritten.
    actor_repr = "ABC"

    @ray.remote
    class A:
        def f(self):
            a = 3
            b = 4
            c = a + b
            return g(c)

        def __repr__(self):
            return actor_repr

    a = A.remote()
    try:
        ray.get(a.f.remote())
    except ValueError as ex:
        print(ex)
        label_dict = parse_labels_from_traceback(ex)
        assert label_dict["repr"] == actor_repr


def test_unpickleable_stacktrace(shutdown_only):
    expected_output = """System error: Failed to unpickle serialized exception
traceback: Traceback (most recent call last):
  File "FILE", line ZZ, in from_ray_exception
    return pickle.loads(ray_exception.serialized_exception)
TypeError: __init__() missing 1 required positional argument: 'arg'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "FILE", line ZZ, in deserialize_objects
    obj = self._deserialize_object(data, metadata, object_ref)
  File "FILE", line ZZ, in _deserialize_object
    return RayError.from_bytes(obj)
  File "FILE", line ZZ, in from_bytes
    return RayError.from_ray_exception(ray_exception)
  File "FILE", line ZZ, in from_ray_exception
    raise RuntimeError(msg) from e
RuntimeError: Failed to unpickle serialized exception"""

    class NoPickleError(OSError):
        def __init__(self, arg):
            pass

    def g(a):
        raise NoPickleError("asdf")

    @ray.remote
    def f():
        a = 3
        b = 4
        c = a + b
        return g(c)

    try:
        ray.get(f.remote())
    except Exception as ex:
        assert clean_noqa(expected_output) == scrub_traceback(str(ex))


def test_serialization_error_message(shutdown_only):
    expected_output_task = """Could not serialize the argument <unlocked _thread.lock object at ADDRESS> for a task or actor test_traceback.test_serialization_error_message.<locals>.task_with_unserializable_arg. Check https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting for more information."""  # noqa
    expected_output_actor = """Could not serialize the argument <unlocked _thread.lock object at ADDRESS> for a task or actor test_traceback.test_serialization_error_message.<locals>.A.__init__. Check https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting for more information."""  # noqa
    expected_capture_output_task = """Could not serialize the function test_traceback.test_serialization_error_message.<locals>.capture_lock. Check https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting for more information."""  # noqa
    expected_capture_output_actor = """Could not serialize the actor class test_traceback.test_serialization_error_message.<locals>.B.__init__. Check https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting for more information."""  # noqa
    ray.init(num_cpus=1)
    lock = threading.Lock()

    @ray.remote
    def task_with_unserializable_arg(lock):
        print(lock)

    @ray.remote
    class A:
        def __init__(self, lock):
            print(lock)

    @ray.remote
    def capture_lock():
        print(lock)

    @ray.remote
    class B:
        def __init__(self):
            print(lock)

    """
    Test a task with an unserializable object.
    """
    with pytest.raises(TypeError) as excinfo:
        task_with_unserializable_arg.remote(lock)

    def scrub_traceback(ex):
        return re.sub("object at .*> for a", "object at ADDRESS> for a", ex)

    test_prefix = "com_github_ray_project_ray.python.ray.tests."

    assert clean_noqa(expected_output_task) == scrub_traceback(
        str(excinfo.value)
    ).replace(test_prefix, "")
    """
    Test an actor with an unserializable object.
    """
    with pytest.raises(TypeError) as excinfo:
        a = A.remote(lock)
        print(a)
    assert clean_noqa(expected_output_actor) == scrub_traceback(
        str(excinfo.value)
    ).replace(test_prefix, "")
    """
    Test the case where an unserializable object is captured by tasks.
    """
    with pytest.raises(TypeError) as excinfo:
        capture_lock.remote()
    assert clean_noqa(expected_capture_output_task) == str(excinfo.value).replace(
        test_prefix, ""
    )
    """
    Test the case where an unserializable object is captured by actors.
    """
    with pytest.raises(TypeError) as excinfo:
        b = B.remote()
        print(b)
    assert clean_noqa(expected_capture_output_actor) == str(excinfo.value).replace(
        test_prefix, ""
    )


if __name__ == "__main__":
    import pytest
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
