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


def test_actor_creation_stacktrace(ray_start_regular):
    """Test the actor creation task stacktrace.

    Expected output:
        The actor died because of an error raised in its creation task, ray::A.__init__() (pid=23585, ip=192.168.1.5) # noqa
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 27, in __init__
            g(3)
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 22, in g
            raise ValueError(a)
        ValueError: 3
    """

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
        error_msg = str(ex)
        error_lines = error_msg.split("\n")
        assert len(error_lines) == 6


def test_task_stacktrace(ray_start_regular):
    """Test the normal task stacktrace.

    Expected output:
        ray::f() (pid=23839, ip=192.168.1.5) # noqa
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 63, in f
            return g(c)
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 55, in g
            raise ValueError(a)
        ValueError: 7
    """

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
        error_msg = str(ex)
        error_lines = error_msg.split("\n")
        assert len(error_lines) == 6


def test_actor_task_stacktrace(ray_start_regular):
    """Test the actor task stacktrace.

    Expected output:
        ray::A.f() (pid=24029, ip=192.168.1.5, repr=<test_traceback.A object at 0x7fe1dde3ec50>) # noqa
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 95, in f
            return g(c)
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 87, in g
            raise ValueError(a)
        ValueError: 7
    """

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
        error_msg = str(ex)
        error_lines = error_msg.split("\n")
        assert len(error_lines) == 6


def test_exception_chain(ray_start_regular):
    """Test the chained stacktrace.
    
    Expected output:
        ray::foo() (pid=24171, ip=192.168.1.5, label=ray::foo()) # noqa
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 127, in foo
            return ray.get(bar.remote())
        ray.exceptions.RayTaskError(ZeroDivisionError): ray::bar() (pid=24186, ip=192.168.1.5)
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 123, in bar
            return 1 / 0
        ZeroDivisionError: division by zero
    """

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
        error_msg = str(ex)
        error_lines = error_msg.split("\n")
        assert len(error_lines) == 7


def test_dep_failure(ray_start_regular):
    """Test the stacktrace genereated due to dependency failures.
    
    Expected output:
        ray::f() (pid=24413, ip=192.168.1.5) # noqa
          The task, ray::f(), failed because the below input task has failed.
        ray.exceptions.RayTaskError: ray::a() (pid=24413, ip=192.168.1.5)
          The task, ray::a(), failed because the below input task has failed.
        ray.exceptions.RayTaskError: ray::b() (pid=24413, ip=192.168.1.5)
          File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 159, in b
            raise ValueError("b failed")
        ValueError: b failed
    """

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
        error_msg = str(ex)
        error_lines = error_msg.split("\n")
        assert len(error_lines) == 8


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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
