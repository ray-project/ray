import ray

from ray.exceptions import RayTaskError, RayActorError
"""This module tests stacktrace of Ray.

There are total 4 different stacktrace types in Ray.

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
        The actor died because of an error raised in its creation task, ray::A.__init__() (pid=23585, ip=192.168.1.5, label=ray::A.__init__()) # noqa
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
        len(error_lines) == 6


def test_task_stacktrace(ray_start_regular):
    """Test the normal task stacktrace.

    Expected output:
        ray::f() (pid=23839, ip=192.168.1.5, label=ray::f()) # noqa
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
        len(error_lines) == 6


def test_actor_task_stacktrace(ray_start_regular):
    """Test the actor task stacktrace.

    Expected output:
        ray::A.f() (pid=24029, ip=192.168.1.5, label=Actor(class_name=test_actor_task_stacktrace.<locals>.A, actor_id=99da335726d2f7fa26e5c83201000000) # noqa
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
        len(error_lines) == 6


def test_exception_chain(ray_start_regular):
    """Test the chained stacktrace.
    
    Expected output:
        ray::foo() (pid=24171, ip=192.168.1.5, label=ray::foo()) # noqa
        File "/Users/sangbincho/work/ray/python/ray/tests/test_traceback.py", line 127, in foo
            return ray.get(bar.remote())
        ray.exceptions.RayTaskError(ZeroDivisionError): ray::bar() (pid=24186, ip=192.168.1.5, label=ray::bar())
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
        len(error_lines) == 7


def test_dep_failure(ray_start_regular):
    """Test the stacktrace genereated due to dependency failures.
    
    Expected output:
        ray::f() (pid=24413, ip=192.168.1.5, label=ray::f()) # noqa
          The task, ray::f(), failed because the below input task has failed.
        ray.exceptions.RayTaskError: ray::a() (pid=24413, ip=192.168.1.5, label=ray::a())
          The task, ray::a(), failed because the below input task has failed.
        ray.exceptions.RayTaskError: ray::b() (pid=24413, ip=192.168.1.5, label=ray::b())
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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
