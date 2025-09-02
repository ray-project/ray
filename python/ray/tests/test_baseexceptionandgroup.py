import sys
from textwrap import dedent

import pytest

import ray
from ray.exceptions import (
    ActorDiedError,
    RayTaskError,
    TaskCancelledError,
    WorkerCrashedError,
)


def test_baseexception_task(ray_start_regular_shared):
    class MyBaseException(BaseException):
        pass

    @ray.remote
    def task():
        raise MyBaseException("abc")

    with pytest.raises(MyBaseException):
        ray.get(task.remote())


def test_baseexception_actor_task(ray_start_regular_shared):
    class MyBaseException(BaseException):
        pass

    @ray.remote
    class Actor:
        def f(self):
            raise MyBaseException("abc")

        async def async_f(self):
            raise MyBaseException("abc")

    a = Actor.remote()
    with pytest.raises(MyBaseException):
        ray.get(a.f.remote())

    with pytest.raises(MyBaseException):
        ray.get(a.async_f.remote())


def test_baseexception_actor_creation(ray_start_regular_shared):
    class MyBaseException(BaseException):
        pass

    @ray.remote
    class Actor:
        def __init__(self):
            raise MyBaseException("abc")

    with pytest.raises(ActorDiedError) as e:
        a = Actor.remote()
        ray.get(a.__ray_ready__.remote())
    assert "MyBaseException" in str(e.value)


def test_baseexception_streaming_generator(ray_start_regular_shared):
    class MyBaseException(BaseException):
        pass

    @ray.remote
    def raise_at_beginning():
        raise MyBaseException("rip")
        yield 1

    raise_at_beginning_ref = raise_at_beginning.remote()
    with pytest.raises(MyBaseException):
        ray.get(next(raise_at_beginning_ref))

    @ray.remote
    def raise_at_middle():
        for i in range(1, 10):
            if i == 5:
                raise MyBaseException("rip")
            yield i

    raise_at_middle_ref = raise_at_middle.remote()
    for i in range(1, 5):
        assert i == ray.get(next(raise_at_middle_ref))
    with pytest.raises(MyBaseException):
        ray.get(next(raise_at_middle_ref))

    @ray.remote(_generator_backpressure_num_objects=1)
    def raise_after_backpressure():
        for i in range(1, 10):
            if i == 5:
                raise MyBaseException("rip")
            yield i

    raise_after_backpressure_ref = raise_after_backpressure.remote()
    for i in range(1, 5):
        assert i == ray.get(next(raise_after_backpressure_ref))
    with pytest.raises(MyBaseException):
        ray.get(next(raise_after_backpressure_ref))


def test_raise_system_exit(ray_start_regular_shared):
    @ray.remote
    def task():
        raise SystemExit("abc")

    with pytest.raises(WorkerCrashedError):
        ray.get(task.remote())


def test_raise_keyboard_interrupt(ray_start_regular_shared):
    @ray.remote
    def task():
        raise KeyboardInterrupt("abc")

    with pytest.raises(TaskCancelledError):
        ray.get(task.remote())


skip_if_python_less_than_3_11 = pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="ExceptionGroup is only available in Python 3.11+",
)


@skip_if_python_less_than_3_11
def test_baseexceptiongroup_task(ray_start_regular_shared):
    baseexceptiongroup = BaseExceptionGroup(  # noqa: F821
        "test baseexceptiongroup", [BaseException("abc")]
    )

    @ray.remote
    def task():
        raise baseexceptiongroup

    with pytest.raises(ray.exceptions.RayTaskError):  # noqa: F821
        ray.get(task.remote())


@skip_if_python_less_than_3_11
def test_baseexceptiongroup_actor(ray_start_regular_shared):
    baseexceptiongroup = BaseExceptionGroup(  # noqa: F821
        "test baseexceptiongroup", [BaseException("abc")]
    )

    @ray.remote
    class Actor:
        def f(self):
            raise baseexceptiongroup

    with pytest.raises(ray.exceptions.RayTaskError):  # noqa: F821
        a = Actor.remote()
        ray.get(a.f.remote())


@skip_if_python_less_than_3_11
def test_except_exceptiongroup(ray_start_regular_shared):
    exceptiongroup = ExceptionGroup(  # noqa: F821
        "test exceptiongroup", [ValueError(), TypeError()]
    )

    @ray.remote
    def task():
        raise exceptiongroup

    @ray.remote
    class Actor:
        def f(self):
            raise exceptiongroup

    try:
        ray.get(task.remote())
    except Exception as ex:
        assert isinstance(ex, RayTaskError)
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 2
        assert isinstance(ex.exceptions[0], ValueError)
        assert isinstance(ex.exceptions[1], TypeError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except Exception as ex:
        assert isinstance(ex, RayTaskError)
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 2
        assert isinstance(ex.exceptions[0], ValueError)
        assert isinstance(ex.exceptions[1], TypeError)


@skip_if_python_less_than_3_11
def test_except_star_exception(ray_start_regular_shared):
    @ray.remote
    def task():
        raise ValueError

    @ray.remote
    class Actor:
        def f(self):
            raise ValueError

    # TODO: Don't use exec() when we only support Python 3.11+
    # Here the exec() is used to avoid SyntaxError for except* for Python < 3.11
    python_code = dedent(
        """\
    try:
        ray.get(task.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)

    try:
        ray.get(task.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)
    """
    )
    exec(python_code)


@skip_if_python_less_than_3_11
def test_except_star_exceptiongroup(ray_start_regular_shared):
    exceptiongroup = ExceptionGroup(  # noqa: F821
        "test exceptiongroup", [ValueError(), TypeError()]
    )

    @ray.remote
    def task():
        raise exceptiongroup

    @ray.remote
    class Actor:
        def f(self):
            raise exceptiongroup

    # TODO: Don't use exec() when we only support Python 3.11+
    # Here the exec() is used to avoid SyntaxError for except* for Python < 3.11
    python_code = dedent(
        """\
    try:
        ray.get(task.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 2
        assert isinstance(ex.exceptions[0], ValueError)
        assert isinstance(ex.exceptions[1], TypeError)

    try:
        ray.get(task.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], ValueError)
    except* TypeError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], TypeError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 2
        assert isinstance(ex.exceptions[0], ValueError)
        assert isinstance(ex.exceptions[1], TypeError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], ValueError)
    except* TypeError as ex:
        assert isinstance(ex, ExceptionGroup)
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], TypeError)
    """
    )
    exec(python_code)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
