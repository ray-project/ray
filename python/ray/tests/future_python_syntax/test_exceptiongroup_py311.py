import pytest

import ray
from ray.exceptions import RayTaskError


def test_baseexceptiongroup_task(ray_start_regular):
    baseexceptiongroup = BaseExceptionGroup(  # noqa: F821
        "test baseexceptiongroup", [BaseException("abc")]
    )

    @ray.remote
    def task():
        raise baseexceptiongroup

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(task.remote())


def test_baseexceptiongroup_actor(ray_start_regular):
    baseexceptiongroup = BaseExceptionGroup(  # noqa: F821
        "test baseexceptiongroup", [BaseException("abc")]
    )

    @ray.remote
    class Actor:
        def f(self):
            raise baseexceptiongroup

    with pytest.raises(ray.exceptions.ActorDiedError):
        a = Actor.remote()
        ray.get(a.f.remote())


def test_except_exceptiongroup(ray_start_regular):
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


def test_except_star_exception(ray_start_regular):
    @ray.remote
    def task():
        raise ValueError

    @ray.remote
    class Actor:
        def f(self):
            raise ValueError

    try:
        ray.get(task.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)

    try:
        ray.get(task.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], RayTaskError)
        assert isinstance(ex.exceptions[0], ValueError)


def test_except_star_exceptiongroup(ray_start_regular):
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
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 2
        assert isinstance(ex.exceptions[0], ValueError)
        assert isinstance(ex.exceptions[1], TypeError)

    try:
        ray.get(task.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], ValueError)
    except* TypeError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], TypeError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* RayTaskError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 2
        assert isinstance(ex.exceptions[0], ValueError)
        assert isinstance(ex.exceptions[1], TypeError)

    try:
        a = Actor.remote()
        ray.get(a.f.remote())
    except* ValueError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], ValueError)
    except* TypeError as ex:
        assert isinstance(ex, ExceptionGroup)  # noqa: F821
        assert len(ex.exceptions) == 1
        assert isinstance(ex.exceptions[0], TypeError)
