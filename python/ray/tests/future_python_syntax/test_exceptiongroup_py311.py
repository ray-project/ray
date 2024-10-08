import pytest

import ray
from ray.exceptions import RayTaskError


def test_base_exception_group_task(ray_start_regular):
    @ray.remote
    def task():
        raise BaseExceptionGroup("abc", [BaseException("def")])  # noqa: F821

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(task.remote())


def test_base_exception_group_actor(ray_start_regular):
    @ray.remote
    class Actor:
        def f(self):
            raise BaseExceptionGroup("abc", [BaseException("def")])  # noqa: F821

    with pytest.raises(ray.exceptions.ActorDiedError):
        a = Actor.remote()
        ray.get(a.f.remote())


def test_exception_group(ray_start_regular):
    exception_group = ExceptionGroup(  # noqa: F821
        "test", [ValueError("This is an error"), TypeError("This is another error")]
    )

    @ray.remote
    def task():
        raise exception_group

    @ray.remote
    class Actor:
        def f(self):
            raise exception_group

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
