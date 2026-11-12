import pytest

from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.execution_flags import (
    actor_only_backend_enabled,
)
from ray.data._internal.util import get_compute_strategy


def _identity(batch):
    return batch


class _Identity:
    def __call__(self, batch):
        return batch


def test_plain_function_with_actor_pool_strategy():
    actor_pool = ActorPoolStrategy(min_size=1, max_size=20)
    assert get_compute_strategy(_identity, compute=actor_pool) is actor_pool


def test_plain_function_with_actor_pool_strategy_rejected_without_backend(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("RAY_DATA_ACTOR_ONLY_BACKEND", raising=False)
    assert not actor_only_backend_enabled()

    with pytest.raises(ValueError, match="regular functions with the actor"):
        get_compute_strategy(
            _identity, compute=ActorPoolStrategy(min_size=1, max_size=20)
        )


def test_callable_class_with_task_pool_strategy_still_rejected():
    with pytest.raises(ValueError, match="callable classes with the task"):
        get_compute_strategy(_Identity, compute=TaskPoolStrategy())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
