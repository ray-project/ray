import pytest

from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.execution_flags import (
    actor_only_backend_enabled,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    _pop_actor_task_remote_args,
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


def test_pop_actor_task_remote_args_translates_task_options():
    remote_args = {
        "num_cpus": 1,
        "retry_exceptions": True,
        "_generator_backpressure_num_objects": 4,
        "max_retries": 0,
        "num_returns": 2,
        "max_calls": 1,
        "_num_objects_per_yield": 8,
    }

    task_args = _pop_actor_task_remote_args(remote_args)

    assert task_args == {
        "retry_exceptions": True,
        "_generator_backpressure_num_objects": 4,
        "max_task_retries": 0,
    }
    assert remote_args == {"num_cpus": 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
