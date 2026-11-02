from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray.data.context import DataContext


def get_task_pool_map_operator_cls():
    from ray.data._internal.execution.operators.task_pool_map_operator import (
        TaskPoolMapOperator,
    )

    return TaskPoolMapOperator


def get_actor_pool_map_operator_cls(data_context: Optional["DataContext"] = None):
    from ray.data._internal.execution.execution_flags import actor_only_backend_enabled
    from ray.data._internal.execution.operators.actor_pool_map_operator import (
        ActorPoolMapOperator,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
        ExperimentalActorPoolMapOperator,
    )

    # Read per call, like ``get_streaming_executor_cls``: the flag is checked
    # when an operator is planned, not when this module was first imported.
    if actor_only_backend_enabled(data_context):
        return ExperimentalActorPoolMapOperator
    return ActorPoolMapOperator
