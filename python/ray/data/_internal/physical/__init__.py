def get_task_pool_map_operator_cls():
    from ray.data._internal.physical.task_pool_map_operator import (
        TaskPoolMapOperator,
    )

    return TaskPoolMapOperator


def get_actor_pool_map_operator_cls():
    from ray.data._internal.physical.actor_pool_map_operator import (
        ActorPoolMapOperator,
    )

    return ActorPoolMapOperator
