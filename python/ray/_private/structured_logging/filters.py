import logging
import ray

from enum import Enum


class CoreContextEnum(str, Enum):
    JOB_ID = "job_id"
    WORKER_ID = "worker_id"
    NODE_ID = "node_id"
    ACTOR_ID = "actor_id"
    TASK_ID = "task_id"


class CoreContextFilter(logging.Filter):
    def filter(self, record):
        runtime_context = ray.get_runtime_context()
        setattr(record, CoreContextEnum.JOB_ID, runtime_context.get_job_id())
        setattr(record, CoreContextEnum.WORKER_ID, runtime_context.get_worker_id())
        setattr(record, CoreContextEnum.NODE_ID, runtime_context.get_node_id())
        if runtime_context.worker.mode == ray.WORKER_MODE:
            actor_id = runtime_context.get_actor_id()
            if actor_id is not None:
                setattr(record, CoreContextEnum.ACTOR_ID.value, actor_id)
            task_id = runtime_context.get_task_id()
            if task_id is not None:
                setattr(record, CoreContextEnum.TASK_ID.value, task_id)
        return True
