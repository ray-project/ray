import logging
import ray
from ray._private.ray_logging.constants import LogKey


class CoreContextFilter(logging.Filter):
    def filter(self, record):
        if not ray.is_initialized():
            # There is no additional context if ray is not initialized
            return True

        runtime_context = ray.get_runtime_context()
        setattr(record, LogKey.JOB_ID.value, runtime_context.get_job_id())
        setattr(record, LogKey.WORKER_ID.value, runtime_context.get_worker_id())
        setattr(record, LogKey.NODE_ID.value, runtime_context.get_node_id())
        if runtime_context.worker.mode == ray.WORKER_MODE:
            actor_id = runtime_context.get_actor_id()
            if actor_id is not None:
                setattr(record, LogKey.ACTOR_ID.value, actor_id)
            task_id = runtime_context.get_task_id()
            if task_id is not None:
                setattr(record, LogKey.TASK_ID.value, task_id)
            task_name = runtime_context.get_task_name()
            if task_name is not None:
                setattr(record, LogKey.TASK_NAME.value, task_name)
            task_function_name = runtime_context.get_task_function_name()
            if task_function_name is not None:
                setattr(record, LogKey.TASK_FUNCTION_NAME.value, task_function_name)
            actor_name = runtime_context.get_actor_name()
            if actor_name is not None:
                setattr(record, LogKey.ACTOR_NAME.value, actor_name)
        return True
