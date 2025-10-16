import logging
from typing import Any, Dict

import ray
from ray._private.ray_logging.constants import LogKey


class CoreContextFilter(logging.Filter):
    TASK_LEVEL_LOG_KEYS = [
        LogKey.TASK_ID.value,
        LogKey.TASK_NAME.value,
        LogKey.TASK_FUNCTION_NAME.value,
    ]

    @classmethod
    def get_ray_core_logging_context(cls) -> Dict[str, Any]:
        """
        Get the ray core logging context as a dict.
        Only use this function if you need include the attributes to the log record
        yourself by bypassing the filter.
        """
        if not ray.is_initialized():
            # There is no additional context if ray is not initialized
            return {}

        runtime_context = ray.get_runtime_context()
        ray_core_logging_context = {
            LogKey.JOB_ID.value: runtime_context.get_job_id(),
            LogKey.WORKER_ID.value: runtime_context.get_worker_id(),
            LogKey.NODE_ID.value: runtime_context.get_node_id(),
        }
        if runtime_context.worker.mode == ray.WORKER_MODE:
            ray_core_logging_context[
                LogKey.ACTOR_ID.value
            ] = runtime_context.get_actor_id()
            ray_core_logging_context[
                LogKey.TASK_ID.value
            ] = runtime_context.get_task_id()
            ray_core_logging_context[
                LogKey.TASK_NAME.value
            ] = runtime_context.get_task_name()
            ray_core_logging_context[
                LogKey.TASK_FUNCTION_NAME.value
            ] = runtime_context.get_task_function_name()
            ray_core_logging_context[
                LogKey.ACTOR_NAME.value
            ] = runtime_context.get_actor_name()
        return ray_core_logging_context

    def filter(self, record):
        context = self.get_ray_core_logging_context()
        for key, value in context.items():
            if value is not None:
                setattr(record, key, value)
        return True
