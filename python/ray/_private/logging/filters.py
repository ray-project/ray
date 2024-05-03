import logging
import ray


class CoreContextFilter(logging.Filter):
    def filter(self, record):
        runtime_context = ray.get_runtime_context()
        record.job_id = runtime_context.get_job_id()
        record.worker_id = runtime_context.get_worker_id()
        record.node_id = runtime_context.get_node_id()
        if runtime_context.worker.mode == ray.WORKER_MODE:
            actor_id = runtime_context.get_actor_id()
            if actor_id is not None:
                record.actor_id = actor_id
            task_id = runtime_context.get_task_id()
            if task_id is not None:
                record.task_id = task_id
        return True
