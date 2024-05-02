import logging
import ray
import json

from ray._private.ray_constants import (
    LOGGER_RAY_ATTRS,
    LOGRECORD_STANDARD_ATTRS,
)


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


class JSONFormatter(logging.Formatter):
    def format(self, record):
        record_format = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "msg": record.getMessage(),
            "filename": record.filename,
            "lineno": record.lineno,
        }
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            record_format["exc_text"] = record.exc_text

        for key, value in record.__dict__.items():
            if key in LOGGER_RAY_ATTRS:  # Ray context
                record_format[key] = value
            elif key not in LOGRECORD_STANDARD_ATTRS:  # User-provided context
                record_format[key] = value
        return json.dumps(record_format)
