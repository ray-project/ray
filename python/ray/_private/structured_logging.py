import logging
import ray
import json

LOGRECORD_STANDARD_ATTRS = logging.makeLogRecord({"message": "test"}).__dict__.keys()
LOG_RAY_CORE_CONTEXT = "ray_core"
LOGGER_RAY_ATTRS = {LOG_RAY_CORE_CONTEXT}


class CoreContextFilter(logging.Filter):
    def filter(self, record):
        runtime_context = ray.get_runtime_context()
        ray_core_context = {
            "job_id": runtime_context.get_job_id(),
            "worker_id": runtime_context.get_worker_id(),
            "node_id": runtime_context.get_node_id(),
        }
        if runtime_context.worker.mode == ray.WORKER_MODE:
            ray_core_context["actor_id"] = runtime_context.get_actor_id()
            ray_core_context["task_id"] = runtime_context.get_task_id()
        setattr(record, LOG_RAY_CORE_CONTEXT, ray_core_context)
        return True


class JSONFormatter(logging.Formatter):
    def format(self, record):
        super().format(record)
        record_format = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "msg": record.getMessage(),
            "filename": record.filename,
            "lineno": record.lineno,
        }
        for key, value in record.__dict__.items():
            if key in LOGGER_RAY_ATTRS:  # Ray context
                record_format.update(value)
            elif key not in LOGRECORD_STANDARD_ATTRS:  # User-provided context
                record_format[key] = value
        return json.dumps(record_format)
