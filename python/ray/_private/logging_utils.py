from ray.core.generated.logging_pb2 import LogBatch


def log_batch_dict_to_proto(log_json: dict) -> LogBatch:
    """Converts a dict containing a batch of logs to a LogBatch proto."""
    return LogBatch(
        ip=log_json.get("ip"),
        # Cast to support string pid like "gcs".
        pid=str(log_json.get("pid")) if log_json.get("pid") else None,
        # Job ID as a hex string.
        job_id=log_json.get("job"),
        is_error=bool(log_json.get("is_err")),
        lines=log_json.get("lines"),
        actor_name=log_json.get("actor_name"),
        task_name=log_json.get("task_name"),
    )


def log_batch_proto_to_dict(log_batch: LogBatch) -> dict:
    """Converts a LogBatch proto to a dict containing a batch of logs."""
    return {
        "ip": log_batch.ip,
        "pid": log_batch.pid,
        "job": log_batch.job_id,
        "is_err": log_batch.is_error,
        "lines": log_batch.lines,
        "actor_name": log_batch.actor_name,
        "task_name": log_batch.task_name,
    }
