from ray.core.generated.logging_pb2 import LogBatch
from ray._raylet import StreamRedirector
from ray._private.utils import open_log
import sys


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


def redirect_stdout_stderr_if_needed(
    stdout_filepath: str,
    stderr_filepath: str,
    rotation_bytes: int,
    rotation_backup_count: int,
):
    """This function sets up redirection for stdout and stderr if needed, based on the given rotation parameters.

    params:
    stdout_filepath: the filepath stdout will be redirected to; if empty, stdout will not be redirected.
    stderr_filepath: the filepath stderr will be redirected to; if empty, stderr will not be redirected.
    rotation_bytes: number of bytes which triggers file rotation.
    rotation_backup_count: the max size of rotation files.
    """

    # Setup redirection for stdout and stderr.
    if stdout_filepath:
        StreamRedirector.redirect_stdout(
            stdout_filepath,
            rotation_bytes,
            rotation_backup_count,
            False,  # tee_to_stdout
            False,  # tee_to_stderr
        )
    if stderr_filepath:
        StreamRedirector.redirect_stderr(
            stderr_filepath,
            rotation_bytes,
            rotation_backup_count,
            False,  # tee_to_stdout
            False,  # tee_to_stderr
        )

    # Setup python system stdout/stderr.
    stdout_fileno = sys.stdout.fileno()
    stderr_fileno = sys.stderr.fileno()
    # We also manually set sys.stdout and sys.stderr because that seems to
    # have an effect on the output buffering. Without doing this, stdout
    # and stderr are heavily buffered resulting in seemingly lost logging
    # statements. We never want to close the stdout file descriptor, dup2 will
    # close it when necessary and we don't want python's GC to close it.
    sys.stdout = open_log(stdout_fileno, unbuffered=True, closefd=False)
    sys.stderr = open_log(stderr_fileno, unbuffered=True, closefd=False)
