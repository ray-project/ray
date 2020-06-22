from ray.core.generated.gcs_pb2 import (
    ActorCheckpointIdData,
    ActorTableData,
    GcsNodeInfo,
    JobTableData,
    ErrorTableData,
    ErrorType,
    GcsEntry,
    HeartbeatBatchTableData,
    HeartbeatTableData,
    ObjectTableData,
    ProfileTableData,
    TablePrefix,
    TablePubsub,
    TaskTableData,
    ResourceMap,
    ResourceTableData,
    ObjectLocationInfo,
    PubSubMessage,
)

__all__ = [
    "ActorCheckpointIdData",
    "ActorTableData",
    "GcsNodeInfo",
    "JobTableData",
    "ErrorTableData",
    "ErrorType",
    "GcsEntry",
    "HeartbeatBatchTableData",
    "HeartbeatTableData",
    "ObjectTableData",
    "ProfileTableData",
    "TablePrefix",
    "TablePubsub",
    "TaskTableData",
    "ResourceMap",
    "ResourceTableData",
    "construct_error_message",
    "ObjectLocationInfo",
    "PubSubMessage",
]

FUNCTION_PREFIX = "RemoteFunction:"
LOG_FILE_CHANNEL = "RAY_LOG_CHANNEL"
REPORTER_CHANNEL = "RAY_REPORTER"

# xray heartbeats
XRAY_HEARTBEAT_PATTERN = "HEARTBEAT:*".encode("ascii")
XRAY_HEARTBEAT_BATCH_PATTERN = "HEARTBEAT_BATCH:".encode("ascii")

# xray job updates
XRAY_JOB_PATTERN = "JOB:*".encode("ascii")

# Actor pub/sub updates
RAY_ACTOR_PUBSUB_PATTERN = "ACTOR:*".encode("ascii")

# Reporter pub/sub updates
RAY_REPORTER_PUBSUB_PATTERN = "RAY_REPORTER.*".encode("ascii")

# These prefixes must be kept up-to-date with the TablePrefix enum in
# gcs.proto.
# TODO(rkn): We should use scoped enums, in which case we should be able to
# just access the flatbuffer generated values.
TablePrefix_RAYLET_TASK_string = "RAYLET_TASK"
TablePrefix_OBJECT_string = "OBJECT"
TablePrefix_ERROR_INFO_string = "ERROR_INFO"
TablePrefix_PROFILE_string = "PROFILE"
TablePrefix_JOB_string = "JOB"
TablePrefix_ACTOR_string = "ACTOR"


def construct_error_message(job_id, error_type, message, timestamp):
    """Construct a serialized ErrorTableData object.

    Args:
        job_id: The ID of the job that the error should go to. If this is
            nil, then the error will go to all drivers.
        error_type: The type of the error.
        message: The error message.
        timestamp: The time of the error.

    Returns:
        The serialized object.
    """
    data = ErrorTableData()
    data.job_id = job_id.binary()
    data.type = error_type
    data.error_message = message
    data.timestamp = timestamp
    return data.SerializeToString()
