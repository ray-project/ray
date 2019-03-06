from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import flatbuffers
import ray.core.generated.ErrorTableData

from ray.core.generated.ActorCheckpointIdData import ActorCheckpointIdData
from ray.core.generated.ClientTableData import ClientTableData
from ray.core.generated.DriverTableData import DriverTableData
from ray.core.generated.ErrorTableData import ErrorTableData
from ray.core.generated.GcsTableEntry import GcsTableEntry
from ray.core.generated.HeartbeatBatchTableData import HeartbeatBatchTableData
from ray.core.generated.HeartbeatTableData import HeartbeatTableData
from ray.core.generated.Language import Language
from ray.core.generated.ObjectTableData import ObjectTableData
from ray.core.generated.ProfileTableData import ProfileTableData
from ray.core.generated.TablePrefix import TablePrefix
from ray.core.generated.TablePubsub import TablePubsub

from ray.core.generated.ray.protocol.Task import Task

__all__ = [
    "ActorCheckpointIdData",
    "ClientTableData",
    "DriverTableData",
    "ErrorTableData",
    "GcsTableEntry",
    "HeartbeatBatchTableData",
    "HeartbeatTableData",
    "Language",
    "ObjectTableData",
    "ProfileTableData",
    "TablePrefix",
    "TablePubsub",
    "Task",
    "construct_error_message",
]

FUNCTION_PREFIX = "RemoteFunction:"
LOG_FILE_CHANNEL = "RAY_LOG_CHANNEL"
REPORTER_CHANNEL = "RAY_REPORTER"

# xray heartbeats
XRAY_HEARTBEAT_CHANNEL = str(TablePubsub.HEARTBEAT).encode("ascii")
XRAY_HEARTBEAT_BATCH_CHANNEL = str(TablePubsub.HEARTBEAT_BATCH).encode("ascii")

# xray driver updates
XRAY_DRIVER_CHANNEL = str(TablePubsub.DRIVER).encode("ascii")

# These prefixes must be kept up-to-date with the TablePrefix enum in gcs.fbs.
# TODO(rkn): We should use scoped enums, in which case we should be able to
# just access the flatbuffer generated values.
TablePrefix_RAYLET_TASK_string = "RAYLET_TASK"
TablePrefix_OBJECT_string = "OBJECT"
TablePrefix_ERROR_INFO_string = "ERROR_INFO"
TablePrefix_PROFILE_string = "PROFILE"


def construct_error_message(driver_id, error_type, message, timestamp):
    """Construct a serialized ErrorTableData object.

    Args:
        driver_id: The ID of the driver that the error should go to. If this is
            nil, then the error will go to all drivers.
        error_type: The type of the error.
        message: The error message.
        timestamp: The time of the error.

    Returns:
        The serialized object.
    """
    builder = flatbuffers.Builder(0)
    driver_offset = builder.CreateString(driver_id.binary())
    error_type_offset = builder.CreateString(error_type)
    message_offset = builder.CreateString(message)

    ray.core.generated.ErrorTableData.ErrorTableDataStart(builder)
    ray.core.generated.ErrorTableData.ErrorTableDataAddJobId(
        builder, driver_offset)
    ray.core.generated.ErrorTableData.ErrorTableDataAddType(
        builder, error_type_offset)
    ray.core.generated.ErrorTableData.ErrorTableDataAddErrorMessage(
        builder, message_offset)
    ray.core.generated.ErrorTableData.ErrorTableDataAddTimestamp(
        builder, timestamp)
    error_data_offset = ray.core.generated.ErrorTableData.ErrorTableDataEnd(
        builder)
    builder.Finish(error_data_offset)

    return bytes(builder.Output())
