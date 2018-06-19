from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import flatbuffers

import ray.core.generated.ErrorTableData as ErrorTableData
from ray.core.generated.TablePrefix import TablePrefix
from ray.core.generated.TablePubsub import TablePubsub

__all__ = ["TablePrefix", "TablePubsub", "construct_error_message"]


def construct_error_message(error_type, message, timestamp):
    """Construct a serialized ErrorTableData object.

    Args:
        error_type: The type of the error.
        message: The error message.
        timestamp: The time of the error.

    Returns:
        The serialized object.
    """
    builder = flatbuffers.Builder(0)
    error_type_offset = builder.CreateString(error_type)
    message_offset = builder.CreateString(message)

    ErrorTableData.ErrorTableDataStart(builder)
    ErrorTableData.ErrorTableDataAddType(builder, error_type_offset)
    ErrorTableData.ErrorTableDataAddErrorMessage(builder, message_offset)
    ErrorTableData.ErrorTableDataAddTimestamp(builder, timestamp)
    error_data_offset = ErrorTableData.ErrorTableDataEnd(builder)
    builder.Finish(error_data_offset)

    return bytes(builder.Output())
