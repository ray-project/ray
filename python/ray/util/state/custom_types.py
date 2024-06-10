from ray.core.generated.common_pb2 import (
    TaskStatus,
    TaskType,
    WorkerExitType,
    WorkerType,
)
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    GcsNodeInfo,
    PlacementGroupTableData,
)
from ray.dashboard.memory_utils import ReferenceType

from typing import Literal


ACTOR_STATUS = [
    "DEPENDENCIES_UNREADY",
    "PENDING_CREATION",
    "ALIVE",
    "RESTARTING",
    "DEAD",
]
TypeActorStatus = Literal[tuple(ACTOR_STATUS)]
PLACEMENT_GROUP_STATUS = [
    "PENDING",
    "CREATED",
    "REMOVED",
    "RESCHEDULING",
]
TypePlacementGroupStatus = Literal[tuple(PLACEMENT_GROUP_STATUS)]
TASK_STATUS = [
    "NIL",
    "PENDING_ARGS_AVAIL",
    "PENDING_NODE_ASSIGNMENT",
    "PENDING_OBJ_STORE_MEM_AVAIL",
    "PENDING_ARGS_FETCH",
    "SUBMITTED_TO_WORKER",
    "RUNNING",
    "RUNNING_IN_RAY_GET",
    "RUNNING_IN_RAY_WAIT",
    "FINISHED",
    "FAILED",
]
TypeTaskStatus = Literal[tuple(TASK_STATUS)]
NODE_STATUS = ["ALIVE", "DEAD"]
TypeNodeStatus = Literal[tuple(NODE_STATUS)]
WORKER_TYPE = [
    "WORKER",
    "DRIVER",
    "SPILL_WORKER",
    "RESTORE_WORKER",
]
TypeWorkerType = Literal[tuple(WORKER_TYPE)]
WORKER_EXIT_TYPE = [
    "SYSTEM_ERROR",
    "INTENDED_SYSTEM_EXIT",
    "USER_ERROR",
    "INTENDED_USER_EXIT",
    "NODE_OUT_OF_MEMORY",
]
TypeWorkerExitType = Literal[tuple(WORKER_EXIT_TYPE)]
TASK_TYPE = [
    "NORMAL_TASK",
    "ACTOR_CREATION_TASK",
    "ACTOR_TASK",
    "DRIVER_TASK",
]
TypeTaskType = Literal[tuple(TASK_TYPE)]
TypeReferenceType = Literal[
    tuple(reference_type.value for reference_type in ReferenceType)
]


def validate_protobuf_enum(grpc_enum, custom_enum):
    """Validate the literal contains the correct enum values from protobuf"""
    enum_vals = set(grpc_enum.DESCRIPTOR.values_by_name)
    # Sometimes, the grpc enum is mocked, and it
    # doesn't include any values in that case.
    if len(enum_vals) > 0:
        assert enum_vals == set(custom_enum)


# Do the enum validation here.
# It is necessary to avoid regression. Alternatively, we can auto generate this
# directly by protobuf.
validate_protobuf_enum(ActorTableData.ActorState, ACTOR_STATUS)
validate_protobuf_enum(
    PlacementGroupTableData.PlacementGroupState, PLACEMENT_GROUP_STATUS
)
validate_protobuf_enum(TaskStatus, TASK_STATUS)
validate_protobuf_enum(GcsNodeInfo.GcsNodeState, NODE_STATUS)
validate_protobuf_enum(WorkerType, WORKER_TYPE)
validate_protobuf_enum(WorkerExitType, WORKER_EXIT_TYPE)
validate_protobuf_enum(TaskType, TASK_TYPE)
