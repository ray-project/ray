from ray.core.generated.common_pb2 import (
    TaskStatus,
    TaskType,
    WorkerExitType,
    WorkerType,
    ErrorType,
    Language,
)
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    GcsNodeInfo,
    PlacementGroupTableData,
)

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
    "PREPARED",
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
    "PENDING_ACTOR_TASK_ARGS_FETCH",
    "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY",
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
# TODO(kevin85421): `class ReferenceType(Enum)` is defined in
# `dashboard/memory_utils.py` to avoid complex dependencies. I redefined
# it here. Eventually, we should remove the one in `dashboard/memory_utils.py`
# and define it under `ray/_private`.
REFERENCE_TYPE = [
    "ACTOR_HANDLE",
    "PINNED_IN_MEMORY",
    "LOCAL_REFERENCE",
    "USED_BY_PENDING_TASK",
    "CAPTURED_IN_OBJECT",
    "UNKNOWN_STATUS",
]
TypeReferenceType = Literal[tuple(REFERENCE_TYPE)]
# The ErrorType enum is used in the export API so it is public
# and any modifications must be backward compatible.
ERROR_TYPE = [
    "WORKER_DIED",
    "ACTOR_DIED",
    "OBJECT_UNRECONSTRUCTABLE",
    "TASK_EXECUTION_EXCEPTION",
    "OBJECT_IN_PLASMA",
    "TASK_CANCELLED",
    "ACTOR_CREATION_FAILED",
    "RUNTIME_ENV_SETUP_FAILED",
    "OBJECT_LOST",
    "OWNER_DIED",
    "OBJECT_DELETED",
    "DEPENDENCY_RESOLUTION_FAILED",
    "OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED",
    "OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED",
    "OBJECT_FETCH_TIMED_OUT",
    "LOCAL_RAYLET_DIED",
    "TASK_PLACEMENT_GROUP_REMOVED",
    "ACTOR_PLACEMENT_GROUP_REMOVED",
    "TASK_UNSCHEDULABLE_ERROR",
    "ACTOR_UNSCHEDULABLE_ERROR",
    "OUT_OF_DISK_ERROR",
    "OBJECT_FREED",
    "OUT_OF_MEMORY",
    "NODE_DIED",
    "END_OF_STREAMING_GENERATOR",
    "ACTOR_UNAVAILABLE",
]
# The Language enum is used in the export API so it is public
# and any modifications must be backward compatible.
LANGUAGE = ["PYTHON", "JAVA", "CPP"]


def validate_protobuf_enum(grpc_enum, custom_enum):
    """Validate the literal contains the correct enum values from protobuf"""
    enum_vals = set(grpc_enum.DESCRIPTOR.values_by_name)
    # Sometimes, the grpc enum is mocked, and it
    # doesn't include any values in that case.
    if len(enum_vals) > 0:
        assert enum_vals == set(
            custom_enum
        ), """Literals and protos out of sync,\
consider building //:install_py_proto with bazel?"""


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
validate_protobuf_enum(ErrorType, ERROR_TYPE)
validate_protobuf_enum(Language, LANGUAGE)
