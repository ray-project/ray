import base64

from collections import defaultdict
from enum import Enum
from typing import List

import ray

from ray._raylet import (TaskID, ActorID, JobID)

# These values are used to calculate if objectIDs are actor handles.
TASKID_BYTES_SIZE = TaskID.size()
ACTORID_BYTES_SIZE = ActorID.size()
JOBID_BYTES_SIZE = JobID.size()
# We need to multiply 2 because we need bits size instead of bytes size.
TASKID_RANDOM_BITS_SIZE = (TASKID_BYTES_SIZE - ACTORID_BYTES_SIZE) * 2
ACTORID_RANDOM_BITS_SIZE = (ACTORID_BYTES_SIZE - JOBID_BYTES_SIZE) * 2


def decode_object_id_if_needed(object_id: str) -> bytes:
    """Decode objectID bytes string.

    gRPC reply contains an objectID that is encodded by Base64.
    This function is used to decode the objectID.
    Note that there are times that objectID is already decoded as
    a hex string. In this case, just convert it to a binary number.
    """
    if object_id.endswith("="):
        # If the object id ends with =, that means it is base64 encoded.
        # Object ids will always have = as a padding
        # when it is base64 encoded because objectID is always 20B.
        return base64.standard_b64decode(object_id)
    else:
        return ray.utils.hex_to_binary(object_id)


class SortingType(Enum):
    PID = 1
    OBJECT_SIZE = 3
    REFERENCE_TYPE = 4


class GroupByType(Enum):
    NODE_ADDRESS = 2


class ReferenceType:
    # We don't use enum because enum is not json serializable.
    ACTOR_HANDLE = "ACTOR_HANDLE"
    PINNED_IN_MEMORY = "PINNED_IN_MEMORY"
    LOCAL_REFERENCE = "LOCAL_REFERENCE"
    USED_BY_PENDING_TASK = "USED_BY_PENDING_TASK"
    CAPTURED_IN_OBJECT = "CAPTURED_IN_OBJECT"
    UNKNOWN_STATUS = "UNKNOWN_STATUS"


class MemoryTableEntry:
    def __init__(self, *, object_ref: dict, node_address: str, is_driver: bool,
                 pid: int):
        # worker info
        self.is_driver = is_driver
        self.pid = pid
        self.node_address = node_address

        # object info
        self.object_size = int(object_ref.get("objectSize", -1))
        self.call_site = object_ref.get("callSite", "<Unknown>")
        self.object_id = ray.ObjectID(
            decode_object_id_if_needed(object_ref["objectId"]))

        # reference info
        self.local_ref_count = int(object_ref.get("localRefCount", 0))
        self.pinned_in_memory = bool(object_ref.get("pinnedInMemory", False))
        self.submitted_task_ref_count = int(
            object_ref.get("submittedTaskRefCount", 0))
        self.contained_in_owned = [
            ray.ObjectID(decode_object_id_if_needed(object_id))
            for object_id in object_ref.get("containedInOwned", [])
        ]
        self.reference_type = self._get_reference_type()

    def is_valid(self) -> bool:
        # If the entry doesn't have a reference type or some invalid state,
        # (e.g., no object ID presented), it is considered invalid.
        if (not self.pinned_in_memory and self.local_ref_count == 0
                and self.submitted_task_ref_count == 0
                and len(self.contained_in_owned) == 0):
            return False
        elif self.object_id.is_nil():
            return False
        else:
            return True

    def group_key(self, group_by_type: GroupByType) -> str:
        if group_by_type == GroupByType.NODE_ADDRESS:
            return self.node_address
        else:
            raise ValueError(
                "group by type {} is invalid.".format(group_by_type))

    def _get_reference_type(self) -> str:
        if self._is_object_id_actor_handle():
            return ReferenceType.ACTOR_HANDLE
        if self.pinned_in_memory:
            return ReferenceType.PINNED_IN_MEMORY
        elif self.submitted_task_ref_count > 0:
            return ReferenceType.USED_BY_PENDING_TASK
        elif self.local_ref_count > 0:
            return ReferenceType.LOCAL_REFERENCE
        elif len(self.contained_in_owned) > 0:
            return ReferenceType.CAPTURED_IN_OBJECT
        else:
            return ReferenceType.UNKNOWN_STATUS

    def _is_object_id_actor_handle(self) -> bool:
        object_id_hex = self.object_id.hex()

        # random (8B) | ActorID(6B) | flag (2B) | index (6B)
        # ActorID(6B) == ActorRandomByte(4B) + JobID(2B)
        # If random bytes are all 'f', but ActorRandomBytes
        # are not all 'f', that means it is an actor creation
        # task, which is an actor handle.
        random_bits = object_id_hex[:TASKID_RANDOM_BITS_SIZE]
        actor_random_bits = object_id_hex[TASKID_RANDOM_BITS_SIZE:
                                          TASKID_RANDOM_BITS_SIZE +
                                          ACTORID_RANDOM_BITS_SIZE]
        if (random_bits == "f" * 16 and not actor_random_bits == "f" * 8):
            return True
        else:
            return False

    def __dict__(self):
        return {
            "object_id": self.object_id.hex(),
            "pid": self.pid,
            "node_ip_address": self.node_address,
            "object_size": self.object_size,
            "reference_type": self.reference_type,
            "call_site": self.call_site,
            "local_ref_count": self.local_ref_count,
            "pinned_in_memory": self.pinned_in_memory,
            "submitted_task_ref_count": self.submitted_task_ref_count,
            "contained_in_owned": [
                object_id.hex() for object_id in self.contained_in_owned
            ],
            "type": "Driver" if self.is_driver else "Worker"
        }

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.__dict__())


class MemoryTable:
    def __init__(self,
                 entries: List[MemoryTableEntry],
                 group_by_type: GroupByType = GroupByType.NODE_ADDRESS,
                 sort_by_type: SortingType = SortingType.PID):
        self.table = entries
        # Group is a list of memory tables grouped by a group key.
        self.group = {}
        self.summary = defaultdict(int)
        if group_by_type and sort_by_type:
            self.setup(group_by_type, sort_by_type)
        elif group_by_type:
            self._group_by(group_by_type)
        elif sort_by_type:
            self._sort_by(sort_by_type)

    def setup(self, group_by_type: GroupByType, sort_by_type: SortingType):
        """Setup memory table.

        This will sort entries first and gruop them after.
        Sort order will be still kept.
        """
        self._sort_by(sort_by_type)._group_by(group_by_type)
        for group_memory_table in self.group.values():
            group_memory_table.summarize()
        self.summarize()
        return self

    def insert_entry(self, entry: MemoryTableEntry):
        self.table.append(entry)

    def summarize(self):
        # Reset summary.
        total_object_size = 0
        total_local_ref_count = 0
        total_pinned_in_memory = 0
        total_used_by_pending_task = 0
        total_captured_in_objects = 0
        total_actor_handles = 0

        for entry in self.table:
            if entry.object_size > 0:
                total_object_size += entry.object_size
            if entry.reference_type == ReferenceType.LOCAL_REFERENCE:
                total_local_ref_count += 1
            elif entry.reference_type == ReferenceType.PINNED_IN_MEMORY:
                total_pinned_in_memory += 1
            elif entry.reference_type == ReferenceType.USED_BY_PENDING_TASK:
                total_used_by_pending_task += 1
            elif entry.reference_type == ReferenceType.CAPTURED_IN_OBJECT:
                total_captured_in_objects += 1
            elif entry.reference_type == ReferenceType.ACTOR_HANDLE:
                total_actor_handles += 1

        self.summary = {
            "total_object_size": total_object_size,
            "total_local_ref_count": total_local_ref_count,
            "total_pinned_in_memory": total_pinned_in_memory,
            "total_used_by_pending_task": total_used_by_pending_task,
            "total_captured_in_objects": total_captured_in_objects,
            "total_actor_handles": total_actor_handles
        }
        return self

    def _sort_by(self, sorting_type: SortingType):
        if sorting_type == SortingType.PID:
            self.table.sort(key=lambda entry: entry.pid)
        elif sorting_type == SortingType.OBJECT_SIZE:
            self.table.sort(key=lambda entry: entry.object_size)
        elif sorting_type == SortingType.REFERENCE_TYPE:
            self.table.sort(key=lambda entry: entry.reference_type)
        else:
            raise ValueError(
                "Give sorting type: {} is invalid.".format(sorting_type))
        return self

    def _group_by(self, group_by_type: GroupByType):
        """Group entries and summarize the result.

        NOTE: Each group is another MemoryTable.
        """
        # Reset group
        self.group = {}

        # Build entries per group.
        group = defaultdict(list)
        for entry in self.table:
            group[entry.group_key(group_by_type)].append(entry)

        # Build a group table.
        for group_key, entries in group.items():
            self.group[group_key] = MemoryTable(
                entries, group_by_type=None, sort_by_type=None)
        for group_key, group_memory_table in self.group.items():
            group_memory_table.summarize()
        return self

    def __dict__(self):
        return {
            "summary": self.summary,
            "group": {
                group_key: {
                    "entries": group_memory_table.get_entries(),
                    "summary": group_memory_table.summary
                }
                for group_key, group_memory_table in self.group.items()
            }
        }

    def get_entries(self) -> List[dict]:
        return [entry.__dict__() for entry in self.table]

    def __repr__(self):
        return str(self.__dict__())

    def __str__(self):
        return self.__repr__()


def construct_memory_table(workers_info_by_node: dict) -> MemoryTable:
    memory_table_entries = []
    for node_id, worker_infos in workers_info_by_node.items():
        for worker_info in worker_infos:
            pid = worker_info["pid"]
            is_driver = worker_info.get("isDriver", False)
            core_worker_stats = worker_info["coreWorkerStats"]
            node_address = core_worker_stats["ipAddress"]
            object_refs = core_worker_stats.get("objectRefs", [])

            for object_ref in object_refs:
                memory_table_entry = MemoryTableEntry(
                    object_ref=object_ref,
                    node_address=node_address,
                    is_driver=is_driver,
                    pid=pid)
                if memory_table_entry.is_valid():
                    memory_table_entries.append(memory_table_entry)
    memory_table = MemoryTable(memory_table_entries)
    return memory_table
