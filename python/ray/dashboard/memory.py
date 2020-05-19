import base64
import typing

from collections import defaultdict
from enum import Enum

import ray


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
        self.submittedd_task_ref_count = int(
            object_ref.get("submittedTaskRefCount", 0))
        self.contained_in_owned = [
            ray.ObjectID(decode_object_id_if_needed(object_id))
            for object_id in object_ref.get("containedInOwned", [])
        ]
        self.reference_type = self._get_reference_type()

    def is_valid(self) -> bool:
        if (not self.pinned_in_memory and self.local_ref_count == 0
                and self.submittedd_task_ref_count == 0
                and len(self.contained_in_owned) == 0):
            return False
        elif self.object_id.is_nil():
            return False
        else:
            return True

    def group_key(self, group_by_type: GroupByType):
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
        elif self.submittedd_task_ref_count > 0:
            return ReferenceType.USED_BY_PENDING_TASK
        elif self.local_ref_count > 0:
            return ReferenceType.LOCAL_REFERENCE
        elif len(self.contained_in_owned) > 0:
            return ReferenceType.CAPTURED_IN_OBJECT
        else:
            return ReferenceType.UNKNOWN_STATUS

    def _is_object_id_actor_handle(self) -> bool:
        object_id_hex = self.object_id.hex()
        random_bits = object_id_hex[:16]
        actor_random_bits = object_id_hex[16:24]

        # random (8B) | ActorID(6B) | flag (2B) | index (6B)
        # ActorID(6B) == ActorRandomByte(4B) + JobID(2B)
        # If random bytes are all 'f', but ActorRandomBytes
        # are not all 'f', that means it is an actor creation
        # task, which is an actor handle.
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
            "submittedd_task_ref_count": self.submittedd_task_ref_count,
            "contained_in_owned": [
                object_id.hex() for object_id in self.contained_in_owned
            ],
            "type": "Driver" if self.is_driver else "Worker"
        }

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.__dict__())


class MemoryTableSummary:
    def __init__(self):
        self.total_object_size = 0
        self.total_local_ref_count = 0
        self.total_pinned_in_memory = 0
        self.total_used_by_pending_task = 0
        self.total_captured_in_objects = 0
        self.total_actor_handles = 0

    def summarize(self, table: typing.List[MemoryTableEntry]):
        for entry in table:
            if entry.object_size > 0:
                self.total_object_size += entry.object_size
            if entry.reference_type == ReferenceType.LOCAL_REFERENCE:
                self.total_local_ref_count += 1
            elif entry.reference_type == ReferenceType.PINNED_IN_MEMORY:
                self.total_pinned_in_memory += 1
            elif entry.reference_type == ReferenceType.USED_BY_PENDING_TASK:
                self.total_used_by_pending_task += 1
            elif entry.reference_type == ReferenceType.CAPTURED_IN_OBJECT:
                self.total_captured_in_objects += 1
            elif entry.reference_type == ReferenceType.ACTOR_HANDLE:
                self.total_actor_handles += 1

    def __dict__(self):
        return {
            "total_object_size": self.total_object_size,
            "total_local_ref_count": self.total_local_ref_count,
            "total_pinned_in_memory": self.total_pinned_in_memory,
            "total_used_by_pending_task": self.total_used_by_pending_task,
            "total_captured_in_objects": self.total_captured_in_objects,
            "total_actor_handles": self.total_actor_handles
        }

    def __str__(self):
        return str(self.__dict__())


class MemoryTable:
    def __init__(self):
        self.table: typing.List[MemoryTableEntry] = []
        # Group is a list of memory tables grouped by a group key.
        self.group: typing.Dict[str, MemoryTable] = self._get_resetted_group()
        self.summary: MemoryTableSummary = MemoryTableSummary()

    def insert_entry(self, entry: MemoryTableEntry):
        """Insert a new memory table entry to the table."""
        self.table.append(entry)

    def setup(self,
              *,
              group_by_type: GroupByType = GroupByType.NODE_ADDRESS,
              sort_by_type: SortingType = SortingType.PID):
        """Setup memory table.

        This will sort entries first and gruop them after.
        Sort order will be still kept.
        """
        self.sort_by(sort_by_type).group_by(group_by_type)
        for group_memory_table in self.group.values():
            group_memory_table.summarize()
        self.summarize()

    def summarize(self):
        # Reset summary.
        self.summary = MemoryTableSummary()
        self.summary.summarize(self.table)
        return self

    def sort_by(self, sorting_type: SortingType):
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

    def group_by(self, group_by_type: GroupByType):
        """Group entries and summarize the result.

        NOTE: Each group is another MemoryTable.
        """
        self.group = self._get_resetted_group()
        for entry in self.table:
            self.group[entry.group_key(group_by_type)].insert_entry(entry)
        for group_key, group_memory_table in self.group.items():
            group_memory_table.summarize()
        return self

    def __dict__(self):
        return {
            "summary": self.summary.__dict__(),
            "group": {
                group_key: {
                    "entries": group_memory_table.get_entries(),
                    "summary": group_memory_table.summary.__dict__()
                }
                for group_key, group_memory_table in self.group.items()
            }
        }

    def get_entries(self):
        return [entry.__dict__() for entry in self.table]

    def _get_resetted_group(self):
        return defaultdict(MemoryTable)

    def __repr__(self):
        return str(self.__dict__())

    def __str__(self):
        return self.__repr__()


def construct_memory_table(workers_info_by_node: dict):
    memory_table = MemoryTable()
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
                    memory_table.insert_entry(memory_table_entry)
    memory_table.setup(
        group_by_type=GroupByType.NODE_ADDRESS, sort_by_type=SortingType.PID)
    return memory_table
