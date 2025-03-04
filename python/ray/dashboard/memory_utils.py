import base64
import logging
from collections import defaultdict
from enum import Enum
from typing import List

import ray
from ray._private.internal_api import node_stats
from ray._raylet import ActorID, JobID, TaskID

logger = logging.getLogger(__name__)

# These values are used to calculate if objectRefs are actor handles.
TASKID_BYTES_SIZE = TaskID.size()
ACTORID_BYTES_SIZE = ActorID.size()
JOBID_BYTES_SIZE = JobID.size()


def decode_object_ref_if_needed(object_ref: str) -> bytes:
    """Decode objectRef bytes string.

    gRPC reply contains an objectRef that is encodded by Base64.
    This function is used to decode the objectRef.
    Note that there are times that objectRef is already decoded as
    a hex string. In this case, just convert it to a binary number.
    """
    if object_ref.endswith("="):
        # If the object ref ends with =, that means it is base64 encoded.
        # Object refs will always have = as a padding
        # when it is base64 encoded because objectRef is always 20B.
        return base64.standard_b64decode(object_ref)
    else:
        return ray._private.utils.hex_to_binary(object_ref)


class SortingType(Enum):
    PID = 1
    OBJECT_SIZE = 3
    REFERENCE_TYPE = 4


class GroupByType(Enum):
    NODE_ADDRESS = "node"
    STACK_TRACE = "stack_trace"


class ReferenceType(Enum):
    # We don't use enum because enum is not json serializable.
    ACTOR_HANDLE = "ACTOR_HANDLE"
    PINNED_IN_MEMORY = "PINNED_IN_MEMORY"
    LOCAL_REFERENCE = "LOCAL_REFERENCE"
    USED_BY_PENDING_TASK = "USED_BY_PENDING_TASK"
    CAPTURED_IN_OBJECT = "CAPTURED_IN_OBJECT"
    UNKNOWN_STATUS = "UNKNOWN_STATUS"


def get_sorting_type(sort_by: str):
    """Translate string input into SortingType instance"""
    sort_by = sort_by.upper()
    if sort_by == "PID":
        return SortingType.PID
    elif sort_by == "OBJECT_SIZE":
        return SortingType.OBJECT_SIZE
    elif sort_by == "REFERENCE_TYPE":
        return SortingType.REFERENCE_TYPE
    else:
        raise Exception(
            "The sort-by input provided is not one of\
                PID, OBJECT_SIZE, or REFERENCE_TYPE."
        )


def get_group_by_type(group_by: str):
    """Translate string input into GroupByType instance"""
    group_by = group_by.upper()
    if group_by == "NODE_ADDRESS":
        return GroupByType.NODE_ADDRESS
    elif group_by == "STACK_TRACE":
        return GroupByType.STACK_TRACE
    else:
        raise Exception(
            "The group-by input provided is not one of\
                NODE_ADDRESS or STACK_TRACE."
        )


class MemoryTableEntry:
    def __init__(
        self, *, object_ref: dict, node_address: str, is_driver: bool, pid: int
    ):
        # worker info
        self.is_driver = is_driver
        self.pid = pid
        self.node_address = node_address

        # object info
        self.task_status = object_ref.get("taskStatus", "?")
        if self.task_status == "NIL":
            self.task_status = "-"
        self.attempt_number = int(object_ref.get("attemptNumber", 0)) + 1
        self.object_size = int(object_ref.get("objectSize", -1))
        self.call_site = object_ref.get("callSite", "<Unknown>")
        if len(self.call_site) == 0:
            self.call_site = "disabled"
        self.object_ref = ray.ObjectRef(
            decode_object_ref_if_needed(object_ref["objectId"])
        )

        # reference info
        self.local_ref_count = int(object_ref.get("localRefCount", 0))
        self.pinned_in_memory = bool(object_ref.get("pinnedInMemory", False))
        self.submitted_task_ref_count = int(object_ref.get("submittedTaskRefCount", 0))
        self.contained_in_owned = [
            ray.ObjectRef(decode_object_ref_if_needed(object_ref))
            for object_ref in object_ref.get("containedInOwned", [])
        ]
        self.reference_type = self._get_reference_type()

    def is_valid(self) -> bool:
        # If the entry doesn't have a reference type or some invalid state,
        # (e.g., no object ref presented), it is considered invalid.
        if (
            not self.pinned_in_memory
            and self.local_ref_count == 0
            and self.submitted_task_ref_count == 0
            and len(self.contained_in_owned) == 0
        ):
            return False
        elif self.object_ref.is_nil():
            return False
        else:
            return True

    def group_key(self, group_by_type: GroupByType) -> str:
        if group_by_type == GroupByType.NODE_ADDRESS:
            return self.node_address
        elif group_by_type == GroupByType.STACK_TRACE:
            return self.call_site
        else:
            raise ValueError(f"group by type {group_by_type} is invalid.")

    def _get_reference_type(self) -> str:
        if self._is_object_ref_actor_handle():
            return ReferenceType.ACTOR_HANDLE.value
        if self.pinned_in_memory:
            return ReferenceType.PINNED_IN_MEMORY.value
        elif self.submitted_task_ref_count > 0:
            return ReferenceType.USED_BY_PENDING_TASK.value
        elif self.local_ref_count > 0:
            return ReferenceType.LOCAL_REFERENCE.value
        elif len(self.contained_in_owned) > 0:
            return ReferenceType.CAPTURED_IN_OBJECT.value
        else:
            return ReferenceType.UNKNOWN_STATUS.value

    def _is_object_ref_actor_handle(self) -> bool:
        object_ref_hex = self.object_ref.hex()

        # We need to multiply 2 because we need bits size instead of bytes size.
        taskid_random_bits_size = (TASKID_BYTES_SIZE - ACTORID_BYTES_SIZE) * 2
        actorid_random_bits_size = (ACTORID_BYTES_SIZE - JOBID_BYTES_SIZE) * 2

        # random (8B) | ActorID(6B) | flag (2B) | index (6B)
        # ActorID(6B) == ActorRandomByte(4B) + JobID(2B)
        # If random bytes are all 'f', but ActorRandomBytes
        # are not all 'f', that means it is an actor creation
        # task, which is an actor handle.
        random_bits = object_ref_hex[:taskid_random_bits_size]
        actor_random_bits = object_ref_hex[
            taskid_random_bits_size : taskid_random_bits_size + actorid_random_bits_size
        ]
        if random_bits == "f" * 16 and not actor_random_bits == "f" * 24:
            return True
        else:
            return False

    def as_dict(self):
        return {
            "object_ref": self.object_ref.hex(),
            "pid": self.pid,
            "node_ip_address": self.node_address,
            "object_size": self.object_size,
            "reference_type": self.reference_type,
            "call_site": self.call_site,
            "task_status": self.task_status,
            "attempt_number": self.attempt_number,
            "local_ref_count": self.local_ref_count,
            "pinned_in_memory": self.pinned_in_memory,
            "submitted_task_ref_count": self.submitted_task_ref_count,
            "contained_in_owned": [
                object_ref.hex() for object_ref in self.contained_in_owned
            ],
            "type": "Driver" if self.is_driver else "Worker",
        }

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return str(self.as_dict())


class MemoryTable:
    def __init__(
        self,
        entries: List[MemoryTableEntry],
        group_by_type: GroupByType = GroupByType.NODE_ADDRESS,
        sort_by_type: SortingType = SortingType.PID,
    ):
        self.table = entries
        # Group is a list of memory tables grouped by a group key.
        self.group = {}
        self.summary = defaultdict(int)
        # NOTE YOU MUST SORT TABLE BEFORE GROUPING.
        # self._group_by(..)._sort_by(..) != self._sort_by(..)._group_by(..)
        if group_by_type and sort_by_type:
            self.setup(group_by_type, sort_by_type)
        elif group_by_type:
            self._group_by(group_by_type)
        elif sort_by_type:
            self._sort_by(sort_by_type)

    def setup(self, group_by_type: GroupByType, sort_by_type: SortingType):
        """Setup memory table.

        This will sort entries first and group them after.
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
            if entry.reference_type == ReferenceType.LOCAL_REFERENCE.value:
                total_local_ref_count += 1
            elif entry.reference_type == ReferenceType.PINNED_IN_MEMORY.value:
                total_pinned_in_memory += 1
            elif entry.reference_type == ReferenceType.USED_BY_PENDING_TASK.value:
                total_used_by_pending_task += 1
            elif entry.reference_type == ReferenceType.CAPTURED_IN_OBJECT.value:
                total_captured_in_objects += 1
            elif entry.reference_type == ReferenceType.ACTOR_HANDLE.value:
                total_actor_handles += 1

        self.summary = {
            "total_object_size": total_object_size,
            "total_local_ref_count": total_local_ref_count,
            "total_pinned_in_memory": total_pinned_in_memory,
            "total_used_by_pending_task": total_used_by_pending_task,
            "total_captured_in_objects": total_captured_in_objects,
            "total_actor_handles": total_actor_handles,
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
            raise ValueError(f"Give sorting type: {sorting_type} is invalid.")
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
                entries, group_by_type=None, sort_by_type=None
            )
        for group_key, group_memory_table in self.group.items():
            group_memory_table.summarize()
        return self

    def as_dict(self):
        return {
            "summary": self.summary,
            "group": {
                group_key: {
                    "entries": group_memory_table.get_entries(),
                    "summary": group_memory_table.summary,
                }
                for group_key, group_memory_table in self.group.items()
            },
        }

    def get_entries(self) -> List[dict]:
        return [entry.as_dict() for entry in self.table]

    def __repr__(self):
        return str(self.as_dict())

    def __str__(self):
        return self.__repr__()


def construct_memory_table(
    workers_stats: List,
    group_by: GroupByType = GroupByType.NODE_ADDRESS,
    sort_by=SortingType.OBJECT_SIZE,
) -> MemoryTable:
    memory_table_entries = []
    for core_worker_stats in workers_stats:
        pid = core_worker_stats["pid"]
        is_driver = core_worker_stats.get("workerType") == "DRIVER"
        node_address = core_worker_stats["ipAddress"]
        object_refs = core_worker_stats.get("objectRefs", [])

        for object_ref in object_refs:
            memory_table_entry = MemoryTableEntry(
                object_ref=object_ref,
                node_address=node_address,
                is_driver=is_driver,
                pid=pid,
            )
            if memory_table_entry.is_valid():
                memory_table_entries.append(memory_table_entry)
    memory_table = MemoryTable(
        memory_table_entries, group_by_type=group_by, sort_by_type=sort_by
    )
    return memory_table


def track_reference_size(group):
    """Returns dictionary mapping reference type
    to memory usage for a given memory table group."""
    d = defaultdict(int)
    table_name = {
        "LOCAL_REFERENCE": "total_local_ref_count",
        "PINNED_IN_MEMORY": "total_pinned_in_memory",
        "USED_BY_PENDING_TASK": "total_used_by_pending_task",
        "CAPTURED_IN_OBJECT": "total_captured_in_objects",
        "ACTOR_HANDLE": "total_actor_handles",
    }
    for entry in group["entries"]:
        size = entry["object_size"]
        if size == -1:
            # size not recorded
            size = 0
        d[table_name[entry["reference_type"]]] += size
    return d


def memory_summary(
    state,
    group_by="NODE_ADDRESS",
    sort_by="OBJECT_SIZE",
    line_wrap=True,
    unit="B",
    num_entries=None,
) -> str:
    # Get terminal size
    import shutil

    from ray.dashboard.modules.node.node_head import node_stats_to_dict

    size = shutil.get_terminal_size((80, 20)).columns
    line_wrap_threshold = 137

    # Unit conversions
    units = {"B": 10**0, "KB": 10**3, "MB": 10**6, "GB": 10**9}

    # Fetch core memory worker stats, store as a dictionary
    core_worker_stats = []
    for raylet in state.node_table():
        if not raylet["Alive"]:
            continue
        try:
            stats = node_stats_to_dict(
                node_stats(raylet["NodeManagerAddress"], raylet["NodeManagerPort"])
            )
        except RuntimeError:
            continue
        core_worker_stats.extend(stats["coreWorkersStats"])
        assert type(stats) is dict and "coreWorkersStats" in stats

    # Build memory table with "group_by" and "sort_by" parameters
    group_by, sort_by = get_group_by_type(group_by), get_sorting_type(sort_by)
    memory_table = construct_memory_table(
        core_worker_stats, group_by, sort_by
    ).as_dict()
    assert "summary" in memory_table and "group" in memory_table

    # Build memory summary
    mem = ""
    group_by, sort_by = group_by.name.lower().replace(
        "_", " "
    ), sort_by.name.lower().replace("_", " ")
    summary_labels = [
        "Mem Used by Objects",
        "Local References",
        "Pinned",
        "Used by task",
        "Captured in Objects",
        "Actor Handles",
    ]
    summary_string = "{:<19}  {:<16}  {:<12}  {:<13}  {:<19}  {:<13}\n"

    object_ref_labels = [
        "IP Address",
        "PID",
        "Type",
        "Call Site",
        "Status",
        "Attampt",
        "Size",
        "Reference Type",
        "Object Ref",
    ]
    object_ref_string = "{:<13} | {:<8} | {:<7} | {:<9} \
| {:<9} | {:<8} | {:<8} | {:<14} | {:<10}\n"

    if size > line_wrap_threshold and line_wrap:
        object_ref_string = "{:<15}  {:<5}  {:<6}  {:<22}  {:<14}  {:<8}  {:<6}  \
{:<18}  {:<56}\n"

    mem += f"Grouping by {group_by}...\
        Sorting by {sort_by}...\
        Display {num_entries if num_entries is not None else 'all'}\
entries per group...\n\n\n"

    for key, group in memory_table["group"].items():
        # Group summary
        summary = group["summary"]
        ref_size = track_reference_size(group)
        for k, v in summary.items():
            if k == "total_object_size":
                summary[k] = str(v / units[unit]) + f" {unit}"
            else:
                summary[k] = str(v) + f", ({ref_size[k] / units[unit]} {unit})"
        mem += f"--- Summary for {group_by}: {key} ---\n"
        mem += summary_string.format(*summary_labels)
        mem += summary_string.format(*summary.values()) + "\n"

        # Memory table per group
        mem += f"--- Object references for {group_by}: {key} ---\n"
        mem += object_ref_string.format(*object_ref_labels)
        n = 1  # Counter for num entries per group
        for entry in group["entries"]:
            if num_entries is not None and n > num_entries:
                break
            entry["object_size"] = (
                str(entry["object_size"] / units[unit]) + f" {unit}"
                if entry["object_size"] > -1
                else "?"
            )
            num_lines = 1
            if size > line_wrap_threshold and line_wrap:
                call_site_length = 22
                if len(entry["call_site"]) == 0:
                    entry["call_site"] = ["disabled"]
                else:
                    entry["call_site"] = [
                        entry["call_site"][i : i + call_site_length]
                        for i in range(0, len(entry["call_site"]), call_site_length)
                    ]

                task_status_length = 12
                entry["task_status"] = [
                    entry["task_status"][i : i + task_status_length]
                    for i in range(0, len(entry["task_status"]), task_status_length)
                ]
                num_lines = max(len(entry["call_site"]), len(entry["task_status"]))

            else:
                mem += "\n"
            object_ref_values = [
                entry["node_ip_address"],
                entry["pid"],
                entry["type"],
                entry["call_site"],
                entry["task_status"],
                entry["attempt_number"],
                entry["object_size"],
                entry["reference_type"],
                entry["object_ref"],
            ]
            for i in range(len(object_ref_values)):
                if not isinstance(object_ref_values[i], list):
                    object_ref_values[i] = [object_ref_values[i]]
                object_ref_values[i].extend(
                    ["" for x in range(num_lines - len(object_ref_values[i]))]
                )
            for i in range(num_lines):
                row = [elem[i] for elem in object_ref_values]
                mem += object_ref_string.format(*row)
            mem += "\n"
            n += 1

    mem += (
        "To record callsite information for each ObjectRef created, set "
        "env variable RAY_record_ref_creation_sites=1\n\n"
    )

    return mem
