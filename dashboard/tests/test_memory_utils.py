import ray
from ray.dashboard.memory_utils import (
    ReferenceType,
    decode_object_ref_if_needed,
    MemoryTableEntry,
    MemoryTable,
    SortingType,
)

"""Memory Table Unit Test"""

NODE_ADDRESS = "127.0.0.1"
IS_DRIVER = True
PID = 1

OBJECT_ID = "ZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZg=="
ACTOR_ID = "fffffffffffffffffffffffffffffffff66d17ba010000c801000000"
DECODED_ID = decode_object_ref_if_needed(OBJECT_ID)
OBJECT_SIZE = 100


def build_memory_entry(
    *,
    local_ref_count,
    pinned_in_memory,
    submitted_task_reference_count,
    contained_in_owned,
    object_size,
    pid,
    object_id=OBJECT_ID,
    node_address=NODE_ADDRESS
):
    object_ref = {
        "objectId": object_id,
        "callSite": "(task call) /Users:458",
        "objectSize": object_size,
        "localRefCount": local_ref_count,
        "pinnedInMemory": pinned_in_memory,
        "submittedTaskRefCount": submitted_task_reference_count,
        "containedInOwned": contained_in_owned,
    }
    return MemoryTableEntry(
        object_ref=object_ref, node_address=node_address, is_driver=IS_DRIVER, pid=pid
    )


def build_local_reference_entry(
    object_size=OBJECT_SIZE, pid=PID, node_address=NODE_ADDRESS
):
    return build_memory_entry(
        local_ref_count=1,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address,
    )


def build_used_by_pending_task_entry(
    object_size=OBJECT_SIZE, pid=PID, node_address=NODE_ADDRESS
):
    return build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=2,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address,
    )


def build_captured_in_object_entry(
    object_size=OBJECT_SIZE, pid=PID, node_address=NODE_ADDRESS
):
    return build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[OBJECT_ID],
        object_size=object_size,
        pid=pid,
        node_address=node_address,
    )


def build_actor_handle_entry(
    object_size=OBJECT_SIZE, pid=PID, node_address=NODE_ADDRESS
):
    return build_memory_entry(
        local_ref_count=1,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address,
        object_id=ACTOR_ID,
    )


def build_pinned_in_memory_entry(
    object_size=OBJECT_SIZE, pid=PID, node_address=NODE_ADDRESS
):
    return build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=True,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=object_size,
        pid=pid,
        node_address=node_address,
    )


def build_entry(
    object_size=OBJECT_SIZE,
    pid=PID,
    node_address=NODE_ADDRESS,
    reference_type=ReferenceType.PINNED_IN_MEMORY,
):
    if reference_type == ReferenceType.USED_BY_PENDING_TASK:
        return build_used_by_pending_task_entry(
            pid=pid, object_size=object_size, node_address=node_address
        )
    elif reference_type == ReferenceType.LOCAL_REFERENCE:
        return build_local_reference_entry(
            pid=pid, object_size=object_size, node_address=node_address
        )
    elif reference_type == ReferenceType.PINNED_IN_MEMORY:
        return build_pinned_in_memory_entry(
            pid=pid, object_size=object_size, node_address=node_address
        )
    elif reference_type == ReferenceType.ACTOR_HANDLE:
        return build_actor_handle_entry(
            pid=pid, object_size=object_size, node_address=node_address
        )
    elif reference_type == ReferenceType.CAPTURED_IN_OBJECT:
        return build_captured_in_object_entry(
            pid=pid, object_size=object_size, node_address=node_address
        )


def test_invalid_memory_entry():
    memory_entry = build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=OBJECT_SIZE,
        pid=PID,
    )
    assert memory_entry.is_valid() is False
    memory_entry = build_memory_entry(
        local_ref_count=0,
        pinned_in_memory=False,
        submitted_task_reference_count=0,
        contained_in_owned=[],
        object_size=-1,
        pid=PID,
    )
    assert memory_entry.is_valid() is False


def test_valid_reference_memory_entry():
    memory_entry = build_local_reference_entry()
    assert memory_entry.reference_type == ReferenceType.LOCAL_REFERENCE
    assert memory_entry.object_ref == ray.ObjectRef(
        decode_object_ref_if_needed(OBJECT_ID)
    )
    assert memory_entry.is_valid() is True


def test_reference_type():
    # pinned in memory
    memory_entry = build_pinned_in_memory_entry()
    assert memory_entry.reference_type == ReferenceType.PINNED_IN_MEMORY

    # used by pending task
    memory_entry = build_used_by_pending_task_entry()
    assert memory_entry.reference_type == ReferenceType.USED_BY_PENDING_TASK

    # captued in object
    memory_entry = build_captured_in_object_entry()
    assert memory_entry.reference_type == ReferenceType.CAPTURED_IN_OBJECT

    # actor handle
    memory_entry = build_actor_handle_entry()
    assert memory_entry.reference_type == ReferenceType.ACTOR_HANDLE


def test_memory_table_summary():
    entries = [
        build_pinned_in_memory_entry(),
        build_used_by_pending_task_entry(),
        build_captured_in_object_entry(),
        build_actor_handle_entry(),
        build_local_reference_entry(),
        build_local_reference_entry(),
    ]
    memory_table = MemoryTable(entries)
    assert len(memory_table.group) == 1
    assert memory_table.summary["total_actor_handles"] == 1
    assert memory_table.summary["total_captured_in_objects"] == 1
    assert memory_table.summary["total_local_ref_count"] == 2
    assert memory_table.summary["total_object_size"] == len(entries) * OBJECT_SIZE
    assert memory_table.summary["total_pinned_in_memory"] == 1
    assert memory_table.summary["total_used_by_pending_task"] == 1


def test_memory_table_sort_by_pid():
    unsort = [1, 3, 2]
    entries = [build_entry(pid=pid) for pid in unsort]
    memory_table = MemoryTable(entries, sort_by_type=SortingType.PID)
    sort = sorted(unsort)
    for pid, entry in zip(sort, memory_table.table):
        assert pid == entry.pid


def test_memory_table_sort_by_reference_type():
    unsort = [
        ReferenceType.USED_BY_PENDING_TASK,
        ReferenceType.LOCAL_REFERENCE,
        ReferenceType.LOCAL_REFERENCE,
        ReferenceType.PINNED_IN_MEMORY,
    ]
    entries = [build_entry(reference_type=reference_type) for reference_type in unsort]
    memory_table = MemoryTable(entries, sort_by_type=SortingType.REFERENCE_TYPE)
    sort = sorted(unsort)
    for reference_type, entry in zip(sort, memory_table.table):
        assert reference_type == entry.reference_type


def test_memory_table_sort_by_object_size():
    unsort = [312, 214, -1, 1244, 642]
    entries = [build_entry(object_size=object_size) for object_size in unsort]
    memory_table = MemoryTable(entries, sort_by_type=SortingType.OBJECT_SIZE)
    sort = sorted(unsort)
    for object_size, entry in zip(sort, memory_table.table):
        assert object_size == entry.object_size


def test_group_by():
    node_second = "127.0.0.2"
    node_first = "127.0.0.1"
    entries = [
        build_entry(node_address=node_second, pid=2),
        build_entry(node_address=node_second, pid=1),
        build_entry(node_address=node_first, pid=2),
        build_entry(node_address=node_first, pid=1),
    ]
    memory_table = MemoryTable(entries)

    # Make sure it is correctly grouped
    assert node_first in memory_table.group
    assert node_second in memory_table.group

    # make sure pid is sorted in the right order.
    for group_key, group_memory_table in memory_table.group.items():
        pid = 1
        for entry in group_memory_table.table:
            assert pid == entry.pid
            pid += 1


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
