import base64

import ray
from ray.dashboard.memory_utils import (
    GroupByType,
    MemoryTable,
    MemoryTableEntry,
    ReferenceType,
    SortingType,
    _collect_rdt_objects,
    _format_rdt_memory_summary,
    construct_memory_table,
    decode_object_ref_if_needed,
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
    assert memory_entry.reference_type == ReferenceType.LOCAL_REFERENCE.value
    assert memory_entry.object_ref == ray.ObjectRef(
        decode_object_ref_if_needed(OBJECT_ID)
    )
    assert memory_entry.is_valid() is True


def test_reference_type():
    # pinned in memory
    memory_entry = build_pinned_in_memory_entry()
    assert memory_entry.reference_type == ReferenceType.PINNED_IN_MEMORY.value

    # used by pending task
    memory_entry = build_used_by_pending_task_entry()
    assert memory_entry.reference_type == ReferenceType.USED_BY_PENDING_TASK.value

    # captued in object
    memory_entry = build_captured_in_object_entry()
    assert memory_entry.reference_type == ReferenceType.CAPTURED_IN_OBJECT.value

    # actor handle
    memory_entry = build_actor_handle_entry()
    assert memory_entry.reference_type == ReferenceType.ACTOR_HANDLE.value


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
    sort = sorted([entry.value for entry in unsort])
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


# ---------------------------------------------------------------------------
# RDT store table tests
# ---------------------------------------------------------------------------


def _b64(obj_id_hex):
    """Base64-encode a hex object ID, as protojson does for bytes fields."""
    return base64.standard_b64encode(bytes.fromhex(obj_id_hex)).decode()


RDT_HEX_1 = "00" * ray.ObjectRef.size()
RDT_HEX_2 = "01" * ray.ObjectRef.size()


def _build_rdt_object(
    object_id=RDT_HEX_1,
    tensor_bytes=None,
    num_copies=1,
    num_errors=0,
):
    rdt_object = {
        "objectId": _b64(object_id),
        "numCopies": num_copies,
        "numErrors": num_errors,
    }
    if tensor_bytes is not None:
        rdt_object["tensorBytes"] = tensor_bytes
    return rdt_object


def _build_worker_stats(
    rdt_objects=None,
    rdt_objects_total=None,
    pid=1,
    worker_type="DRIVER",
    node_address=NODE_ADDRESS,
):
    stats = {
        "pid": pid,
        "ipAddress": node_address,
        "workerType": worker_type,
    }
    if rdt_objects is not None:
        stats["rdtObjects"] = rdt_objects
    if rdt_objects_total is not None:
        stats["rdtObjectsTotal"] = rdt_objects_total
    return stats


def test_rdt_memory_summary_empty_when_no_rdt_objects():
    # Zero total + zero rows must produce no RDT table, leaving `ray memory`
    # byte-for-byte unchanged.
    stats = [_build_worker_stats(rdt_objects=[], rdt_objects_total=0)]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert out == ""


def test_rdt_memory_summary_truncated_to_zero_rows_shows_notice():
    # Native limit=0 returns no rows but rdtObjectsTotal > 0: still show the
    # RDT section header plus a truncation notice (not "no RDT output").
    stats = [_build_worker_stats(rdt_objects=[], rdt_objects_total=3)]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert "--- RDT store objects" in out
    assert "showing 0 of 3 RDT objects" in out


def test_rdt_memory_summary_renders_row():
    stats = [
        _build_worker_stats(
            rdt_objects=[_build_rdt_object(tensor_bytes={"cpu": 80})],
            rdt_objects_total=1,
        )
    ]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert "--- RDT store objects" in out
    assert RDT_HEX_1 in out
    assert "cpu" in out
    assert "80 B" in out
    assert "Copies" in out and "Errors" in out


def _separator_offsets(line):
    """Return the column indexes of every ``" | "`` separator in a table row."""
    offsets = []
    start = 0
    while True:
        idx = line.find(" | ", start)
        if idx == -1:
            break
        offsets.append(idx)
        start = idx + 3
    return offsets


def test_rdt_memory_summary_column_alignment():
    # Header and data rows must share the same separator offsets, including for
    # multi-device rows and the full 28-byte (56 hex char) ObjectID.
    objects = [
        _build_rdt_object(object_id=RDT_HEX_1, tensor_bytes={"cpu": 80}),
        _build_rdt_object(object_id=RDT_HEX_2, tensor_bytes={"cpu": 80, "cuda:0": 64}),
    ]
    stats = [_build_worker_stats(rdt_objects=objects, rdt_objects_total=2)]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    header = next(line for line in out.splitlines() if "Object ID" in line)
    for obj_id in (RDT_HEX_1, RDT_HEX_2):
        data = next(line for line in out.splitlines() if obj_id in line)
        assert _separator_offsets(header) == _separator_offsets(data)


def test_rdt_memory_summary_producer_without_object_ref():
    # A producer actor holds tensors in its RDT store but has no ObjectRef for
    # the object; the RDT table still renders that row, and the object-reference
    # table stays empty (RDT residency must not leak into ObjectRefInfo).
    stats = [
        _build_worker_stats(
            rdt_objects=[_build_rdt_object(tensor_bytes={"cpu": 16})],
            rdt_objects_total=1,
            worker_type="WORKER",
        )
    ]
    assert len(construct_memory_table(stats).table) == 0
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert "Worker" in out
    assert RDT_HEX_1 in out


def test_rdt_memory_summary_mixed_workers():
    stats = [
        _build_worker_stats(
            rdt_objects=[_build_rdt_object(tensor_bytes={"cpu": 16})],
            rdt_objects_total=1,
            worker_type="WORKER",
        ),
        _build_worker_stats(
            rdt_objects=[
                _build_rdt_object(object_id=RDT_HEX_2, tensor_bytes={"cuda:0": 64})
            ],
            rdt_objects_total=1,
            worker_type="DRIVER",
        ),
    ]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert RDT_HEX_1 in out and RDT_HEX_2 in out
    assert "cuda:0" in out and "cpu" in out


def test_collect_rdt_objects_cells_empty_errors_unknown():
    objects = [
        # Empty tensor list: a real 0-byte copy (no tensorBytes).
        _build_rdt_object(
            object_id=RDT_HEX_1, tensor_bytes={}, num_copies=1, num_errors=0
        ),
        # Error-only: zero tensor bytes, errors shown explicitly.
        _build_rdt_object(
            object_id=RDT_HEX_2, tensor_bytes={}, num_copies=0, num_errors=1
        ),
        # Unknown custom object: device "unknown" with -1 bytes.
        _build_rdt_object(
            object_id="02" * ray.ObjectRef.size(),
            tensor_bytes={"unknown": -1},
            num_copies=1,
            num_errors=0,
        ),
    ]
    stats = [_build_worker_stats(rdt_objects=objects, rdt_objects_total=3)]
    rows, returned, total = _collect_rdt_objects(stats)
    assert returned == 3 and total == 3
    by_obj = {row["object_id"]: row for row in rows}

    assert by_obj[RDT_HEX_1]["tensor_bytes"] == {}
    assert by_obj[RDT_HEX_1]["num_copies"] == 1
    assert by_obj[RDT_HEX_2]["tensor_bytes"] == {}
    assert by_obj[RDT_HEX_2]["num_errors"] == 1
    assert by_obj["02" * ray.ObjectRef.size()]["tensor_bytes"] == {"unknown": -1}


def test_collect_rdt_objects_multi_device_is_one_row():
    obj = _build_rdt_object(tensor_bytes={"cpu": 80, "cuda:0": 64})
    stats = [_build_worker_stats(rdt_objects=[obj], rdt_objects_total=1)]
    rows, returned, total = _collect_rdt_objects(stats)
    assert returned == 1 and total == 1
    assert len(rows) == 1
    assert rows[0]["tensor_bytes"] == {"cpu": 80, "cuda:0": 64}


def test_collect_rdt_objects_string_int64_and_omitted_zeros():
    obj = {
        "objectId": _b64(RDT_HEX_1),
        "tensorBytes": {"cpu": "80"},  # int64 rendered as a string
        "numCopies": "2",  # count rendered as a string
        # numErrors omitted -> 0
    }
    stats = [
        _build_worker_stats(
            rdt_objects=[obj],
            rdt_objects_total="1",  # total rendered as a string
        )
    ]
    rows, returned, total = _collect_rdt_objects(stats)
    assert returned == 1 and total == 1
    assert rows[0]["tensor_bytes"] == {"cpu": 80}
    assert rows[0]["num_copies"] == 2
    assert rows[0]["num_errors"] == 0


def test_rdt_memory_summary_unit():
    stats = [
        _build_worker_stats(
            rdt_objects=[_build_rdt_object(tensor_bytes={"cpu": 1024})],
            rdt_objects_total=1,
        )
    ]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "KB", None
    )
    assert "1.024 KB" in out


def test_rdt_memory_summary_truncation_note():
    # Native reports 3 total objects but only returned 1 (request limit).
    stats = [
        _build_worker_stats(
            rdt_objects=[_build_rdt_object(tensor_bytes={"cpu": 8})],
            rdt_objects_total=3,
        )
    ]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert "showing 1 of 3 RDT objects" in out


def test_rdt_memory_summary_num_entries_counts_objects():
    # One multi-device object plus a second object; num_entries=1 must show the
    # whole first object (both devices) and not cut the multi-device row in half.
    objects = [
        _build_rdt_object(object_id=RDT_HEX_1, tensor_bytes={"cpu": 8, "cuda:0": 16}),
        _build_rdt_object(object_id=RDT_HEX_2, tensor_bytes={"cpu": 8}),
    ]
    stats = [_build_worker_stats(rdt_objects=objects, rdt_objects_total=2)]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.PID, "B", 1
    )
    assert RDT_HEX_1 in out
    assert "cuda:0" in out  # second device of the same object is still shown
    assert RDT_HEX_2 not in out


def test_rdt_memory_summary_sort_by_object_size():
    # OBJECT_SIZE sorts by known logical bytes, unknown last.
    objects = [
        _build_rdt_object(object_id=RDT_HEX_1, tensor_bytes={"cpu": 8}),
        _build_rdt_object(object_id=RDT_HEX_2, tensor_bytes={"cpu": 64}),
    ]
    stats = [_build_worker_stats(rdt_objects=objects, rdt_objects_total=2)]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.OBJECT_SIZE, "B", None
    )
    assert out.index(RDT_HEX_1) < out.index(RDT_HEX_2)


def test_rdt_memory_summary_sort_by_pid():
    objects = [
        _build_rdt_object(object_id=RDT_HEX_2, tensor_bytes={"cpu": 8}),
    ]
    stats = [
        _build_worker_stats(rdt_objects=[], rdt_objects_total=0, pid=1),
        _build_worker_stats(rdt_objects=objects, rdt_objects_total=1, pid=2),
    ]
    out = _format_rdt_memory_summary(
        stats, GroupByType.NODE_ADDRESS, SortingType.PID, "B", None
    )
    assert "--- RDT store objects" in out
    assert RDT_HEX_2 in out


def test_rdt_memory_summary_stack_trace_groups_by_node():
    stats = [
        _build_worker_stats(
            rdt_objects=[_build_rdt_object(tensor_bytes={"cpu": 8})],
            rdt_objects_total=1,
        )
    ]
    out = _format_rdt_memory_summary(
        stats, GroupByType.STACK_TRACE, SortingType.OBJECT_SIZE, "B", None
    )
    assert "no call site" in out
    assert RDT_HEX_1 in out


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
