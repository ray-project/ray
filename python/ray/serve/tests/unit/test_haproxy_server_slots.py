"""Unit tests for HAProxy runtime-managed server slots (no HAProxy binary)."""
import sys

import pytest

from ray.serve._private.haproxy import HAProxyManager
from ray.serve._private.haproxy_server_slots import (
    SLOT_PLACEHOLDER_HOST,
    SLOT_PLACEHOLDER_PORT,
    SRV_ADMF_FMAINT,
    SRV_ADMF_IMAINT,
    SRV_ADMF_RMAINT,
    RuntimeServerState,
    ServerSlot,
    ServerSlotTable,
    SlotServer,
    SlotState,
    get_safe_name,
    idle_slot_names,
    is_slot_name,
    map_sync_commands,
    parse_map_entries,
    parse_server_idleness,
    parse_servers_state,
    replica_map_key,
    required_slot_capacity,
    runtime_serves_slot,
    server_sync_commands,
    slot_name,
)

# `disabled` in the config sets both forced (0x01) and config (0x04) maintenance.
_CONFIG_DISABLED = SRV_ADMF_FMAINT | 0x04


def _server(name: str, port: int, host: str = "10.0.0.1") -> SlotServer:
    return SlotServer(
        name=name, host=host, port=port, replica_id=f"SERVE_REPLICA::app#{name}"
    )


def _states(table: ServerSlotTable) -> dict:
    return {
        slot.name: (slot.state, slot.server.name if slot.server else None)
        for slot in table.slots
    }


def _runtime(
    addr: str = SLOT_PLACEHOLDER_HOST,
    port: int = SLOT_PLACEHOLDER_PORT,
    admin_state: int = _CONFIG_DISABLED,
    check_port: int = 0,
) -> RuntimeServerState:
    return RuntimeServerState(
        backend="be",
        server="SERVE_REPLICA_SLOT_1",
        addr=addr,
        port=port,
        admin_state=admin_state,
        check_port=check_port,
    )


@pytest.mark.parametrize(
    "needed,minimum,expected",
    [(0, 16, 16), (16, 16, 16), (17, 16, 32), (100, 16, 128), (3, 2, 4), (1, 0, 1)],
)
def test_required_slot_capacity(needed, minimum, expected):
    assert required_slot_capacity(needed, minimum) == expected


@pytest.mark.parametrize(
    "name,expected",
    [
        ("SERVE_REPLICA_SLOT_1", True),
        ("SERVE_REPLICA_SLOT_128", True),
        ("SERVE_REPLICA_SLOT_0", False),
        ("SERVE_REPLICA_SLOT_", False),
        ("SERVE_REPLICA_SLOT_01", False),
        ("SERVE_REPLICA__app-dep-abc", False),
        ("BACKEND", False),
    ],
)
def test_is_slot_name(name, expected):
    assert is_slot_name(name) is expected


def test_slot_names_match_server_template_numbering():
    table = ServerSlotTable(3)
    assert [slot.name for slot in table.slots] == [slot_name(i) for i in (1, 2, 3)]
    assert table.slot_by_name("SERVE_REPLICA_SLOT_3") is table.slots[2]
    assert table.slot_by_name("SERVE_REPLICA_SLOT_4") is None
    assert table.slot_by_name("other") is None


def test_new_table_rejects_non_positive_capacity():
    with pytest.raises(ValueError):
        ServerSlotTable(0)


def test_assign_places_replicas_in_lowest_free_slots_and_is_idempotent():
    table = ServerSlotTable(4)
    desired = [_server("b", 2), _server("a", 1)]

    first = table.assign(desired)
    # Sorted by name so placement does not depend on broadcast order.
    assert [slot.name for slot in first.activated] == [slot_name(1), slot_name(2)]
    assert _states(table)[slot_name(1)] == (SlotState.ACTIVE, "a")
    assert _states(table)[slot_name(2)] == (SlotState.ACTIVE, "b")

    second = table.assign(list(reversed(desired)))
    assert not (
        second.activated or second.deactivated or second.released or second.unplaced
    )


def test_removed_replica_drains_and_its_slot_is_not_reused_immediately():
    table = ServerSlotTable(2)
    table.assign([_server("a", 1)])

    # "a" is removed and "b" added in the same update. Even with a (stale) idle
    # sample, "b" must not take a's slot before a's maintenance is confirmed.
    result = table.assign([_server("b", 2)], idle_slot_names={slot_name(1)})

    assert [slot.name for slot in result.deactivated] == [slot_name(1)]
    assert result.released == []
    assert [slot.name for slot in result.activated] == [slot_name(2)]
    assert _states(table)[slot_name(1)] == (SlotState.DRAINING, "a")


def test_draining_slot_is_reused_only_once_confirmed_idle():
    table = ServerSlotTable(2)
    table.assign([_server("a", 1), _server("b", 2)])
    table.assign([_server("b", 2)])  # "a" starts draining.

    # Busy draining slot: the new replica does not fit.
    busy = table.assign([_server("b", 2), _server("c", 3)], idle_slot_names=set())
    assert busy.released == []
    assert busy.unplaced == [_server("c", 3)]

    # Idle in a later sample: the slot is released and handed to "c".
    idle = table.assign(
        [_server("b", 2), _server("c", 3)], idle_slot_names={slot_name(1)}
    )
    assert [slot.name for slot in idle.released] == [slot_name(1)]
    assert [slot.name for slot in idle.activated] == [slot_name(1)]
    assert _states(table)[slot_name(1)] == (SlotState.ACTIVE, "c")


def test_readded_replica_reactivates_its_draining_slot():
    table = ServerSlotTable(4)
    table.assign([_server("a", 1)])
    table.assign([])

    result = table.assign([_server("a", 1)])

    assert [slot.name for slot in result.activated] == [slot_name(1)]
    assert _states(table)[slot_name(1)] == (SlotState.ACTIVE, "a")
    assert table.slots_in(SlotState.DRAINING) == []


def test_address_change_drains_old_slot_and_uses_a_new_one():
    table = ServerSlotTable(4)
    table.assign([_server("a", 1)])

    result = table.assign([_server("a", 9)])

    assert [slot.name for slot in result.deactivated] == [slot_name(1)]
    assert [slot.name for slot in result.activated] == [slot_name(2)]
    assert table.slots[1].server.port == 9


def test_updated_replica_metadata_is_kept_for_active_slot():
    table = ServerSlotTable(1)
    table.assign([SlotServer(name="a", host="10.0.0.1", port=1)])

    table.assign([SlotServer(name="a", host="10.0.0.1", port=1, replica_id="rid")])

    assert table.slots[0].server.replica_id == "rid"


def test_grow_keeps_existing_assignments():
    table = ServerSlotTable(2)
    table.assign([_server("a", 1), _server("b", 2)])
    overflow = table.assign([_server("a", 1), _server("b", 2), _server("c", 3)])
    assert overflow.unplaced == [_server("c", 3)]
    assert table.occupied_count == 2

    table.grow(4)
    placed = table.assign([_server("a", 1), _server("b", 2), _server("c", 3)])

    assert table.capacity == 4
    assert [slot.name for slot in placed.activated] == [slot_name(3)]
    assert _states(table)[slot_name(1)] == (SlotState.ACTIVE, "a")
    assert _states(table)[slot_name(2)] == (SlotState.ACTIVE, "b")


def test_replica_lookups_only_cover_active_slots():
    table = ServerSlotTable(3)
    table.assign(
        [
            _server("a", 1),
            _server("b", 2),
            SlotServer(name="no_id", host="10.0.0.1", port=3),
        ]
    )
    table.assign([_server("b", 2), SlotServer(name="no_id", host="10.0.0.1", port=3)])

    # "a" is draining; "no_id" has no replica ID to pin on.
    assert table.replica_name_for_slot(slot_name(1)) is None
    assert table.replica_name_for_slot(slot_name(2)) == "b"
    assert table.replica_map_entries() == {
        replica_map_key("SERVE_REPLICA::app#b"): slot_name(2)
    }


@pytest.mark.parametrize(
    "name",
    [
        "SERVE_REPLICA::app#dep#abc",
        "SERVE_PROXY_ACTOR-node/1",
        "app with spaces#dep",
        "ünïcode#dep",
        "a::b??c",
    ],
)
def test_safe_name_matches_manager_server_names(name):
    safe = get_safe_name(name)
    assert HAProxyManager.get_safe_name(name) == safe
    assert replica_map_key(name) == safe
    assert all(c.isascii() and (c.isalnum() or c in "._-") for c in safe)


_SERVERS_STATE = """1
# be_id be_name srv_id srv_name srv_addr srv_op_state srv_admin_state srv_uweight srv_iweight srv_time_since_last_change srv_check_status srv_check_result srv_check_health srv_check_state srv_agent_state bk_f_forced_id srv_f_forced_id srv_fqdn srv_port srvrecord srv_use_ssl srv_check_port srv_check_addr srv_agent_addr srv_agent_port
3 http-app 1 SERVE_REPLICA_SLOT_1 10.0.0.5 2 0 1 1 12 6 3 4 6 0 0 0 - 8001 - 0 8001 - - 0
3 http-app 2 SERVE_REPLICA_SLOT_2 127.0.0.1 0 5 1 1 12 1 0 0 14 0 0 0 - 1 - 0 0 - - 0
4 http-app-via-ingress-request-router 1 SERVE_REPLICA_SLOT_1 10.0.0.5 2 2 1 1 3 1 0 0 0 0 0 0 - 8001 - 0 0 - - 0
"""


def test_parse_servers_state():
    states = parse_servers_state(_SERVERS_STATE)

    active = states[("http-app", "SERVE_REPLICA_SLOT_1")]
    assert (active.addr, active.port, active.check_port) == ("10.0.0.5", 8001, 8001)
    assert not active.in_maintenance

    free = states[("http-app", "SERVE_REPLICA_SLOT_2")]
    assert (free.addr, free.port) == ("127.0.0.1", 1)
    assert free.in_maintenance

    tracking = states[("http-app-via-ingress-request-router", "SERVE_REPLICA_SLOT_1")]
    assert tracking.in_maintenance  # Inherited maintenance (IMAINT).

    assert parse_servers_state("") == {}
    assert parse_servers_state("1\n") == {}


@pytest.mark.parametrize(
    "admin_state,expected",
    [
        (0, False),
        (SRV_ADMF_FMAINT, True),
        (SRV_ADMF_IMAINT, True),
        (SRV_ADMF_RMAINT, True),
        (0x04, False),  # Config maintenance alone, after `state ready`.
        (0x08, False),  # Forced drain is not maintenance.
    ],
)
def test_runtime_state_in_maintenance(admin_state, expected):
    assert _runtime(admin_state=admin_state).in_maintenance is expected


_SHOW_STAT = """# pxname,svname,qcur,scur,status,used_conn_cur,idle_conn_cur,safe_conn_cur
http-app,SERVE_REPLICA_SLOT_1,0,0,MAINT,0,0,0
http-app,SERVE_REPLICA_SLOT_2,0,1,MAINT,1,0,0
http-app,SERVE_REPLICA_SLOT_3,0,0,MAINT,0,1,0
http-app,SERVE_REPLICA_SLOT_4,2,0,MAINT,0,0,0
http-app,BACKEND,2,1,UP,0,0,0
http-app-via-ingress-request-router,SERVE_REPLICA_SLOT_1,0,1,MAINT,0,0,0
other,SERVE_REPLICA_SLOT_5,0,0,MAINT,0,0,0
"""


def test_parse_server_idleness_requires_no_sessions_queue_or_connections():
    idleness = parse_server_idleness(_SHOW_STAT)

    assert idleness[("http-app", "SERVE_REPLICA_SLOT_1")] is True
    assert idleness[("http-app", "SERVE_REPLICA_SLOT_2")] is False  # Session.
    assert idleness[("http-app", "SERVE_REPLICA_SLOT_3")] is False  # Pooled conn.
    assert idleness[("http-app", "SERVE_REPLICA_SLOT_4")] is False  # Queued.
    assert ("http-app", "BACKEND") not in idleness
    assert parse_server_idleness("") == {}


def test_parse_server_idleness_tolerates_missing_connection_columns():
    stats = "# pxname,svname,qcur,scur\nbe,SERVE_REPLICA_SLOT_1,0,0\n"
    assert parse_server_idleness(stats) == {("be", "SERVE_REPLICA_SLOT_1"): True}


def test_idle_slot_names_considers_tracking_backend():
    idleness = parse_server_idleness(_SHOW_STAT)

    assert idle_slot_names(idleness, "http-app") == {"SERVE_REPLICA_SLOT_1"}
    # Slot 1 still carries a pinned session in the tracking backend.
    assert (
        idle_slot_names(idleness, "http-app", "http-app-via-ingress-request-router")
        == set()
    )
    # A tracking backend absent from the running config holds no traffic.
    assert idle_slot_names(idleness, "http-app", "not-rendered-yet") == {
        "SERVE_REPLICA_SLOT_1"
    }


def test_parse_map_entries():
    output = (
        "0x55e5c8a0b2c0 SERVE_REPLICA_app-dep-a SERVE_REPLICA_SLOT_1\n"
        "0x55e5c8a0b320 SERVE_REPLICA_app-dep-b SERVE_REPLICA_SLOT_2\n"
    )
    assert parse_map_entries(output) == {
        "SERVE_REPLICA_app-dep-a": "SERVE_REPLICA_SLOT_1",
        "SERVE_REPLICA_app-dep-b": "SERVE_REPLICA_SLOT_2",
    }
    assert (
        parse_map_entries("Unknown map identifier. Please use #<id> or <file>.\n") == {}
    )


def _active_slot(host: str = "10.0.0.5", port: int = 8001) -> ServerSlot:
    return ServerSlot(
        index=1,
        state=SlotState.ACTIVE,
        server=SlotServer(name="r", host=host, port=port),
    )


def test_replica_map_entries_limited_to_existing_slots():
    table = ServerSlotTable(2)
    table.assign([_server("a", 1), _server("b", 2)])

    assert table.replica_map_entries(max_index=1) == {
        replica_map_key("SERVE_REPLICA::app#a"): slot_name(1)
    }
    assert table.replica_map_entries(max_index=0) == {}
    assert len(table.replica_map_entries()) == 2


@pytest.mark.parametrize(
    "runtime,expected",
    [
        (_runtime(addr="10.0.0.5", port=8001, admin_state=0), True),
        (_runtime(addr="10.0.0.5", port=8001, admin_state=SRV_ADMF_FMAINT), False),
        (_runtime(addr="10.0.0.9", port=8001, admin_state=0), False),
        (_runtime(addr="10.0.0.5", port=9999, admin_state=0), False),
        (_runtime(), False),
    ],
)
def test_runtime_serves_slot(runtime, expected):
    assert runtime_serves_slot(_active_slot(), runtime) is expected
    assert not runtime_serves_slot(ServerSlot(index=1), runtime)


def test_sync_commands_activate_slot_from_placeholder():
    commands = server_sync_commands(
        "be", _active_slot(), _runtime(), has_health_check=True
    )
    assert commands == [
        "set server be/SERVE_REPLICA_SLOT_1 addr 10.0.0.5 port 8001",
        "set server be/SERVE_REPLICA_SLOT_1 check-port 8001",
        "set server be/SERVE_REPLICA_SLOT_1 state ready",
        "set server be/SERVE_REPLICA_SLOT_1 health up",
    ]


def test_sync_commands_activate_tracking_slot_without_health_commands():
    commands = server_sync_commands(
        "be-via-ingress-request-router",
        _active_slot(),
        _runtime(),
        has_health_check=False,
    )
    assert commands == [
        "set server be-via-ingress-request-router/SERVE_REPLICA_SLOT_1 "
        "addr 10.0.0.5 port 8001",
        "set server be-via-ingress-request-router/SERVE_REPLICA_SLOT_1 state ready",
    ]


def test_sync_commands_are_empty_for_matching_active_slot():
    # A running slot that is DOWN is left to its health checks.
    runtime = _runtime(addr="10.0.0.5", port=8001, admin_state=0x04, check_port=8001)
    assert (
        server_sync_commands("be", _active_slot(), runtime, has_health_check=True) == []
    )


def test_sync_commands_fix_address_drift_without_touching_state():
    runtime = _runtime(addr="10.0.0.9", port=8001, admin_state=0, check_port=8001)
    assert server_sync_commands(
        "be", _active_slot(), runtime, has_health_check=True
    ) == ["set server be/SERVE_REPLICA_SLOT_1 addr 10.0.0.5 port 8001"]


def test_sync_commands_normalize_ipv6_addresses():
    runtime = _runtime(
        addr="0:0:0:0:0:0:0:1", port=8001, admin_state=0, check_port=8001
    )
    slot = _active_slot(host="::1")
    assert server_sync_commands("be", slot, runtime, has_health_check=True) == []


@pytest.mark.parametrize("state", [SlotState.DRAINING, SlotState.FREE])
def test_sync_commands_put_inactive_slot_in_maintenance(state):
    slot = ServerSlot(index=1, state=state)
    serving = _runtime(addr="10.0.0.5", port=8001, admin_state=0)

    assert server_sync_commands("be", slot, serving, has_health_check=True) == [
        "set server be/SERVE_REPLICA_SLOT_1 state maint"
    ]
    assert server_sync_commands("be", slot, _runtime(), has_health_check=True) == []


def test_map_sync_commands():
    removals, additions = map_sync_commands(
        "/tmp/be.map",
        current={"gone": "S1", "moved": "S2", "same": "S3"},
        desired={"moved": "S4", "same": "S3", "new": "S5"},
    )
    assert removals == ["del map /tmp/be.map gone", "del map /tmp/be.map moved"]
    assert additions == ["add map /tmp/be.map moved S4", "add map /tmp/be.map new S5"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
