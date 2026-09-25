"""Building blocks for runtime-managed HAProxy backend membership.

The goal (#66056) is for each backend to render a fixed number of placeholder
servers (`server-template`) instead of one `server` line per replica, so
replica membership is not part of haproxy.cfg. Replicas are then assigned to
those slots through the Runtime API (admin socket), and a membership change
never reloads HAProxy: there is no soft-stopping worker holding frontend
keep-alive connections (and a frozen backend view) until hard-stop-after.

A slot moves through three states:

    FREE ──assign──> ACTIVE ──replica removed──> DRAINING ──confirmed idle──> FREE

A DRAINING slot is in maintenance, so HAProxy sends it no new requests, but it
is only handed to another replica after HAProxy reports it idle. Otherwise an
existing session or pooled connection could be sent to the new address.

This module is pure bookkeeping plus Runtime API parsing and command
formatting, with no socket I/O; HAProxyApi is wired to it in a follow-up.
"""

import csv
import io
import ipaddress
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Container, Dict, List, Optional, Sequence, Set, Tuple

# Slot servers are named `<prefix><n>` with n in [1, capacity], matching the
# numbering `server-template <prefix> <capacity>` generates. The prefix keeps the
# `SERVE_REPLICA` stem so name-based "is this a replica server" checks still
# match slot servers.
SLOT_NAME_PREFIX = "SERVE_REPLICA_SLOT_"

# Address rendered for every slot. Slots start in maintenance, so HAProxy never
# health-checks or routes to it; if one were ever enabled by mistake, port 1 is
# refused immediately and `retry-on conn-failure` redispatches the request.
SLOT_PLACEHOLDER_HOST = "127.0.0.1"
SLOT_PLACEHOLDER_PORT = 1

# Administrative state bits reported by `show servers state` (see HAProxy's
# management guide). A server is in maintenance if it was forced into it (FMAINT,
# e.g. `disabled` or `set server ... state maint`), inherited it from a tracked
# server (IMAINT), or has an unresolvable address (RMAINT).
SRV_ADMF_FMAINT = 0x01
SRV_ADMF_IMAINT = 0x02
SRV_ADMF_RMAINT = 0x20
SRV_ADMF_MAINT = SRV_ADMF_FMAINT | SRV_ADMF_IMAINT | SRV_ADMF_RMAINT

_SLOT_NAME_RE = re.compile(rf"^{re.escape(SLOT_NAME_PREFIX)}([1-9][0-9]*)$")

# `show stat` columns that must all be zero before a draining slot is reused:
# live sessions, queued requests, and connections HAProxy still holds to the
# old address (in use, or idle in the reuse pool).
_IDLE_STAT_COLUMNS = (
    "scur",
    "qcur",
    "used_conn_cur",
    "idle_conn_cur",
    "safe_conn_cur",
)


def get_safe_name(name: str) -> str:
    """Get a safe label name for the haproxy config."""
    name = name.replace("#", "-").replace("/", ".")
    # replace all remaining non-alphanumeric and non-{".", "_", "-"} with "_"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def slot_name(index: int) -> str:
    """Server name of the 1-based slot `index`."""
    return f"{SLOT_NAME_PREFIX}{index}"


def is_slot_name(name: str) -> bool:
    return _SLOT_NAME_RE.match(name) is not None


def required_slot_capacity(needed: int, minimum: int) -> int:
    """Smallest power-of-two multiple of `minimum` that fits `needed` servers.

    Doubling keeps capacity-driven reloads logarithmic in the peak replica count.
    """
    capacity = max(minimum, 1)
    while capacity < needed:
        capacity *= 2
    return capacity


def replica_map_key(replica_id: str) -> str:
    """Key for a replica in an ingress-request-router map file.

    Map keys cannot contain whitespace, so the unsanitized actor name returned
    by /internal/route is sanitized the same way server names are. The Lua
    router applies the identical transformation before the map lookup.
    """
    return get_safe_name(replica_id)


class SlotState(str, Enum):
    FREE = "FREE"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"


@dataclass(frozen=True)
class SlotServer:
    """The replica address a slot serves (or last served, while draining)."""

    # Replica server name (the same name static configs render).
    name: str
    host: str
    port: int
    # Unsanitized replica actor name, as returned by /internal/route.
    replica_id: Optional[str] = None

    @property
    def address(self) -> Tuple[str, int]:
        return (self.host, self.port)


@dataclass
class ServerSlot:
    index: int
    state: SlotState = SlotState.FREE
    server: Optional[SlotServer] = None

    @property
    def name(self) -> str:
        return slot_name(self.index)


@dataclass
class SlotAssignment:
    """Result of reconciling a slot table against the desired replicas."""

    activated: List[ServerSlot] = field(default_factory=list)
    deactivated: List[ServerSlot] = field(default_factory=list)
    released: List[ServerSlot] = field(default_factory=list)
    # Desired replicas that did not fit: no FREE slot was available.
    unplaced: List[SlotServer] = field(default_factory=list)


class ServerSlotTable:
    """Slot assignments for one HAProxy backend."""

    def __init__(self, capacity: int):
        if capacity < 1:
            raise ValueError(f"Slot capacity must be positive, got {capacity}.")
        self._slots: List[ServerSlot] = [
            ServerSlot(index=i) for i in range(1, capacity + 1)
        ]

    @property
    def capacity(self) -> int:
        return len(self._slots)

    @property
    def slots(self) -> Sequence[ServerSlot]:
        return self._slots

    def slots_in(self, state: SlotState) -> List[ServerSlot]:
        return [slot for slot in self._slots if slot.state is state]

    @property
    def occupied_count(self) -> int:
        """Slots that cannot be handed out: ACTIVE plus DRAINING."""
        return sum(1 for slot in self._slots if slot.state is not SlotState.FREE)

    def grow(self, capacity: int) -> None:
        """Append FREE slots up to `capacity`. Existing slots keep their names,
        so the running HAProxy's assignments stay valid across the reload."""
        for index in range(self.capacity + 1, capacity + 1):
            self._slots.append(ServerSlot(index=index))

    def slot_by_name(self, name: str) -> Optional[ServerSlot]:
        match = _SLOT_NAME_RE.match(name)
        if match is None:
            return None
        index = int(match.group(1))
        if index > self.capacity:
            return None
        return self._slots[index - 1]

    def assign(
        self,
        desired: Sequence[SlotServer],
        idle_slot_names: Container[str] = frozenset(),
    ) -> SlotAssignment:
        """Reconcile slots with `desired`, mutating the table.

        Args:
            desired: Replicas that should be routable. Names must be unique.
            idle_slot_names: Slots HAProxy reported idle. Only slots that were
                already DRAINING before this call are released, because the idle
                sample must postdate the slot entering maintenance.

        Returns:
            The slots whose state changed and any replicas left unplaced.
        """
        result = SlotAssignment()
        desired_by_name = {server.name: server for server in desired}

        previously_draining = {
            slot.index for slot in self._slots if slot.state is SlotState.DRAINING
        }

        for slot in self._slots:
            if slot.state is not SlotState.ACTIVE:
                continue
            assert slot.server is not None
            wanted = desired_by_name.get(slot.server.name)
            if wanted is not None and wanted.address == slot.server.address:
                slot.server = wanted
                continue
            # Removed, or the same name moved to a new address. In both cases
            # sessions on the old address must drain before the slot is reused.
            slot.state = SlotState.DRAINING
            result.deactivated.append(slot)

        for slot in self._slots:
            if slot.index in previously_draining and slot.name in idle_slot_names:
                slot.state = SlotState.FREE
                slot.server = None
                result.released.append(slot)

        active_names = {
            slot.server.name
            for slot in self._slots
            if slot.state is SlotState.ACTIVE and slot.server is not None
        }
        for server in sorted(desired, key=lambda s: s.name):
            if server.name in active_names:
                continue
            target = self._find_slot_for(server)
            if target is None:
                result.unplaced.append(server)
                continue
            target.state = SlotState.ACTIVE
            target.server = server
            active_names.add(server.name)
            result.activated.append(target)

        return result

    def _find_slot_for(self, server: SlotServer) -> Optional[ServerSlot]:
        # A draining slot that still points at this exact replica address can be
        # re-activated in place: any sessions on it belong to the same replica.
        for slot in self._slots:
            if (
                slot.state is SlotState.DRAINING
                and slot.server is not None
                and slot.server.name == server.name
                and slot.server.address == server.address
            ):
                return slot
        for slot in self._slots:
            if slot.state is SlotState.FREE:
                return slot
        return None

    def replica_name_for_slot(self, name: str) -> Optional[str]:
        """Replica server name served by an ACTIVE slot, else None."""
        slot = self.slot_by_name(name)
        if slot is None or slot.state is not SlotState.ACTIVE:
            return None
        assert slot.server is not None
        return slot.server.name

    def replica_map_entries(self, max_index: Optional[int] = None) -> Dict[str, str]:
        """Map entries for ACTIVE slots whose replica has an ID.

        Args:
            max_index: If set, only slots numbered up to this index are included.

        Returns:
            {replica map key: slot name}.
        """
        return {
            replica_map_key(slot.server.replica_id): slot.name
            for slot in self._slots
            if slot.state is SlotState.ACTIVE
            and slot.server is not None
            and slot.server.replica_id is not None
            and (max_index is None or slot.index <= max_index)
        }


@dataclass(frozen=True)
class RuntimeServerState:
    """One server row of `show servers state`."""

    backend: str
    server: str
    addr: str
    port: Optional[int]
    admin_state: int
    check_port: Optional[int]

    @property
    def in_maintenance(self) -> bool:
        return bool(self.admin_state & SRV_ADMF_MAINT)


def _optional_int(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def parse_servers_state(output: str) -> Dict[Tuple[str, str], RuntimeServerState]:
    """Parse `show servers state` into {(backend, server): state}.

    The dump is a version line, a `# `-prefixed header naming the columns, and
    one space-separated row per server. Columns are located by name so the
    parser tolerates columns added by newer HAProxy versions.
    """
    columns: Optional[List[str]] = None
    states: Dict[Tuple[str, str], RuntimeServerState] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            columns = line.lstrip("#").split()
            continue
        if columns is None:
            # Version line.
            continue
        row = dict(zip(columns, line.split()))
        backend = row.get("be_name")
        server = row.get("srv_name")
        if not backend or not server:
            continue
        states[(backend, server)] = RuntimeServerState(
            backend=backend,
            server=server,
            addr=row.get("srv_addr", ""),
            port=_optional_int(row.get("srv_port")),
            admin_state=_optional_int(row.get("srv_admin_state")) or 0,
            check_port=_optional_int(row.get("srv_check_port")),
        )
    return states


def parse_server_idleness(stats_output: str) -> Dict[Tuple[str, str], bool]:
    """{(backend, server): idle} for each server row of `show stat`.

    A server is idle when it has no sessions, queued requests, or connections
    left to its current address.
    """
    if not stats_output or not stats_output.strip():
        return {}
    idleness: Dict[Tuple[str, str], bool] = {}
    reader = csv.DictReader(io.StringIO(stats_output.replace("# ", "", 1)))
    for row in reader:
        backend = (row.get("pxname") or "").strip()
        server = (row.get("svname") or "").strip()
        if not backend or not server or server in ("BACKEND", "FRONTEND"):
            continue
        idleness[(backend, server)] = all(
            (_optional_int((row.get(column) or "").strip() or "0") or 0) == 0
            for column in _IDLE_STAT_COLUMNS
        )
    return idleness


def idle_slot_names(
    idleness: Dict[Tuple[str, str], bool],
    backend: str,
    tracking_backend: Optional[str] = None,
) -> Set[str]:
    """Slots of `backend` that are idle, including in their tracking backend.

    A slot missing from the primary backend's stats is not considered idle,
    since HAProxy could not confirm it. A missing tracking row means the tracking
    backend is not in the running config yet, so it cannot hold traffic.
    """
    idle = set()
    for (be, server), is_idle in idleness.items():
        if be != backend or not is_idle or not is_slot_name(server):
            continue
        if tracking_backend is not None and not idleness.get(
            (tracking_backend, server), True
        ):
            continue
        idle.add(server)
    return idle


def parse_map_entries(output: str) -> Dict[str, str]:
    """Parse `show map <file>` rows (`<ref id> <key> <value>`) into {key: value}."""
    entries: Dict[str, str] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0].startswith("0x"):
            entries.setdefault(parts[1], parts[2])
    return entries


def _same_address(actual: str, expected: str) -> bool:
    try:
        return ipaddress.ip_address(actual) == ipaddress.ip_address(expected)
    except ValueError:
        return actual == expected


def runtime_serves_slot(slot: ServerSlot, runtime: RuntimeServerState) -> bool:
    """Whether HAProxy is serving the slot's assigned replica: the slot is
    ACTIVE, out of maintenance, and pointed at the replica's address."""
    if slot.state is not SlotState.ACTIVE or slot.server is None:
        return False
    host, port = slot.server.address
    return (
        not runtime.in_maintenance
        and _same_address(runtime.addr, host)
        and runtime.port == port
    )


def server_sync_commands(
    backend: str,
    slot: ServerSlot,
    runtime: RuntimeServerState,
    *,
    has_health_check: bool,
) -> List[str]:
    """Runtime API commands that move `runtime` to the slot's desired state.

    Returns no commands when the running server already matches, so repeated
    syncs are cheap and an up/down health state is never overridden.
    """
    target = f"{backend}/{slot.name}"
    if slot.state is not SlotState.ACTIVE:
        return [] if runtime.in_maintenance else [f"set server {target} state maint"]

    assert slot.server is not None
    host, port = slot.server.address
    commands = []
    if not _same_address(runtime.addr, host) or runtime.port != port:
        commands.append(f"set server {target} addr {host} port {port}")
    if has_health_check and runtime.check_port != port:
        commands.append(f"set server {target} check-port {port}")
    if runtime.in_maintenance:
        commands.append(f"set server {target} state ready")
        if has_health_check:
            # Leaving maintenance restarts checks from DOWN. Mark the server UP
            # so it takes traffic immediately, like a server loaded from config;
            # a failing check still takes it down after `fall` attempts.
            commands.append(f"set server {target} health up")
    return commands


def map_sync_commands(
    map_path: str, current: Dict[str, str], desired: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """(removals, additions) that turn map `current` into `desired`.

    Removals run before server changes and additions after, so the router can
    only resolve a replica to a slot that is ready.
    """
    removals = [
        f"del map {map_path} {key}"
        for key, value in sorted(current.items())
        if desired.get(key) != value
    ]
    additions = [
        f"add map {map_path} {key} {value}"
        for key, value in sorted(desired.items())
        if current.get(key) != value
    ]
    return removals, additions
