"""HAProxyApi dynamic-server behavior against a fake HAProxy Runtime API.

These tests cover the decisions HAProxyApi makes (when to reload, which Runtime
API commands to send, how stats are translated) without an HAProxy binary. The
real HAProxy behavior is covered by test_haproxy_membership_and_reload_e2e.py.
"""
import asyncio
import re
import sys
from typing import Dict, List, Tuple
from unittest import mock

import pytest

from ray.serve._private.common import RequestProtocol
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    HAProxyManager,
    ServerConfig,
)
from ray.serve._private.haproxy_server_slots import (
    SRV_ADMF_FMAINT,
    replica_map_key,
    slot_name,
)
from ray.serve.config import HTTPOptions, gRPCOptions

_ROUTER_SUFFIX = "-via-ingress-request-router"


class FakeRuntimeApi:
    """In-memory stand-in for HAProxy's admin socket.

    `load_config` mimics a (re)started HAProxy: servers come from the rendered
    haproxy.cfg and maps from their files. With `restore_state`, servers keep
    the address and admin state they had, like `load-server-state-from-file`.
    """

    def __init__(self):
        # (backend, server) -> {"addr", "port", "admin", "check_port"}
        self.servers: Dict[Tuple[str, str], dict] = {}
        self.maps: Dict[str, Dict[str, str]] = {}
        # (backend, server) -> scur, for draining tests.
        self.sessions: Dict[Tuple[str, str], int] = {}
        self.commands: List[str] = []
        self.ignore_writes = False

    def load_config(self, api: HAProxyApi, restore_state: bool = False) -> None:
        previous = self.servers
        self.servers = {}
        backend = None
        with open(api.config_file_path) as f:
            for line in f:
                line = line.strip()
                if match := re.match(r"^backend (\S+)", line):
                    backend = match.group(1)
                elif match := re.match(
                    r"^server-template (\S+) (\d+) (\S+):(\d+) (.*)$", line
                ):
                    prefix, count, host, port, params = match.groups()
                    for i in range(1, int(count) + 1):
                        self._add(backend, f"{prefix}{i}", host, int(port), params)
                elif match := re.match(r"^server (\S+) (\S+):(\d+)(.*)$", line):
                    name, host, port, params = match.groups()
                    self._add(backend, name, host, int(port), params)
                for path in re.findall(r"map\((\S+?)\)", line):
                    with open(path) as map_file:
                        self.maps[path] = dict(
                            entry.split() for entry in map_file if entry.strip()
                        )
        if restore_state:
            for key, server in self.servers.items():
                if key in previous:
                    server.update(previous[key])

    def _add(self, backend, name, host, port, params):
        self.servers[(backend, name)] = {
            "addr": host,
            "port": port,
            "admin": SRV_ADMF_FMAINT if "disabled" in params.split() else 0,
            "check_port": 0,
        }

    def slot(self, backend: str, index: int) -> dict:
        return self.servers[(backend, slot_name(index))]

    async def send(self, command: str) -> str:
        return "".join(self._one(part.strip()) for part in command.split(";"))

    def _one(self, command: str) -> str:
        self.commands.append(command)
        if command == "show servers state":
            header = (
                "1\n# be_id be_name srv_id srv_name srv_addr srv_op_state "
                "srv_admin_state srv_port srv_check_port\n"
            )
            rows = "".join(
                f"1 {be} 1 {name} {s['addr']} 2 {s['admin']} {s['port']} "
                f"{s['check_port']}\n"
                for (be, name), s in self.servers.items()
            )
            return header + rows
        if command == "show stat":
            rows = "".join(
                f"{be},{name},0,{self.sessions.get((be, name), 0)},UP,0,0,0\n"
                for (be, name) in self.servers
            )
            return (
                "# pxname,svname,qcur,scur,status,used_conn_cur,idle_conn_cur,"
                "safe_conn_cur\n" + rows
            )
        if command.startswith("show map "):
            path = command[len("show map ") :]
            if path not in self.maps:
                return "Unknown map identifier.\n"
            return "".join(f"0x1 {k} {v}\n" for k, v in self.maps[path].items())
        if self.ignore_writes:
            return ""

        if match := re.match(r"^set server (\S+)/(\S+) (.*)$", command):
            server = self.servers[(match.group(1), match.group(2))]
            args = match.group(3).split()
            if args[0] == "addr":
                server["addr"], server["port"] = args[1], int(args[3])
            elif args[0] == "check-port":
                server["check_port"] = int(args[1])
            elif args[:2] == ["state", "ready"]:
                server["admin"] &= ~SRV_ADMF_FMAINT
            elif args[:2] == ["state", "maint"]:
                server["admin"] |= SRV_ADMF_FMAINT
            return ""
        if match := re.match(r"^add map (\S+) (\S+) (\S+)$", command):
            self.maps[match.group(1)][match.group(2)] = match.group(3)
            return ""
        if match := re.match(r"^del map (\S+) (\S+)$", command):
            self.maps[match.group(1)].pop(match.group(2), None)
            return ""
        raise AssertionError(f"Unexpected command: {command}")

    def writes(self) -> List[str]:
        return [c for c in self.commands if not c.startswith("show ")]


def _replica(replica_id: str, port: int, host: str = "10.0.0.2") -> ServerConfig:
    """A replica server named the way HAProxyManager names it."""
    return ServerConfig(
        name=HAProxyManager.get_safe_name(replica_id),
        host=host,
        port=port,
        replica_id=replica_id,
    )


def _backend(
    replicas: List[ServerConfig],
    *,
    name: str = "http-app",
    path_prefix: str = "/app",
    router: bool = False,
    protocol: RequestProtocol = RequestProtocol.HTTP,
) -> BackendConfig:
    return BackendConfig(
        name=name,
        path_prefix=path_prefix,
        app_name=name,
        servers=list(replicas),
        ingress_request_router_servers=(
            [ServerConfig(name="router", host="10.0.0.9", port=9100)] if router else []
        ),
        fallback_server=ServerConfig(name="fallback", host="10.0.0.1", port=8500),
        protocol=protocol,
    )


class Harness:
    def __init__(self, tmp_path, dynamic: bool = True, min_slots: int = 2):
        self.runtime = FakeRuntimeApi()
        self.reloads = 0
        self.restore_state_on_reload = True
        self.api = HAProxyApi(
            cfg=HAProxyConfig(
                http_options=HTTPOptions(host="127.0.0.1", port=8000),
                socket_path=str(tmp_path / "admin.sock"),
                server_state_file=str(tmp_path / "server-state"),
                dynamic_servers_enabled=dynamic,
                min_server_slots=min_slots,
                has_received_routes=True,
                has_received_servers=True,
            ),
            config_file_path=str(tmp_path / "haproxy.cfg"),
        )
        self.api._send_socket_command = self.runtime.send
        self.api._graceful_reload = self._fake_reload
        # A running HAProxy, so reloads sync it before snapshotting its state.
        self.api._proc = mock.Mock(returncode=None)
        # Server state of the running process at each reload (the snapshot the
        # new process loads).
        self.reload_snapshots: List[Dict[Tuple[str, str], dict]] = []
        # Router maps as each (re)started process loaded them from the files.
        self.maps_at_load: List[Dict[str, Dict[str, str]]] = []

    def _load(self, restore_state: bool) -> None:
        self.runtime.load_config(self.api, restore_state=restore_state)
        self.maps_at_load.append(
            {path: dict(entries) for path, entries in self.runtime.maps.items()}
        )
        self.api._mark_rendered_config_running()

    async def _fake_reload(self):
        self.reloads += 1
        self.reload_snapshots.append(
            {key: dict(server) for key, server in self.runtime.servers.items()}
        )
        self._load(restore_state=self.restore_state_on_reload)

    async def start(self, backends: List[BackendConfig]):
        """Mirror HAProxyApi.start() without spawning HAProxy."""
        self.set(backends)
        self.api._generate_config_file_internal()
        self._load(restore_state=False)
        await self.api._sync_runtime_servers()

    def set(self, backends: List[BackendConfig]):
        self.api.set_backend_configs({b.name: b for b in backends})

    async def apply(self, backends: List[BackendConfig]):
        self.runtime.commands.clear()
        self.set(backends)
        await self.api.apply()


def _serving(server: dict, host: str, port: int) -> bool:
    return (server["addr"], server["port"], server["check_port"]) == (
        host,
        port,
        port,
    ) and not server["admin"] & SRV_ADMF_FMAINT


@pytest.mark.asyncio
async def test_membership_changes_use_runtime_api_without_reload(tmp_path):
    h = Harness(tmp_path, min_slots=4)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([a])])
    assert _serving(h.runtime.slot("http-app", 1), "10.0.0.2", 8001)

    # Scale up.
    await h.apply([_backend([a, b])])
    assert h.reloads == 0
    assert _serving(h.runtime.slot("http-app", 2), "10.0.0.2", 8002)
    assert h.runtime.writes() == [
        "set server http-app/SERVE_REPLICA_SLOT_2 addr 10.0.0.2 port 8002",
        "set server http-app/SERVE_REPLICA_SLOT_2 check-port 8002",
        "set server http-app/SERVE_REPLICA_SLOT_2 state ready",
        "set server http-app/SERVE_REPLICA_SLOT_2 health up",
    ]

    # Scale down: the removed replica's slot enters maintenance immediately.
    await h.apply([_backend([b])])
    assert h.reloads == 0
    assert h.runtime.slot("http-app", 1)["admin"] & SRV_ADMF_FMAINT
    assert h.runtime.writes() == [
        "set server http-app/SERVE_REPLICA_SLOT_1 state maint"
    ]

    # A no-op update sends nothing.
    await h.apply([_backend([b])])
    assert h.reloads == 0
    assert h.runtime.writes() == []


@pytest.mark.asyncio
async def test_config_change_reloads_and_keeps_assignments(tmp_path):
    h = Harness(tmp_path)
    a = _replica("SERVE_REPLICA::app#d#a", 8001)
    await h.start([_backend([a])])

    other = _backend([], name="http-other", path_prefix="/other")
    await h.apply([_backend([a]), other])

    assert h.reloads == 1
    assert _serving(h.runtime.slot("http-app", 1), "10.0.0.2", 8001)
    # The state file restored the assignment, so nothing had to be re-applied.
    assert h.runtime.writes() == []


@pytest.mark.asyncio
async def test_reload_snapshot_already_has_membership_changes(tmp_path):
    """Membership changes that arrive with a config change reach the running
    process before the reload, so the state the new process loads never
    resurrects a removed replica or disables a new one."""
    h = Harness(tmp_path, min_slots=4)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([a])])

    # Remove A, add B, and change the route in one update.
    await h.apply([_backend([b], path_prefix="/moved")])

    assert h.reloads == 1
    snapshot = h.reload_snapshots[0]
    assert snapshot[("http-app", slot_name(1))]["admin"] & SRV_ADMF_FMAINT
    assert _serving(snapshot[("http-app", slot_name(2))], "10.0.0.2", 8002)
    # Nothing was left for the post-reload sync to fix.
    assert h.runtime.writes()[-1].startswith("set server http-app/SERVE_REPLICA_SLOT_2")


@pytest.mark.asyncio
async def test_capacity_growth_syncs_existing_slots_before_reload(tmp_path):
    h = Harness(tmp_path, min_slots=1)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([a])])

    # Slot 2 does not exist in the running process until the reload.
    await h.apply([_backend([b])])

    assert h.reloads == 1
    snapshot = h.reload_snapshots[0]
    assert ("http-app", slot_name(2)) not in snapshot
    assert snapshot[("http-app", slot_name(1))]["admin"] & SRV_ADMF_FMAINT
    assert _serving(h.runtime.slot("http-app", 2), "10.0.0.2", 8002)


@pytest.mark.asyncio
async def test_capacity_growth_does_not_pin_to_slots_before_they_exist(tmp_path):
    h = Harness(tmp_path, min_slots=1)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([a], router=True)])
    replica_map = h.api._replica_map_path("http-app")
    maps_at_reload = []
    reload = h.api._graceful_reload

    async def _record_maps_then_reload():
        maps_at_reload.append(dict(h.runtime.maps[replica_map]))
        await reload()

    h.api._graceful_reload = _record_maps_then_reload

    # B lands in slot 2, which only exists after the capacity-growth reload.
    await h.apply([_backend([a, b], router=True)])

    assert maps_at_reload == [{replica_map_key(a.replica_id): slot_name(1)}]
    assert h.runtime.maps[replica_map] == {
        replica_map_key(a.replica_id): slot_name(1),
        replica_map_key(b.replica_id): slot_name(2),
    }


@pytest.mark.asyncio
async def test_map_files_only_seed_pins_ready_on_load(tmp_path):
    """A (re)started process must not load a pin to a slot that is still a
    disabled placeholder; such pins are added once the slot is ready."""
    h = Harness(tmp_path, min_slots=1)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    replica_map = h.api._replica_map_path("http-app")
    apps_map = h.api._router_apps_map_path()

    await h.start([_backend([a], router=True)])
    # On first start no slot is assigned yet when the files load.
    assert h.maps_at_load[0][replica_map] == {}
    assert h.maps_at_load[0][apps_map] == {}
    assert h.runtime.maps[replica_map] == {replica_map_key(a.replica_id): slot_name(1)}

    # B needs slot 2, which only exists in the reloaded process.
    await h.apply([_backend([a, b], router=True)])
    assert h.maps_at_load[1][replica_map] == {
        replica_map_key(a.replica_id): slot_name(1)
    }
    assert h.runtime.maps[replica_map] == {
        replica_map_key(a.replica_id): slot_name(1),
        replica_map_key(b.replica_id): slot_name(2),
    }


@pytest.mark.asyncio
async def test_map_files_do_not_seed_pins_without_state_file(tmp_path):
    h = Harness(tmp_path)
    h.api.cfg.enable_hap_optimization = False
    h.restore_state_on_reload = False
    a = _replica("SERVE_REPLICA::app#d#a", 8001)
    replica_map = h.api._replica_map_path("http-app")
    await h.start([_backend([a], router=True)])

    await h.apply([_backend([a], router=True, path_prefix="/moved")])

    # Every slot restarts disabled, so no pin is loaded until the sync.
    assert h.maps_at_load[1][replica_map] == {}
    assert h.maps_at_load[1][h.api._router_apps_map_path()] == {}
    assert h.runtime.maps[replica_map] == {replica_map_key(a.replica_id): slot_name(1)}


@pytest.mark.asyncio
async def test_stats_only_report_replicas_haproxy_is_serving(tmp_path):
    h = Harness(tmp_path, min_slots=4)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([a])])
    h.runtime.ignore_writes = True

    # B is assigned a slot in memory, but HAProxy never applies it.
    with pytest.raises(RuntimeError, match="did not converge"):
        await h.apply([_backend([a, b])])

    stats = await h.api.get_all_stats()
    assert set(stats["http-app"]) == {a.name, "fallback"}
    assert await h.api.compute_target_mismatch() == 1


@pytest.mark.asyncio
async def test_app_becoming_router_bearing_reloads_before_syncing_maps(tmp_path):
    h = Harness(tmp_path, min_slots=2)
    a = _replica("SERVE_REPLICA::app#d#a", 8001)
    await h.start([_backend([a])])

    # The running process has no router maps or tracking servers yet.
    await h.apply([_backend([a], router=True)])

    assert h.reloads == 1
    assert h.runtime.maps[h.api._replica_map_path("http-app")] == {
        replica_map_key(a.replica_id): slot_name(1)
    }


@pytest.mark.asyncio
async def test_reload_without_state_file_reapplies_assignments(tmp_path):
    h = Harness(tmp_path)
    h.restore_state_on_reload = False
    a = _replica("SERVE_REPLICA::app#d#a", 8001)
    await h.start([_backend([a])])

    await h.apply([_backend([a], path_prefix="/moved")])

    assert h.reloads == 1
    assert _serving(h.runtime.slot("http-app", 1), "10.0.0.2", 8001)
    assert "set server http-app/SERVE_REPLICA_SLOT_1 state ready" in h.runtime.writes()


@pytest.mark.asyncio
async def test_slot_exhaustion_grows_capacity_with_one_reload(tmp_path):
    h = Harness(tmp_path, min_slots=2)
    replicas = [_replica(f"SERVE_REPLICA::app#d#{i}", 8000 + i) for i in range(3)]
    await h.start([_backend(replicas[:2])])
    assert h.api._slot_tables["http-app"].capacity == 2

    await h.apply([_backend(replicas)])

    assert h.reloads == 1
    assert h.api._slot_tables["http-app"].capacity == 4
    with open(h.api.config_file_path) as f:
        assert "server-template SERVE_REPLICA_SLOT_ 4 127.0.0.1:1" in f.read()
    for index, replica in enumerate(replicas, start=1):
        assert _serving(h.runtime.slot("http-app", index), "10.0.0.2", replica.port)

    # Capacity does not shrink on scale-down, so that does not reload either.
    await h.apply([_backend(replicas[:1])])
    assert h.reloads == 1


@pytest.mark.asyncio
async def test_draining_slot_is_not_reused_while_it_has_sessions(tmp_path):
    h = Harness(tmp_path, min_slots=2)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    c = _replica("SERVE_REPLICA::app#d#c", 8003)
    await h.start([_backend([a, b])])

    await h.apply([_backend([b])])  # Slot 1 drains.
    h.runtime.sessions[("http-app", slot_name(1))] = 1

    # Slot 1 is busy, so "c" needs a new slot: capacity grows and HAProxy reloads.
    await h.apply([_backend([b, c])])
    assert h.reloads == 1
    assert _serving(h.runtime.slot("http-app", 3), "10.0.0.2", 8003)
    assert h.runtime.slot("http-app", 1)["port"] == 8001  # Untouched.

    h.runtime.sessions.clear()
    d = _replica("SERVE_REPLICA::app#d#d", 8004)
    await h.apply([_backend([b, c, d])])
    # Slot 1 was confirmed idle while in maintenance, so "d" reuses it.
    assert h.reloads == 1
    assert _serving(h.runtime.slot("http-app", 1), "10.0.0.2", 8004)


@pytest.mark.asyncio
async def test_sync_raises_when_runtime_does_not_converge(tmp_path):
    h = Harness(tmp_path)
    await h.start([_backend([])])
    h.runtime.ignore_writes = True

    with pytest.raises(RuntimeError, match="did not converge"):
        await h.apply([_backend([_replica("SERVE_REPLICA::app#d#a", 8001)])])


@pytest.mark.asyncio
async def test_sync_reports_slots_missing_from_running_config(tmp_path):
    h = Harness(tmp_path, min_slots=2)
    await h.start([_backend([])])
    # The running process does not have slot 2 (e.g. a failed reload).
    del h.runtime.servers[("http-app", slot_name(2))]

    with pytest.raises(RuntimeError, match="http-app/SERVE_REPLICA_SLOT_2"):
        await h.api._sync_runtime_servers()


@pytest.mark.asyncio
async def test_stats_report_replica_names_for_active_slots(tmp_path):
    h = Harness(tmp_path)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([a, b])])
    await h.apply([_backend([b])])
    h.runtime.sessions[("http-app", slot_name(1))] = 2

    stats = await h.api.get_all_stats()
    assert set(stats["http-app"]) == {b.name, "fallback"}

    # Draining sessions still count toward load for proxy draining decisions.
    haproxy_stats = await h.api.get_haproxy_stats()
    assert haproxy_stats.total_active_sessions == 2

    h.api.backend_configs["http-app"].fallback_server = None
    assert await h.api.compute_target_mismatch() == 1  # Only the fallback row.


@pytest.mark.asyncio
async def test_router_maps_follow_membership_without_reload(tmp_path):
    h = Harness(tmp_path, min_slots=4)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )
    await h.start([_backend([], router=True)])

    replica_map = h.api._replica_map_path("http-app")
    apps_map = h.api._router_apps_map_path()
    # No replicas yet: the router stays off for the app, like a static config.
    assert h.runtime.maps[apps_map] == {}

    await h.apply([_backend([a, b], router=True)])
    assert h.reloads == 0
    assert h.runtime.maps[replica_map] == {
        replica_map_key(a.replica_id): slot_name(1),
        replica_map_key(b.replica_id): slot_name(2),
    }
    assert h.runtime.maps[apps_map] == {"http-app": "1"}
    writes = h.runtime.writes()
    # Tracking servers follow their primary slot, and maps are only updated
    # after the slots they point to are ready.
    assert (
        f"set server http-app{_ROUTER_SUFFIX}/SERVE_REPLICA_SLOT_1 state ready"
        in writes
    )
    first_map_add = next(i for i, c in enumerate(writes) if c.startswith("add map"))
    assert all(not c.startswith("set server") for c in writes[first_map_add:])

    await h.apply([_backend([b], router=True)])
    writes = h.runtime.writes()
    assert h.runtime.maps[replica_map] == {replica_map_key(b.replica_id): slot_name(2)}
    # Stop resolving the replica before its slot goes into maintenance.
    assert writes.index(f"del map {replica_map} {replica_map_key(a.replica_id)}") < (
        writes.index("set server http-app/SERVE_REPLICA_SLOT_1 state maint")
    )

    await h.apply([_backend([], router=True)])
    assert h.runtime.maps[apps_map] == {}
    assert h.reloads == 0


@pytest.mark.asyncio
async def test_router_map_files_seed_reloads(tmp_path):
    h = Harness(tmp_path)
    h.restore_state_on_reload = False
    a = _replica("SERVE_REPLICA::app#d#a", 8001)
    await h.start([_backend([a], router=True)])

    await h.apply([_backend([a], router=True, path_prefix="/moved")])

    assert h.reloads == 1
    # The reloaded process loaded the current entries from the map files.
    assert h.runtime.maps[h.api._replica_map_path("http-app")] == {
        replica_map_key(a.replica_id): slot_name(1)
    }
    assert not [c for c in h.runtime.writes() if "map" in c]


@pytest.mark.asyncio
async def test_grpc_backends_are_slotted_only_when_rendered(tmp_path):
    h = Harness(tmp_path, min_slots=2)
    a = _replica("SERVE_REPLICA::app#d#a", 9001)
    grpc_backend = _backend([a], name="grpc-app", protocol=RequestProtocol.GRPC)

    # Without the gRPC frontend, gRPC backends are not rendered or synced.
    await h.start([grpc_backend])
    assert "grpc-app" not in h.api._slot_tables

    h.api.cfg.grpc_options = gRPCOptions(
        port=9000, grpc_servicer_functions=["fake.add_servicer"]
    )
    await h.start([grpc_backend])

    with open(h.api.config_file_path) as f:
        config = f.read()
    assert (
        "server-template SERVE_REPLICA_SLOT_ 2 127.0.0.1:1 proto h2 check disabled"
        in (config)
    )
    assert "10.0.0.2:9001" not in config
    assert _serving(h.runtime.slot("grpc-app", 1), "10.0.0.2", 9001)


def test_rendered_config_is_independent_of_membership(tmp_path):
    h = Harness(tmp_path, min_slots=4)
    a, b = _replica("SERVE_REPLICA::app#d#a", 8001), _replica(
        "SERVE_REPLICA::app#d#b", 8002
    )

    h.set([_backend([a], router=True)])
    h.api._generate_config_file_internal()
    first = h.api._rendered_config_fingerprint
    with open(h.api.config_file_path) as f:
        config = f.read()

    h.set([_backend([b], router=True)])
    h.api._generate_config_file_internal()

    assert h.api._rendered_config_fingerprint == first
    assert "server-template SERVE_REPLICA_SLOT_ 4 127.0.0.1:1 check disabled" in config
    assert "10.0.0.2:8001" not in config
    lua_path = tmp_path / "ingress_request_router.lua"
    lua = lua_path.read_text()
    assert "local DYNAMIC_REPLICA_TARGETS = true" in lua
    assert "SERVE_REPLICA::app" not in lua


def test_router_app_claim_does_not_depend_on_replicas(tmp_path):
    """The longest-prefix router app must claim its requests even with no
    replicas; only running its router depends on the runtime apps map."""
    h = Harness(tmp_path)
    parent = _backend(
        [_replica("SERVE_REPLICA::parent#d#a", 8001)],
        name="http-parent",
        path_prefix="/app",
        router=True,
    )
    child = _backend([], name="http-child", path_prefix="/app/child", router=True)
    h.set([parent, child])
    h.api._generate_config_file_internal()
    with open(h.api.config_file_path) as f:
        config = f.read()

    apps_map = h.api._router_apps_map_path()
    claims = [
        line.strip()
        for line in config.splitlines()
        if "set-var(txn.ingress_request_router_app)" in line
    ]
    assert claims == [
        "http-request set-var(txn.ingress_request_router_app) str(http-child) "
        "if is_http-child !{ var(txn.ingress_request_router_app) -m found }",
        "http-request set-var(txn.ingress_request_router_app) str(http-parent) "
        "if is_http-parent !{ var(txn.ingress_request_router_app) -m found }",
    ]
    assert (
        "acl ingress_request_router_app_has_replicas "
        f"var(txn.ingress_request_router_app),map({apps_map}) -m found"
    ) in config
    assert (
        "http-request lua.route_via_ingress_request_router if METH_POST "
        "has_ingress_request_router_app ingress_request_router_app_has_replicas"
    ) in config


@pytest.mark.asyncio
async def test_static_config_apply_still_reloads(tmp_path):
    h = Harness(tmp_path, dynamic=False)
    a = _replica("SERVE_REPLICA::app#d#a", 8001)
    h.set([_backend([a])])
    h.api._generate_config_file_internal()
    h.runtime.load_config(h.api)

    await h.apply([_backend([a])])

    assert h.reloads == 1
    assert h.runtime.writes() == []
    with open(h.api.config_file_path) as f:
        config = f.read()
    assert "server-template" not in config
    assert f"server {a.name} 10.0.0.2:8001 check" in config


@pytest.mark.asyncio
async def test_manager_applies_updates_instead_of_reloading():
    cls = HAProxyManager.__ray_metadata__.modified_class
    manager = cls.__new__(cls)
    manager._reload_lock = mock.MagicMock()
    manager._reload_lock.__aenter__ = mock.AsyncMock()
    manager._reload_lock.__aexit__ = mock.AsyncMock(return_value=False)

    async def _started():
        return None

    manager._haproxy_start_task = asyncio.ensure_future(_started())
    manager._haproxy = mock.Mock()
    manager._haproxy.apply = mock.AsyncMock()
    manager._haproxy.reload = mock.AsyncMock()

    await manager._reload_haproxy()

    manager._haproxy.apply.assert_awaited_once()
    manager._haproxy.reload.assert_not_awaited()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
