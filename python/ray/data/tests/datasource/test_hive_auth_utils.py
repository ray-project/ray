"""Fast tests for Hive configuration reuse and integration evidence."""

import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from ray.data.tests.datasource._hive_test_utils import (
    _case_directory,
    _connection_options,
    _Events,
    _make_auth_factory,
    _make_connection_factory,
    _read_events,
    _temporary_table,
)


@pytest.fixture
def dbapi(monkeypatch):
    package = ModuleType("impala")
    module = ModuleType("impala.dbapi")
    module.__dict__["connect"] = None
    package.__dict__.update(dbapi=module, __path__=[])
    monkeypatch.setitem(sys.modules, "impala", package)
    monkeypatch.setitem(sys.modules, "impala.dbapi", module)
    return module


@pytest.fixture
def options(monkeypatch):
    monkeypatch.setenv("HIVE_TEST_PASSWORD", "test-only-marker")
    monkeypatch.delenv("RAY_HIVE_TEST_USE_SSL", raising=False)
    return _connection_options("example.test", "PLAIN")


def test_factory_reads_current_password_and_preserves_explicit_ca(
    monkeypatch, options, dbapi
):
    import ray.cloudpickle

    calls = []
    monkeypatch.setattr(dbapi, "connect", lambda **kwargs: calls.append(kwargs))
    options.update(use_ssl=True, ca_cert=None)
    factory = _make_connection_factory(options, timeout=30)
    assert b"test-only-marker" not in ray.cloudpickle.dumps(factory)
    monkeypatch.setenv("HIVE_TEST_PASSWORD", "changed-at-runtime")
    monkeypatch.setenv("RAY_HIVE_TEST_CA_CERT", "/must-not-override.pem")
    factory()
    assert calls == [
        {
            **options,
            "password": "changed-at-runtime",
            "verify_cert": True,
            "retries": 1,
            "timeout": 30,
        }
    ]


def test_original_factory_does_not_add_timeout(monkeypatch, options, dbapi):
    calls = []
    monkeypatch.setattr(dbapi, "connect", lambda **kwargs: calls.append(kwargs))
    _make_connection_factory(options)()
    assert "timeout" not in calls[0]


@pytest.mark.parametrize("auth", ["GSSAPI", "NOSASL"])
def test_non_plain_factory_does_not_read_password(monkeypatch, options, auth, dbapi):
    calls = []
    monkeypatch.setattr(dbapi, "connect", lambda **kwargs: calls.append(kwargs))
    options["auth_mechanism"] = auth
    _make_connection_factory(options)()
    assert calls[0]["password"] is None


@pytest.mark.parametrize("auth,ssl", [("INVALID", "false"), ("PLAIN", "invalid")])
def test_invalid_configuration_is_rejected(monkeypatch, auth, ssl):
    monkeypatch.setenv("RAY_HIVE_TEST_USE_SSL", ssl)
    monkeypatch.setenv("HIVE_TEST_PASSWORD", "present")
    with pytest.raises(pytest.fail.Exception):
        _connection_options("example.test", auth)


def test_invalid_port_keeps_original_value_error(monkeypatch, options):
    monkeypatch.setenv("RAY_HIVE_TEST_PORT", "not-an-integer")
    with pytest.raises(ValueError):
        _connection_options("example.test", "PLAIN")


def test_missing_plain_password_is_rejected_at_invocation(monkeypatch, options, dbapi):
    factory = _make_connection_factory(options)
    monkeypatch.delenv("HIVE_TEST_PASSWORD")
    monkeypatch.delenv("RAY_HIVE_TEST_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="requires HIVE_TEST_PASSWORD"):
        factory()


def _evidence(tmp_path, *, rpc=False):
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "terminal": "error",
                "roles": ["worker"],
                "run_id": "unit",
                "case_id": tmp_path.name,
            }
        )
    )
    writer = _Events(tmp_path, "worker", "node", "/ray/python/ray/data/hive.py")
    writer.emit("factory_enter")
    if rpc:
        writer.emit("cursor_return")
        writer.emit("rpc_attempt", kind="data")
    else:
        writer.emit("error", stage="connect")
    writer.emit("producer_end")
    return writer


def test_zero_rpc_requires_complete_evidence(tmp_path):
    _evidence(tmp_path)
    records = _read_events(tmp_path, roles=["worker"])
    assert not any(r["event"] == "rpc_attempt" for r in records)


def test_writer_flushes_before_delegation(tmp_path):
    writer = _evidence(tmp_path, rpc=True)
    records = [json.loads(line) for line in writer.path.read_text().splitlines()]
    assert [r["seq"] for r in records] == list(range(len(records)))
    assert records[3]["event"] == "rpc_attempt"


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "truncated",
        "sequence",
        "role",
        "identity",
        "end",
        "terminal",
        "extra",
    ],
)
def test_incomplete_or_unexpected_producer_is_rejected(tmp_path, damage):
    writer = _evidence(tmp_path)
    records = [json.loads(line) for line in writer.path.read_text().splitlines()]
    if damage == "missing":
        writer.path.unlink()
    elif damage == "truncated":
        writer.path.write_text(writer.path.read_text()[:-1])
    elif damage == "terminal":
        (tmp_path / "manifest.json").write_text(json.dumps({"roles": ["worker"]}))
    elif damage == "extra":
        other = _Events(tmp_path, "worker", "other-node", "source")
        other.emit("factory_enter")
        other.emit("error", stage="connect")
        other.emit("producer_end")
    else:
        if damage == "sequence":
            records[1]["seq"] = 9
        elif damage == "role":
            for r in records:
                r["role"] = "driver"
        elif damage == "identity":
            records[1]["node"] = "different"
        elif damage == "end":
            records.pop()
        writer.path.write_text("".join(json.dumps(r) + "\n" for r in records))
    with pytest.raises(ValueError):
        _read_events(tmp_path, roles=["worker"])


def test_case_path_cannot_escape_volume(monkeypatch, tmp_path):
    monkeypatch.setenv("HIVE_AUTH_EVENTS", str(tmp_path))
    assert _case_directory("abc123") == tmp_path / "abc123"
    with pytest.raises(ValueError):
        _case_directory("../other")


def test_event_collection_counts_all_producers_before_replay_assertion(tmp_path):
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "terminal": "success",
                "roles": ["driver", "worker"],
                "run_id": "unit",
                "case_id": tmp_path.name,
            }
        )
    )
    for role in ("driver", "worker"):
        writer = _Events(tmp_path, role, role, "source")
        writer.emit("factory_enter")
        writer.emit("rpc_attempt", kind="data")
        writer.emit("producer_end")
    records = _read_events(tmp_path, roles=["driver", "worker"])
    assert sum(r.get("kind") == "data" for r in records) == 2


def test_auth_wrapper_records_real_client_boundary_before_delegation(
    tmp_path, monkeypatch, options
):
    import ray
    from ray.data.tests.datasource import _hive_test_utils as utils

    monkeypatch.setenv("HIVE_AUTH_EVENTS", str(tmp_path))
    monkeypatch.setenv("HIVE_AUTH_ROLE", "worker")
    monkeypatch.setenv("HIVE_AUTH_RUN_ID", "unit")
    monkeypatch.setenv("HIVE_AUTH_CA_OVERRIDE", "/role-specific.pem")
    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "node")
    )
    directory = tmp_path / "abc123"
    resolved = []
    delegated = []

    def execute(request):
        files = list(directory.glob("*.jsonl"))
        records = [json.loads(line) for line in files[0].read_text().splitlines()]
        assert records[-1]["event"] == "rpc_attempt"
        delegated.append(request)
        return "actual-return-value"

    cursor = SimpleNamespace(
        fetchmany=lambda n: [(1,)], close=lambda: None, cancel_operation=lambda: None
    )
    client = SimpleNamespace(ExecuteStatement=execute)
    connection = SimpleNamespace(
        service=SimpleNamespace(client=client),
        cursor=lambda **kw: cursor,
        close=lambda: None,
    )

    def build(settings, **kwargs):
        resolved.append(settings)
        assert kwargs["timeout"] == 30
        return lambda: connection

    monkeypatch.setattr(utils, "_make_connection_factory", build)
    tracked = _make_auth_factory(options, "abc123")()
    wrapped_cursor = tracked.cursor()
    request = SimpleNamespace(statement="SELECT sensitive_statement_marker")
    assert client.ExecuteStatement(request) == "actual-return-value"
    assert delegated == [request]
    assert wrapped_cursor.fetchmany(1024) == [(1,)]
    wrapped_cursor.cancel_operation()
    wrapped_cursor.close()
    tracked.close()
    assert resolved[0]["ca_cert"] == "/role-specific.pem"
    assert options["ca_cert"] != "/role-specific.pem"
    text = next(directory.glob("*.jsonl")).read_text()
    assert "sensitive_statement_marker" not in text
    assert "test-only-marker" not in text


@pytest.mark.parametrize("mode", ["insert", "load", "load_error"])
def test_temporary_table_preserves_default_and_cleans_load_failure(
    monkeypatch, options, mode
):
    from ray.data.tests.datasource import _hive_test_utils as utils

    calls = []

    def execute(statement, parameters=None):
        calls.append((statement, parameters))
        if mode == "load_error" and statement.startswith("LOAD"):
            raise ValueError("load failed")

    cursor = SimpleNamespace(
        execute=execute, close=lambda: calls.append(("cursor_close", None))
    )
    connection = SimpleNamespace(
        cursor=lambda **kw: cursor,
        close=lambda: calls.append(("connection_close", None)),
    )
    monkeypatch.setattr(
        utils, "_make_connection_factory", lambda *a, **kw: lambda: connection
    )

    def prepare():
        with _temporary_table(
            options,
            "id BIGINT",
            "VALUES (1)",
            local_data_path="/security/test-data.tsv" if mode != "insert" else None,
        ):
            pass

    if mode == "load_error":
        with pytest.raises(ValueError, match="load failed"):
            prepare()
    else:
        prepare()
    assert calls[0][0].startswith("CREATE TABLE ray_hive_it_")
    if mode == "insert":
        assert calls[1][0].startswith("INSERT INTO TABLE ") and calls[1][1] is None
    else:
        assert calls[1][0].startswith("LOAD DATA LOCAL INPATH %(path)s")
        assert calls[1][1] == {"path": "/security/test-data.tsv"}
    assert calls[-3][0].startswith("DROP TABLE IF EXISTS ")
    assert [call[0] for call in calls[-2:]] == ["cursor_close", "connection_close"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
