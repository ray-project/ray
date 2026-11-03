"""Check the small security fixture helpers and connection summaries."""

import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from ray.data.tests.datasource import _hive_test_utils as utils


@pytest.mark.parametrize("profile", ["T", "G", "N"])
def test_factory_reads_password_at_invocation(monkeypatch, profile):
    import ray.cloudpickle

    module = ModuleType("impala.dbapi")
    calls = []
    module.__dict__["connect"] = lambda **kw: calls.append(kw)
    monkeypatch.setitem(sys.modules, "impala.dbapi", module)
    monkeypatch.setenv("HIVE_TEST_PASSWORD", "before-serialization")
    factory = utils._make_connection_factory(utils._connection_options(profile))
    assert b"before-serialization" not in ray.cloudpickle.dumps(factory)
    monkeypatch.setenv("HIVE_TEST_PASSWORD", "current-password")
    factory()
    assert calls[0]["password"] == ("current-password" if profile == "T" else None)
    assert calls[0]["verify_cert"] == (profile == "T")
    assert calls[0]["retries"] == 1 and calls[0]["timeout"] == 30


@pytest.mark.parametrize(
    "damage", ["missing", "truncated", "unfinished", "role", "case_id", "node"]
)
def test_incomplete_summary_is_not_zero(tmp_path, damage):
    if damage != "missing":
        summary = utils._Summary(
            tmp_path, "worker", "node", "/ray/python/ray/data/hive.py"
        )
        summary.update(complete=damage != "unfinished", error="connect")
        if damage in {"role", "case_id", "node"}:
            summary.update(**{damage: "unexpected"})
        if damage == "truncated":
            summary.path.write_text(summary.path.read_text()[:-2])
    with pytest.raises(ValueError):
        utils._read_summaries(tmp_path, {"worker": "node"})


@pytest.mark.parametrize("counts", [(0,), (1, 1)])
def test_completed_json_without_newline_counts_all_connections(tmp_path, counts):
    for count in counts:
        summary = utils._Summary(
            tmp_path, "worker", "node", "/ray/python/ray/data/hive.py"
        )
        summary.update(
            complete=True, data_rpc=count, error="connect" if count == 0 else None
        )
        summary.path.write_text(json.dumps(summary.data))
    records = utils._read_summaries(tmp_path, {"worker": "node"})
    assert len(records) == len(counts)
    assert sum(r["data_rpc"] for r in records) == sum(counts)


def test_real_client_boundary_is_persisted_before_delegation(tmp_path, monkeypatch):
    import ray

    monkeypatch.setenv("HIVE_AUTH_EVENTS", str(tmp_path))
    monkeypatch.setenv("HIVE_AUTH_ROLE", "worker")
    monkeypatch.setenv("HIVE_AUTH_CA_OVERRIDE", "/role-specific.pem")
    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "node")
    )
    directory = tmp_path / "abc123"
    resolved = []

    def execute(request):
        record = json.loads(next(directory.glob("*.json")).read_text())
        assert record["data_rpc"] == 1 and record["complete"] is False
        return "actual-result"

    client = SimpleNamespace(ExecuteStatement=execute)
    cursor = SimpleNamespace(
        fetchmany=lambda n: [(1,)], close=lambda: None, cancel_operation=lambda: None
    )
    connection = SimpleNamespace(
        service=SimpleNamespace(client=client),
        cursor=lambda **kw: cursor,
        close=lambda: None,
    )

    def build(options):
        resolved.append(options)
        return lambda: connection

    monkeypatch.setattr(utils, "_make_connection_factory", build)
    tracked = utils._make_auth_factory(utils._connection_options("G"), "abc123")()
    wrapped = tracked.cursor()
    assert (
        client.ExecuteStatement(SimpleNamespace(statement="SELECT secret_sql_marker"))
        == "actual-result"
    )
    assert wrapped.fetchmany(1024) == [(1,)]
    wrapped.cancel_operation()
    wrapped.close()
    tracked.close()
    record = json.loads(next(directory.glob("*.json")).read_text())
    assert (
        record["complete"] and record["cursor_closed"] and record["connection_closed"]
    )
    assert record["batches"] == record["rows"] == 1
    assert resolved[0]["ca_cert"] == "/role-specific.pem"
    assert "secret_sql_marker" not in next(directory.glob("*.json")).read_text()


def test_failed_factory_has_complete_zero_evidence(tmp_path, monkeypatch):
    import ray

    monkeypatch.setenv("HIVE_AUTH_EVENTS", str(tmp_path))
    monkeypatch.setenv("HIVE_AUTH_ROLE", "worker")
    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "node")
    )

    def rejected():
        raise ValueError("rejected")

    monkeypatch.setattr(utils, "_make_connection_factory", lambda options: rejected)
    with pytest.raises(ValueError, match="rejected"):
        utils._make_auth_factory(utils._connection_options("G"), "abc123")()
    record = json.loads(next((tmp_path / "abc123").glob("*.json")).read_text())
    assert (
        record["complete"] and record["error"] == "connect" and record["data_rpc"] == 0
    )


@pytest.mark.parametrize("failure", [None, "CREATE", "LOAD"])
def test_table_cleanup_on_preparation_failure(monkeypatch, failure):
    calls = []

    def execute(statement):
        calls.append(statement.split()[0])
        if calls[-1] == failure:
            raise ValueError("preparation failed")

    cursor = SimpleNamespace(
        execute=execute, close=lambda: calls.append("cursor_close")
    )
    connection = SimpleNamespace(
        cursor=lambda **kw: cursor, close=lambda: calls.append("connection_close")
    )
    monkeypatch.setattr(
        utils, "_make_connection_factory", lambda options: lambda: connection
    )
    if failure:
        with pytest.raises(
            ValueError, match="preparation failed"
        ), utils._temporary_table({"user": "hive"}):
            pass
    else:
        with utils._temporary_table({"user": "hive"}):
            pass
    assert ("DROP" in calls) == (failure != "CREATE")
    assert calls[-2:] == ["cursor_close", "connection_close"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
