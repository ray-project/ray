"""Shared Hive test configuration and non-sensitive integration evidence."""

import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Optional, TypedDict, cast


class _ConnectionOptions(TypedDict):
    """Non-secret options captured by a connection factory."""

    host: str
    port: int
    auth_mechanism: Literal["NOSASL", "PLAIN", "GSSAPI"]
    user: str
    kerberos_service_name: str
    use_ssl: bool
    ca_cert: Optional[str]


def _connection_options(host: Optional[str], auth: str) -> _ConnectionOptions:
    import pytest

    if auth not in ("NOSASL", "PLAIN", "GSSAPI"):
        pytest.fail("RAY_HIVE_TEST_AUTH must be NOSASL, PLAIN, or GSSAPI")
    if auth == "PLAIN" and not (
        os.environ.get("HIVE_TEST_PASSWORD") or os.environ.get("RAY_HIVE_TEST_PASSWORD")
    ):
        pytest.fail("PLAIN integration requires HIVE_TEST_PASSWORD")
    ssl_setting = os.environ.get("RAY_HIVE_TEST_USE_SSL", "false").lower()
    if ssl_setting not in ("true", "false", "1", "0"):
        pytest.fail("RAY_HIVE_TEST_USE_SSL must be true or false")
    return {
        "host": cast(str, host),
        "port": int(os.environ.get("RAY_HIVE_TEST_PORT", "10000")),
        "auth_mechanism": cast(Literal["NOSASL", "PLAIN", "GSSAPI"], auth),
        "user": os.environ.get("RAY_HIVE_TEST_USER", "hive"),
        "kerberos_service_name": os.environ.get(
            "RAY_HIVE_TEST_KERBEROS_SERVICE", "hive"
        ),
        "use_ssl": ssl_setting in ("true", "1"),
        "ca_cert": os.environ.get("RAY_HIVE_TEST_CA_CERT"),
    }


def _make_connection_factory(
    options: _ConnectionOptions, *, timeout: Optional[int] = None
):
    def connection_factory():
        from impala.dbapi import connect

        password = None
        if options["auth_mechanism"] == "PLAIN":
            password = os.environ.get("HIVE_TEST_PASSWORD") or os.environ.get(
                "RAY_HIVE_TEST_PASSWORD"
            )
            if not password:
                raise ValueError("PLAIN integration requires HIVE_TEST_PASSWORD")
        extra = {} if timeout is None else {"timeout": timeout}
        return connect(
            **options,
            password=password,
            verify_cert=options["use_ssl"],
            retries=1,
            **extra,
        )

    return connection_factory


@contextmanager
def _temporary_table(
    options,
    columns,
    insert_clause,
    storage_clause="",
    *,
    local_data_path=None,
    timeout=None,
):
    from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import _connect

    connection = _connect(_make_connection_factory(options, timeout=timeout))
    cursor = None
    table = f"ray_hive_it_{uuid.uuid4().hex[:12]}"
    created = False
    try:
        cursor = connection.cursor(user=options["user"])
        cursor.execute(f"CREATE TABLE {table} ({columns}) {storage_clause}")
        created = True
        if local_data_path is None:
            cursor.execute(f"INSERT INTO TABLE {table} {insert_clause}")
        else:
            cursor.execute(
                f"LOAD DATA LOCAL INPATH %(path)s INTO TABLE {table}",
                {"path": local_data_path},
            )
        yield table, cursor
    finally:
        try:
            if created and cursor is not None:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            try:
                if cursor is not None:
                    cursor.close()
            finally:
                connection.close()


class _Events:
    """One writer per producer; every record reaches the volume before delegation."""

    def __init__(self, directory: Path, role: str, node: str, source: str):
        self.producer = uuid.uuid4().hex
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{self.producer}.jsonl"
        self.base = {
            "run_id": os.environ.get("HIVE_AUTH_RUN_ID", "unit"),
            "case_id": directory.name,
            "producer": self.producer,
            "role": role,
            "node": node,
            "pid": os.getpid(),
            "source": source,
        }
        self.seq = 0
        self.emit("producer_ready")

    def emit(self, event: str, **fields):
        record = dict(self.base, seq=self.seq, event=event, **fields)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
            stream.flush()
        self.seq += 1


def _native(options: _ConnectionOptions) -> str:
    import ray

    connection = _make_connection_factory(options, timeout=30)()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            assert cursor.fetchall() == [(1,)]
        finally:
            cursor.close()
    finally:
        connection.close()
    return ray.get_runtime_context().get_node_id()


def _tls_condition(condition: str) -> None:
    import socket
    import ssl

    import pytest

    host = "hive.auth.test"
    assert socket.gethostbyname(host) == socket.gethostbyname("wrong.hive.auth.test")
    good = ssl.create_default_context(cafile="/ca/ca.pem")
    with socket.create_connection((host, 10000), timeout=30) as transport:
        with good.wrap_socket(transport, server_hostname=host):
            pass
    if condition == "file":
        assert not Path("/ca/missing.pem").exists()
        return
    context = ssl.create_default_context(
        cafile=("/ca/wrong-ca.pem" if condition == "ca" else "/ca/ca.pem")
    )
    name = "wrong.hive.auth.test" if condition == "host" else host
    with socket.create_connection((host, 10000), timeout=30) as transport:
        with pytest.raises(ssl.SSLCertVerificationError):
            context.wrap_socket(transport, server_hostname=name)


def _expiry() -> dict[str, Any]:
    info = json.loads(Path("/auth/expiry.json").read_text())
    assert isinstance(info, dict)
    assert info["initially_valid"] is True
    assert 0 < info["end"] - info["issued"] <= 15
    assert info["clockskew"] == 1
    assert os.environ["KRB5_CONFIG"] == "/krb/krb5.conf"
    assert Path("/auth/expired.ccache").is_file()
    return info


def _case_directory(case_id: str) -> Path:
    # IDs are generated by the driver; reject paths outside the event volume.
    if not case_id or any(c not in "0123456789abcdef" for c in case_id):
        raise ValueError("Expected a hexadecimal case ID")
    return Path(os.environ["HIVE_AUTH_EVENTS"]) / case_id


def _make_auth_factory(options: _ConnectionOptions, case_id: str):
    def factory():
        import ray
        from ray.data._internal.datasource_v2.formats.hive import hive_hs2

        events = _Events(
            _case_directory(case_id),
            os.environ["HIVE_AUTH_ROLE"],
            ray.get_runtime_context().get_node_id(),
            str(Path(hive_hs2.__file__).resolve()),
        )
        events.emit("factory_enter")
        resolved = dict(options)
        for key, variable in (
            ("host", "HIVE_AUTH_HOST_OVERRIDE"),
            ("ca_cert", "HIVE_AUTH_CA_OVERRIDE"),
            ("kerberos_service_name", "HIVE_AUTH_SERVICE_OVERRIDE"),
        ):
            if variable in os.environ:
                resolved[key] = os.environ[variable]
        try:
            connection = _make_connection_factory(
                cast(_ConnectionOptions, resolved), timeout=30
            )()
        except Exception:
            events.emit("error", stage="connect")
            events.emit("producer_end")
            raise
        # Impyla 0.23.0 delegates _execute to this actual Thrift client method.
        # Record each invocation, rather than only the cursor.execute facade.
        client = connection.service.client
        execute = client.ExecuteStatement

        def execute_statement(request):
            statement = request.statement.lstrip().upper()
            kind = "metadata" if statement.startswith(("DESCRIBE ", "USE ")) else "data"
            events.emit("rpc_attempt", kind=kind)
            return execute(request)

        client.ExecuteStatement = execute_statement
        return _TrackedConnection(connection, events)

    return factory


class _TrackedConnection:
    def __init__(self, connection: Any, events: _Events):
        self.connection = connection
        self.events = events

    def cursor(self, **kwargs):
        self.events.emit("cursor_enter")
        try:
            cursor = self.connection.cursor(**kwargs)
        except Exception:
            self.events.emit("error", stage="session")
            raise
        self.events.emit("cursor_return")
        return _TrackedCursor(cursor, self.events)

    def close(self):
        try:
            self.connection.close()
            self.events.emit("connection_close")
        finally:
            self.events.emit("producer_end")


class _TrackedCursor:
    def __init__(self, cursor: Any, events: _Events):
        self.cursor = cursor
        self.events = events

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def get_table_schema(self, *args):
        self.events.emit("metadata_enter")
        return self.cursor.get_table_schema(*args)

    def execute(self, *args):
        try:
            return self.cursor.execute(*args)
        except Exception:
            self.events.emit("error", stage="execute")
            raise

    def fetchmany(self, *args):
        try:
            rows = self.cursor.fetchmany(*args)
        except Exception:
            self.events.emit("error", stage="fetch")
            raise
        self.events.emit("fetch", rows=len(rows))
        return rows

    def cancel_operation(self):
        result = self.cursor.cancel_operation()
        self.events.emit("cancel")
        return result

    def close(self):
        result = self.cursor.close()
        self.events.emit("cursor_close")
        return result


def _read_events(directory: Path, *, roles):
    """Validate all producers before accepting any RPC count, including zero."""
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("terminal") not in {"success", "error"}:
        raise ValueError("Dataset action has no observed terminal state")
    if set(manifest["roles"]) != set(roles):
        raise ValueError("Unexpected role manifest")
    if not manifest.get("run_id") or manifest.get("case_id") != directory.name:
        raise ValueError("Unexpected run or case manifest")
    result = []
    seen_roles = []
    for path in sorted(directory.glob("*.jsonl")):
        text = path.read_text()
        if not text.endswith("\n"):
            raise ValueError("Truncated producer file")
        records = [json.loads(line) for line in text.splitlines()]
        if not records or records[0]["event"] != "producer_ready":
            raise ValueError("Missing producer readiness")
        first = records[0]
        if any(first.get(key) != manifest[key] for key in ("run_id", "case_id")):
            raise ValueError("Producer belongs to another run or case")
        if path.stem != first["producer"] or first["role"] not in roles:
            raise ValueError("Unknown producer")
        for seq, record in enumerate(records):
            if record["seq"] != seq or any(
                record[key] != first[key]
                for key in (
                    "run_id",
                    "case_id",
                    "producer",
                    "role",
                    "node",
                    "pid",
                    "source",
                )
            ):
                raise ValueError("Producer sequence or identity mismatch")
        if not any(r["event"] == "factory_enter" for r in records):
            raise ValueError("Missing factory entry")
        if records[-1]["event"] != "producer_end":
            raise ValueError("Producer has no observed end")
        seen_roles.append(first["role"])
        result.extend(records)
    if sorted(seen_roles) != sorted(roles):
        raise ValueError("Missing or additional producer")
    return result
