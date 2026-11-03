"""Helpers for the fixed real-service Hive security fixture."""

import json
import os
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any


def _connection_options(profile):
    return dict(
        host="hive.auth.test",
        port=10000,
        user="hive",
        auth_mechanism="GSSAPI"
        if profile in {"G", "GT"}
        else ("NOSASL" if profile == "N" else "PLAIN"),
        kerberos_service_name="hive",
        use_ssl=profile in {"T", "GT"},
        ca_cert="/ca/ca.pem",
    )


def _make_connection_factory(options):
    def factory():
        from impala.dbapi import connect

        password = (
            os.environ["HIVE_TEST_PASSWORD"]
            if options["auth_mechanism"] == "PLAIN"
            else None
        )
        return connect(
            **options,
            password=password,
            verify_cert=options["use_ssl"],
            use_http_transport=False,
            retries=1,
            timeout=30,
        )

    return factory


@contextmanager
def _temporary_table(options):
    created = False
    table = f"ray_hive_it_{uuid.uuid4().hex[:12]}"
    with closing(_make_connection_factory(options)()) as connection:
        with closing(connection.cursor(user=options["user"])) as cursor:
            try:
                cursor.execute(
                    f"CREATE TABLE {table} (id BIGINT, name STRING) "
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE"
                )
                created = True
                cursor.execute(
                    f"LOAD DATA LOCAL INPATH '/security/test-data.tsv' INTO TABLE {table}"
                )
                yield table
            finally:
                if created:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")


class _Summary:
    """One file per connection, persisted before delegation; unfinished is not zero."""

    def __init__(self, directory, role, node, source):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{uuid.uuid4().hex}.json"
        self.data: dict[str, Any] = dict(
            case_id=directory.name,
            role=role,
            node=node,
            pid=os.getpid(),
            source=source,
            data_rpc=0,
            metadata_rpc=0,
            batches=0,
            rows=0,
            cursor_closed=False,
            connection_closed=False,
            error=None,
        )
        self.update(complete=False)

    def update(self, **fields):
        self.data.update(fields)
        self.path.write_text(json.dumps(self.data) + "\n")

    def call(self, stage, function, *args, **kwargs):
        self.update(stage=stage)
        try:
            return function(*args, **kwargs)
        except Exception:
            self.update(error=stage)
            raise


def _case_directory(case_id):
    if not case_id or any(c not in "0123456789abcdef" for c in case_id):
        raise ValueError("Expected a hexadecimal case ID")
    return Path(os.environ["HIVE_AUTH_EVENTS"]) / case_id


def _make_auth_factory(options, case_id):
    def factory():
        import ray
        from ray.data._internal.datasource_v2.formats.hive import hive_hs2

        summary = _Summary(
            _case_directory(case_id),
            os.environ["HIVE_AUTH_ROLE"],
            ray.get_runtime_context().get_node_id(),
            str(Path(hive_hs2.__file__).resolve()),
        )
        resolved = dict(options)
        for key, variable in (
            ("host", "HIVE_AUTH_HOST_OVERRIDE"),
            ("ca_cert", "HIVE_AUTH_CA_OVERRIDE"),
            ("kerberos_service_name", "HIVE_AUTH_SERVICE_OVERRIDE"),
        ):
            if variable in os.environ:
                resolved[key] = os.environ[variable]
        try:
            connection = summary.call("connect", _make_connection_factory(resolved))
        except Exception:
            summary.update(complete=True)
            raise
        client = connection.service.client
        execute = client.ExecuteStatement

        def execute_statement(request):
            key = (
                "metadata_rpc"
                if request.statement.lstrip().upper().startswith(("DESCRIBE ", "USE "))
                else "data_rpc"
            )
            summary.update(**{key: summary.data[key] + 1})
            return summary.call("execute", execute, request)

        client.ExecuteStatement = execute_statement
        return _TrackedConnection(connection, summary)

    return factory


class _TrackedConnection:
    def __init__(self, connection: Any, summary):
        self.connection, self.summary = connection, summary

    def cursor(self, **kwargs):
        cursor = self.summary.call("session", self.connection.cursor, **kwargs)
        return _TrackedCursor(cursor, self.summary)

    def close(self):
        self.summary.call("connection_close", self.connection.close)
        self.summary.update(connection_closed=True, complete=True)


class _TrackedCursor:
    def __init__(self, cursor: Any, summary):
        self.cursor, self.summary = cursor, summary

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, *args):
        return self.summary.call("execute", self.cursor.execute, *args)

    def fetchmany(self, *args):
        rows = self.summary.call("fetch", self.cursor.fetchmany, *args)
        self.summary.update(
            rows=self.summary.data["rows"] + len(rows),
            batches=self.summary.data["batches"] + bool(rows),
        )
        return rows

    def cancel_operation(self):
        result = self.summary.call("cancel", self.cursor.cancel_operation)
        self.summary.update(cancelled=True)
        return result

    def close(self):
        result = self.summary.call("cursor_close", self.cursor.close)
        self.summary.update(cursor_closed=True)
        return result


def _read_summaries(directory, nodes):
    records = []
    for path in directory.glob("*.json"):
        text = path.read_text()
        record = json.loads(text)
        if (
            not text.endswith("\n")
            or record["complete"] is not True
            or record["case_id"] != directory.name
            or record["role"] not in nodes
            or record["node"] != nodes[record["role"]]
            or not record["source"].startswith("/ray/python/ray/data/")
        ):
            raise ValueError("Incomplete or unexpected connection evidence")
        records.append(record)
    if {r["role"] for r in records} != set(nodes):
        raise ValueError("Missing connection evidence")
    return records


def _native(options):
    import ray

    with closing(_make_connection_factory(options)()) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchall() == [(1,)]
    return ray.get_runtime_context().get_node_id()


def _tls_condition(condition):
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
        cafile="/ca/wrong-ca.pem" if condition == "ca" else "/ca/ca.pem"
    )
    name = "wrong.hive.auth.test" if condition == "host" else host
    with socket.create_connection((host, 10000), timeout=30) as transport:
        with pytest.raises(ssl.SSLCertVerificationError):
            context.wrap_socket(transport, server_hostname=name)
