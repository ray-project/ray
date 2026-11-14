"""Real HiveServer2 TLS reads using the common Hive integration fixture."""

import base64
import os
import socket
import ssl
import sys
import time
from contextlib import closing

import pyarrow as pa
import pytest

import ray
from ray.data.tests.datasource._hive_test_utils import (
    _make_auth_factory,
    _read_evidence,
)
from ray.data.tests.datasource.test_hive_integration import (
    _connection_options,
    _make_connection_factory,
    _temporary_table,
)

CASES = ("table", "untrusted_ca_driver", "hostname_worker")
SCHEMA = pa.schema([("id", pa.int64()), ("name", pa.string())])
pytestmark = [
    pytest.mark.timeout(180),
    pytest.mark.skipif(
        not os.environ.get("RAY_HIVE_CA_PEM_B64"),
        reason="Set RAY_HIVE_CA_PEM_B64 to run the Hive TLS integration suite",
    ),
]


def _verification_code(host, ca_path):
    context = ssl.create_default_context(cafile=ca_path)
    with socket.create_connection((host, 10000), timeout=30) as transport:
        with pytest.raises(ssl.SSLCertVerificationError) as error:
            context.wrap_socket(transport, server_hostname=host)
    assert isinstance(error.value.verify_code, int)
    return error.value.verify_code


@pytest.mark.parametrize("scenario", CASES, ids=CASES)
def test_hive_tls(scenario, tmp_path, ray_start_cluster_enabled):
    ca = tmp_path / "ca.pem"
    ca.write_bytes(base64.b64decode(os.environ["RAY_HIVE_CA_PEM_B64"], validate=True))
    options = _connection_options()
    assert options["host"] == "hiveserver2" and options["use_ssl"]
    options["ca_cert"] = str(ca)
    assert socket.gethostbyname("hiveserver2") == socket.gethostbyname(
        "wrong-hiveserver2"
    )

    # TCP readiness is not enough: wait for a verified TLS SQL query.
    deadline = time.monotonic() + 120
    while True:
        try:
            with closing(_make_connection_factory(options)()) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute("SELECT 1")
                    assert cursor.fetchall() == [(1,)]
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)

    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2, resources={"hive_tls_worker": 1})
    ray.init(cluster.address)
    driver = ray.get_runtime_context().get_node_id()
    worker = ray.get(
        ray.remote(resources={"hive_tls_worker": 1})(
            lambda: ray.get_runtime_context().get_node_id()
        ).remote()
    )
    assert driver != worker
    with _temporary_table(
        options, "id BIGINT, name STRING", "VALUES (1, 'a'), (2, 'b')"
    ) as (table, _):
        failing_options = options.copy()
        code: int | None = None
        if scenario == "untrusted_ca_driver":
            import certifi

            failing_options["ca_cert"] = certifi.where()
            code = _verification_code("hiveserver2", certifi.where())
        elif scenario == "hostname_worker":
            failing_options["host"] = "wrong-hiveserver2"
            code = _verification_code("wrong-hiveserver2", str(ca))
        events = tmp_path / "events"
        events.mkdir()
        factory = _make_auth_factory(
            _make_connection_factory(failing_options), str(events)
        )

        def read():
            resources: dict[str, float] = {"hive_tls_worker": 1}
            kwargs = dict(connection_factory=factory, resources=resources)
            if scenario == "hostname_worker":
                return ray.data.read_hive(
                    query=f"SELECT id, name FROM `{table}`", schema=SCHEMA, **kwargs
                ).take_all()
            dataset = ray.data.read_hive(table, **kwargs)
            rows = dataset.take_all()
            assert dataset.schema().base_schema == SCHEMA
            return rows

        if scenario == "table":
            assert sorted((dict(row) for row in read()), key=lambda row: row["id"]) == [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
            ]
            records = _read_evidence(events, {driver, worker}, __file__)
            assert all(r["connected"] and not r["certificate_error"] for r in records)
        else:
            with pytest.raises(Exception, match="HiveServer2"):
                read()
            records = _read_evidence(
                events,
                {driver if scenario == "untrusted_ca_driver" else worker},
                __file__,
            )
            assert all(not r["connected"] and r["certificate_error"] for r in records)
            if scenario == "untrusted_ca_driver":
                assert all(r["verify_code"] == code for r in records)
            else:
                # Thrift performs hostname matching after the TLS handshake.
                assert all(r["hostname_mismatch"] for r in records)


class _EnsureCases:
    def __init__(self):
        self.expected = {f"test_hive_tls[{case}]" for case in CASES}
        self.executed: set[str] = set()
        self.skipped = False

    def pytest_collection_finish(self, session):
        if {item.name for item in session.items} != self.expected:
            raise pytest.UsageError(
                "TLS suite did not collect exactly the expected cases"
            )

    def pytest_runtest_logreport(self, report):
        if report.when == "call":
            self.executed.add(report.nodeid.rsplit("::", 1)[-1])
        self.skipped |= report.skipped

    def pytest_sessionfinish(self, session, exitstatus):
        if session.exitstatus == pytest.ExitCode.OK and (
            self.skipped or self.executed != self.expected
        ):
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__], plugins=[_EnsureCases()]))
