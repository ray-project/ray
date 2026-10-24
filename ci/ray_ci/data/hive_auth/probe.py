"""Bounded fixture probes, separate from Dataset query counters."""

import json
import os
import secrets
import socket
import sys
from pathlib import Path


def options():
    from ray.data.tests.datasource._hive_test_utils import _connection_options

    profile = os.environ["HIVE_AUTH_PROFILE"]
    auth = (
        "GSSAPI"
        if profile in {"G", "GT"}
        else ("NOSASL" if profile == "N" else "PLAIN")
    )
    os.environ["RAY_HIVE_TEST_USE_SSL"] = str(profile in {"T", "GT"}).lower()
    # NONE requires a PLAIN protocol field, but does not validate this value.
    os.environ.setdefault("HIVE_TEST_PASSWORD", secrets.token_urlsafe(16))
    return _connection_options(os.environ["RAY_HIVE_TEST_HOST"], auth)


def sql():
    from ray.data.tests.datasource._hive_test_utils import _make_connection_factory

    connection = _make_connection_factory(options(), timeout=30)()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            assert cursor.fetchall() == [(1,)]
        finally:
            cursor.close()
    finally:
        connection.close()


def workers():
    import ray

    ray.init(address="auto")
    try:

        @ray.remote(resources={"hive_auth_worker": 1})
        def check():
            from ray.data._internal.datasource_v2.formats.hive import hive_hs2

            return {
                "node": ray.get_runtime_context().get_node_id(),
                "source": str(Path(hive_hs2.__file__).resolve()),
            }

        result = ray.get(check.remote(), timeout=30)
        assert result["node"] != ray.get_runtime_context().get_node_id()
        assert result["source"].startswith("/ray/python/ray/data/")
        print(json.dumps(result))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    action = sys.argv[1]
    if action == "cluster":
        with socket.create_connection(("127.0.0.1", 6379), timeout=2):
            pass
    elif action == "sql":
        sql()
    elif action == "workers":
        workers()
    else:
        raise SystemExit("Unknown probe")
