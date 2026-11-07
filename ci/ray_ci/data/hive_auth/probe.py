"""Bounded fixture probes, separate from Dataset query counters."""

import json
import os
import socket
import sys
from contextlib import closing
from pathlib import Path


def options():
    from ray.data.tests.datasource._hive_test_utils import _connection_options

    return _connection_options(os.environ["HIVE_AUTH_PROFILE"])


def sql():
    from ray.data.tests.datasource._hive_test_utils import _make_connection_factory

    with closing(_make_connection_factory(options())()) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchall() == [(1,)]


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
