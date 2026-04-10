"""Example: Arrow Flight for inter-actor data transfer in Ray.

Actor A runs an Arrow Flight server that serves a PyArrow table.
Actor B connects as a Flight client and pulls the data.

Usage:
    python test_arrow_flight.py [--num-rows 1000000]
"""

import argparse
import time

import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import ray


def make_table(num_rows: int) -> pa.Table:
    """Create a dummy table with the given number of rows."""
    return pa.table(
        {
            "id": pa.array(np.arange(num_rows, dtype=np.int64)),
            "value": pa.array(np.random.randn(num_rows)),
            "label": pa.array(
                np.random.choice(["cat", "dog", "fish"], size=num_rows)
            ),
        }
    )


class TableFlightServer(flight.FlightServerBase):
    """A minimal Flight server that serves a single table."""

    def __init__(self, table: pa.Table, location: str):
        super().__init__(location)
        self._table = table

    def get_flight_info(self, context, descriptor):
        schema = self._table.schema
        endpoints = [flight.FlightEndpoint(descriptor.command, [])]
        return flight.FlightInfo(
            schema, descriptor, endpoints, self._table.num_rows, -1
        )

    def do_get(self, context, ticket):
        return flight.RecordBatchStream(self._table)


@ray.remote
class DataServer:
    """Ray actor that hosts a Flight server."""

    def __init__(self, num_rows: int):
        self._table = make_table(num_rows)
        self._server = TableFlightServer(self._table, "grpc://0.0.0.0:0")
        self._port = self._server.port
        self._server.serve()

    def port(self) -> int:
        return self._port

    def num_rows(self) -> int:
        return self._table.num_rows

    def shutdown(self):
        self._server.shutdown()


@ray.remote
class DataClient:
    """Ray actor that pulls data from a Flight server."""

    def fetch(self, host: str, port: int) -> dict:
        location = f"grpc://{host}:{port}"
        client = flight.connect(location)

        descriptor = flight.FlightDescriptor.for_command(b"table")
        info = client.get_flight_info(descriptor)
        endpoint = info.endpoints[0]

        start = time.perf_counter()
        reader = client.do_get(endpoint.ticket)
        table = reader.read_all()
        elapsed = time.perf_counter() - start

        size_bytes = table.nbytes
        return {
            "num_rows": table.num_rows,
            "num_columns": table.num_columns,
            "size_mb": size_bytes / (1024 * 1024),
            "elapsed_s": elapsed,
            "throughput_mb_s": (size_bytes / (1024 * 1024)) / elapsed
            if elapsed > 0
            else float("inf"),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-rows", type=int, default=1_000_000)
    args = parser.parse_args()

    ray.init()

    server = DataServer.remote(args.num_rows)
    client = DataClient.remote()

    port = ray.get(server.port.remote())
    num_rows = ray.get(server.num_rows.remote())
    # On a single node, the server is reachable at localhost.
    host = "127.0.0.1"

    print(f"Server ready on port {port}, serving {num_rows:,} rows")
    print(f"Client fetching from {host}:{port} ...")

    result = ray.get(client.fetch.remote(host, port))

    print(f"Received {result['num_rows']:,} rows, {result['num_columns']} columns")
    print(f"Data size: {result['size_mb']:.1f} MB")
    print(f"Transfer time: {result['elapsed_s']:.3f}s")
    print(f"Throughput: {result['throughput_mb_s']:.1f} MB/s")

    ray.get(server.shutdown.remote())
    ray.shutdown()


if __name__ == "__main__":
    main()
