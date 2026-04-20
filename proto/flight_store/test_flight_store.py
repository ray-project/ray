"""Simple test for the C++ Arrow Flight store.

Usage:
    cd proto/flight_store
    pip install -e .
    python test_flight_store.py
"""

import multiprocessing
import time

import pyarrow as pa

from arrow_flight_store import PyArrowFlightStore


def test_local_put_get():
    """Test local put and get_local."""
    store = PyArrowFlightStore()
    store.start_server()

    table = pa.table({"col": [1, 2, 3], "name": ["a", "b", "c"]})
    store.put("obj1", table)

    result = store.get_local("obj1")
    assert result is not None
    assert result.num_rows == 3
    assert result.column_names == ["col", "name"]
    print("  local put/get: OK")

    store.stop_server()


def test_local_delete():
    """Test delete and size."""
    store = PyArrowFlightStore()
    store.start_server()

    store.put("a", pa.table({"x": [1]}))
    store.put("b", pa.table({"x": [2]}))
    assert store.size() == 2

    store.delete("a")
    assert store.size() == 1
    assert store.get_local("a") is None
    assert store.get_local("b") is not None
    print("  delete/size: OK")

    store.stop_server()


def _remote_producer(uri_queue, done_event):
    """Run in a child process: produce a table and serve via Flight."""
    store = PyArrowFlightStore()
    port = store.start_server()
    uri = store.get_uri()

    # Create a large-ish table.
    import numpy as np
    data = np.zeros(1_000_000, dtype=np.float64)
    table = pa.table({"data": data})

    store.put("big_obj", table)
    uri_queue.put(uri)

    # Wait for consumer to fetch (auto-delete on read).
    done_event.wait(timeout=30)
    store.stop_server()


def test_remote_fetch():
    """Test cross-process fetch via Flight RPC."""
    uri_queue = multiprocessing.Queue()
    done_event = multiprocessing.Event()

    producer = multiprocessing.Process(
        target=_remote_producer, args=(uri_queue, done_event)
    )
    producer.start()

    uri = uri_queue.get(timeout=10)
    print(f"  producer URI: {uri}")

    # Consumer fetches from producer.
    consumer_store = PyArrowFlightStore()
    consumer_store.start_server()

    t0 = time.perf_counter()
    result = consumer_store.fetch(uri, "big_obj")
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"  fetched {result.num_rows} rows in {elapsed_ms:.1f}ms")

    assert result.num_rows == 1_000_000
    assert result.column_names == ["data"]

    done_event.set()
    producer.join(timeout=5)
    consumer_store.stop_server()
    print("  remote fetch: OK")


def test_move_semantics():
    """Test that table is auto-deleted after remote fetch (move semantics)."""
    store = PyArrowFlightStore()
    store.start_server()
    uri = store.get_uri()

    store.put("ephemeral", pa.table({"x": [42]}))
    assert store.size() == 1

    # Fetch locally via Flight (simulates remote consumer).
    consumer = PyArrowFlightStore()
    consumer.start_server()
    result = consumer.fetch(uri, "ephemeral")
    assert result.column("x")[0].as_py() == 42

    # Table should be auto-deleted from producer.
    assert store.size() == 0
    assert store.get_local("ephemeral") is None
    print("  move semantics: OK")

    store.stop_server()
    consumer.stop_server()


def _remote_producer_vm(info_queue, done_event):
    """Run in a child process: produce a table and expose for VM fetch."""
    store = PyArrowFlightStore()
    store.start_server()

    import numpy as np
    data = np.zeros(1_000_000, dtype=np.float64)
    table = pa.table({"data": data})

    info = store.put_and_get_transfer_info("vm_obj", table)
    info_queue.put(info)

    # Wait for consumer to fetch via process_vm_readv.
    done_event.wait(timeout=30)
    store.delete("vm_obj")
    store.stop_server()


def test_vm_fetch():
    """Test same-node fetch via process_vm_readv."""
    import sys
    if sys.platform != "linux":
        print("  process_vm_readv: SKIPPED (Linux only)")
        return

    info_queue = multiprocessing.Queue()
    done_event = multiprocessing.Event()

    producer = multiprocessing.Process(
        target=_remote_producer_vm, args=(info_queue, done_event)
    )
    producer.start()

    info = info_queue.get(timeout=10)
    print(f"  producer pid={info['pid']}, ipc_size={info['ipc_size']} bytes")

    consumer_store = PyArrowFlightStore()

    t0 = time.perf_counter()
    result = consumer_store.fetch_via_vm(
        info["pid"], info["ipc_address"], info["ipc_size"]
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"  VM fetched {result.num_rows} rows in {elapsed_ms:.1f}ms")

    assert result.num_rows == 1_000_000
    assert result.column_names == ["data"]

    done_event.set()
    producer.join(timeout=5)
    print("  process_vm_readv fetch: OK")


def test_flight_vs_vm_benchmark():
    """Compare Flight RPC vs process_vm_readv for same-node transfer."""
    import sys
    if sys.platform != "linux":
        print("  benchmark: SKIPPED (Linux only)")
        return

    import numpy as np

    # Producer in-process (simulates same-node).
    store = PyArrowFlightStore()
    store.start_server()
    uri = store.get_uri()

    sizes_mb = [1, 10, 50]
    for size_mb in sizes_mb:
        data = np.zeros(size_mb * 1024 * 1024 // 8, dtype=np.float64)
        table = pa.table({"data": data})

        # Flight path.
        store.put("bench_flight", table)
        consumer = PyArrowFlightStore()
        consumer.start_server()
        t0 = time.perf_counter()
        result = consumer.fetch(uri, "bench_flight")
        flight_ms = (time.perf_counter() - t0) * 1000
        consumer.stop_server()

        # VM path.
        info = store.put_and_get_transfer_info("bench_vm", table)
        t0 = time.perf_counter()
        result = consumer_store_dummy = PyArrowFlightStore()
        result = consumer_store_dummy.fetch_via_vm(
            info["pid"], info["ipc_address"], info["ipc_size"]
        )
        vm_ms = (time.perf_counter() - t0) * 1000
        store.delete("bench_vm")

        print(f"  {size_mb}MB: Flight={flight_ms:.1f}ms  VM={vm_ms:.1f}ms"
              f"  speedup={flight_ms/vm_ms:.1f}x")

    store.stop_server()
    print("  benchmark: OK")


if __name__ == "__main__":
    print("Testing C++ Arrow Flight Store...")
    test_local_put_get()
    test_local_delete()
    test_move_semantics()
    test_remote_fetch()
    test_vm_fetch()
    test_flight_vs_vm_benchmark()
    print("\nAll tests passed!")
