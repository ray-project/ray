"""Correctness checks for the Flight object store path.

Runs the same set of tables through Ray in two ways — ray.get from the
driver, and actor-to-actor — then checks pa.Table.equals against the
original. Exercises both eager and lazy copy modes.

Usage:
    # Default: compares flight-store output against ground-truth local tables.
    RAY_USE_FLIGHT_STORE=1 python proto/verify_flight_store.py
    RAY_USE_FLIGHT_STORE=1 ARROW_IPC_COPY_MODE=lazy python proto/verify_flight_store.py

    # Baseline: plasma path (should also pass — sanity check on the harness).
    python proto/verify_flight_store.py
"""

import os
import sys
import traceback

import numpy as np
import pyarrow as pa

import ray

# ---------------------------------------------------------------------------
# Test cases — diverse schemas and sizes.
# ---------------------------------------------------------------------------


def case_empty():
    return pa.table({"x": pa.array([], type=pa.int64())})


def case_tiny_ints():
    return pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})


def case_float64_1mb():
    n = 1 * 1024 * 1024 // 8
    rng = np.random.default_rng(0)
    return pa.table({"data": rng.standard_normal(n)})


def case_float64_10mb():
    n = 10 * 1024 * 1024 // 8
    rng = np.random.default_rng(1)
    return pa.table({"data": rng.standard_normal(n)})


def case_float64_100mb():
    n = 100 * 1024 * 1024 // 8
    rng = np.random.default_rng(2)
    return pa.table({"data": rng.standard_normal(n)})


def case_many_columns():
    rng = np.random.default_rng(3)
    cols = {f"c{i}": rng.integers(0, 1_000_000, 10_000) for i in range(50)}
    return pa.table(cols)


def case_strings():
    return pa.table(
        {
            "id": pa.array(range(10_000), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(10_000)]),
        }
    )


def case_nulls():
    return pa.table(
        {
            "a": pa.array([1, None, 3, None, 5], type=pa.int64()),
            "b": pa.array(["x", None, None, "y", "z"]),
        }
    )


def case_nested_list():
    return pa.table(
        {
            "tags": pa.array([["a", "b"], [], ["c"], ["d", "e", "f"], None] * 100),
        }
    )


def case_dictionary():
    values = pa.array(["apple", "banana", "cherry"])
    indices = pa.array([0, 1, 2, 0, 1, 2, 0] * 1000, type=pa.int32())
    dict_arr = pa.DictionaryArray.from_arrays(indices, values)
    return pa.table({"fruit": dict_arr})


# Env vars the workers need to see (so RAY_USE_FLIGHT_STORE / ARROW_IPC_COPY_MODE
# set on the driver propagate to actor processes).
_PROPAGATED_ENV_VARS = ("RAY_USE_FLIGHT_STORE", "ARROW_IPC_COPY_MODE")


CASES = [
    ("empty", case_empty),
    ("tiny_ints", case_tiny_ints),
    ("float64_1mb", case_float64_1mb),
    ("float64_10mb", case_float64_10mb),
    ("float64_100mb", case_float64_100mb),
    ("many_columns", case_many_columns),
    ("strings", case_strings),
    ("nulls", case_nulls),
    ("nested_list", case_nested_list),
    ("dictionary", case_dictionary),
]


# ---------------------------------------------------------------------------
# Ray actors.
# ---------------------------------------------------------------------------


@ray.remote(num_cpus=1)
class Producer:
    def make(self, case_name):
        for name, fn in CASES:
            if name == case_name:
                return fn()
        raise KeyError(case_name)


@ray.remote(num_cpus=1)
class Consumer:
    def passthrough(self, table):
        # Forces the table to actually be materialized in this process.
        assert isinstance(table, pa.Table), f"got {type(table)}"
        return table

    def check_equal(self, received, expected):
        """Semantic equality check run in the consumer process.

        Both `received` and `expected` arrive via the same transfer path
        (Flight store when enabled), so this also exercises argument passing
        for pa.Table. Returns a small result dict so we can surface diffs.
        """
        assert isinstance(received, pa.Table), f"got {type(received)}"
        assert isinstance(expected, pa.Table), f"got {type(expected)}"
        return _compare(received, expected)


def _compare(actual, expected):
    """Semantic comparison — returns a dict describing any mismatch."""
    result = {
        "equal": False,
        "num_rows_actual": actual.num_rows,
        "num_rows_expected": expected.num_rows,
        "schema_actual": str(actual.schema),
        "schema_expected": str(expected.schema),
    }
    if actual.schema != expected.schema:
        result["reason"] = "schema mismatch"
        return result
    if actual.num_rows != expected.num_rows:
        result["reason"] = "row count mismatch"
        return result
    # pa.Table.equals ignores chunk structure, compares values.
    if not actual.equals(expected):
        result["reason"] = "values differ"
        return result
    result["equal"] = True
    return result


# ---------------------------------------------------------------------------
# Checks.
# ---------------------------------------------------------------------------


def check(name, result):
    if result["equal"]:
        print(f"  PASS  {name}")
        return True
    print(f"  FAIL  {name}  ({result.get('reason', 'unknown')})")
    if result["schema_actual"] != result["schema_expected"]:
        print(f"         schema expected: {result['schema_expected']}")
        print(f"         schema actual:   {result['schema_actual']}")
    if result["num_rows_actual"] != result["num_rows_expected"]:
        print(
            f"         num_rows expected={result['num_rows_expected']} "
            f"actual={result['num_rows_actual']}"
        )
    return False


def run_suite():
    mode_label = (
        "Flight store"
        if os.environ.get("RAY_USE_FLIGHT_STORE", "0") == "1"
        else "Plasma (baseline)"
    )
    copy_mode = os.environ.get("ARROW_IPC_COPY_MODE", "eager")
    print(f"Mode: {mode_label}  (copy_mode={copy_mode})")
    print()

    env_vars = {k: os.environ[k] for k in _PROPAGATED_ENV_VARS if k in os.environ}
    ray.init(runtime_env={"env_vars": env_vars} if env_vars else None)

    producer = Producer.remote()
    consumer = Consumer.remote()

    passed = failed = 0

    for case_name, fn in CASES:
        expected_table = fn()

        # Path 1: producer -> driver (ray.get).
        try:
            ref = producer.make.remote(case_name)
            got = ray.get(ref)
            ok = check(f"{case_name:16s} driver_get", _compare(got, expected_table))
            passed += ok
            failed += not ok
        except Exception:
            print(f"  ERROR {case_name:16s} driver_get")
            traceback.print_exc()
            failed += 1

        # Path 2: producer -> consumer actor (no driver fetch).
        #   Consumer receives both the produced table and the expected
        #   ground-truth (also transferred through the flight path).
        try:
            ref = producer.make.remote(case_name)
            result = ray.get(consumer.check_equal.remote(ref, expected_table))
            ok = check(f"{case_name:16s} actor_actor", result)
            passed += ok
            failed += not ok
        except Exception:
            print(f"  ERROR {case_name:16s} actor_actor")
            traceback.print_exc()
            failed += 1

        # Path 3: producer -> consumer -> driver (chained).
        try:
            ref = producer.make.remote(case_name)
            round_trip = ray.get(consumer.passthrough.remote(ref))
            ok = check(f"{case_name:16s} chained", _compare(round_trip, expected_table))
            passed += ok
            failed += not ok
        except Exception:
            print(f"  ERROR {case_name:16s} chained")
            traceback.print_exc()
            failed += 1

        # Path 4: ray.put -> ray.get (direct).
        try:
            ref = ray.put(expected_table)
            got = ray.get(ref)
            ok = check(f"{case_name:16s} put_get", _compare(got, expected_table))
            passed += ok
            failed += not ok
        except Exception:
            print(f"  ERROR {case_name:16s} put_get")
            traceback.print_exc()
            failed += 1

    print()
    print(f"Passed: {passed}   Failed: {failed}")
    ray.shutdown()
    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run_suite() else 1)
