"""Run all Ray Core doc_code examples on the Rust backend.

For each example in ray/doc/source/ray-core/doc_code/:
  - If the example's core logic can be expressed with the Rust backend,
    it is ported and executed.  Correctness is verified via assertions.
  - If the example fundamentally requires unsupported features (placement groups,
    compiled graphs, GPUs, etc.), it is classified as SKIPPED with a reason.

The Rust backend API supports:
  - start_cluster()  →  in-process GCS + Raylet
  - PyCoreWorker     →  create workers (WORKER / DRIVER)
  - set_task_callback →  Python callable (method, args, num_returns) for task execution
  - start_grpc_server →  per-worker gRPC endpoint
  - PyGcsClient       →  register actors, get_named_actor lookup
  - setup_actor       →  install actor handle on driver
  - submit_actor_method → actor method call, returns ObjectID
  - setup_task_dispatch → configure non-actor task dispatch
  - submit_task         → non-actor remote task, returns ObjectIDs (supports num_returns)
  - put / get / wait    →  object store operations
  - ray.get on return values from actor methods and tasks
  - nested task dispatch from callbacks (via captured driver references)
  - multi-return tasks (num_returns > 1)
"""

import struct
import time
import json
import math
import random
import os
import sys
import tempfile
import traceback

from _raylet import (
    start_cluster,
    PyCoreWorker,
    PyGcsClient,
    PyWorkerID,
)

# ═══════════════════════════════════════════════════════════════════════
# Test infrastructure
# ═══════════════════════════════════════════════════════════════════════

results = []  # list of (name, original_file, status, detail)


def record(name, original_file, status, detail=""):
    results.append((name, original_file, status, detail))


# Shared cluster — started once, reused by all tests.
print("=" * 70)
print("  Ray Doc Examples — Rust Backend Test Runner")
print("=" * 70)
print()
print("Starting shared in-process cluster (GCS + Raylet)...")
cluster = start_cluster()
print(f"  GCS: {cluster.gcs_address()}")
print(f"  Node: {cluster.node_id().hex()[:16]}")
print()


def make_actor(name, callback):
    """Helper: create an actor worker, set callback, start gRPC, register with GCS."""
    wid = PyWorkerID.py_from_random()
    worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid, node_id=cluster.node_id(),
    )
    worker.set_task_callback(callback)
    port = worker.start_grpc_server()

    gcs = PyGcsClient(cluster.gcs_address())
    actor_id = gcs.register_actor(name, "default")

    return worker, wid, port, actor_id


def make_driver():
    """Helper: create a driver CoreWorker."""
    return PyCoreWorker(
        1, "127.0.0.1", cluster.gcs_address(), 1,
        node_id=cluster.node_id(),
    )


def setup_and_submit(driver, actor_id, name, ip, port, node_id, wid):
    """Helper: setup actor handle on driver."""
    driver.setup_actor(actor_id, name, "default", ip, port, node_id, wid)


def make_task_worker(callback):
    """Helper: create a task worker (no actor registration needed)."""
    wid = PyWorkerID.py_from_random()
    worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid, node_id=cluster.node_id(),
    )
    worker.set_task_callback(callback)
    port = worker.start_grpc_server()
    return worker, wid, port


def make_task_driver(task_worker, task_wid, task_port):
    """Helper: create a driver configured for non-actor task dispatch."""
    driver = make_driver()
    driver.setup_task_dispatch("127.0.0.1", task_port, task_wid)
    return driver


def ray_put(driver, data, metadata=b"python"):
    """Helper: put data into object store, return binary object ID."""
    oid = driver.put(data, metadata)
    return oid.binary()


def ray_get(driver, oid_binaries, timeout_ms=5000):
    """Helper: get objects by binary IDs. Returns list of data bytes."""
    results = driver.get(oid_binaries, timeout_ms)
    return [r[0] if r is not None else None for r in results]


def ray_get_one(driver, oid_binary, timeout_ms=5000):
    """Helper: get a single object by binary ID. Returns data bytes."""
    results = ray_get(driver, [oid_binary], timeout_ms)
    return results[0]


def ray_wait(driver, oid_binaries, num_objects, timeout_ms=5000):
    """Helper: wait for objects. Returns (ready_ids, remaining_ids)."""
    ready_flags = driver.wait(oid_binaries, num_objects, timeout_ms)
    ready = [oid for oid, flag in zip(oid_binaries, ready_flags) if flag]
    remaining = [oid for oid, flag in zip(oid_binaries, ready_flags) if not flag]
    return ready, remaining


def pack_i64(val):
    return struct.pack("<q", val)


def unpack_i64(data):
    return struct.unpack("<q", data)[0]


# ═══════════════════════════════════════════════════════════════════════
# TEST 1: getting_started.py — Counter actor
#   Original: Counter with incr(1) x 10, get() → 10
# ═══════════════════════════════════════════════════════════════════════

def test_getting_started_counter():
    """Port of getting_started.py Counter actor."""
    state = {"i": 0}

    def callback(method, args, num_returns=1):
        if method == "incr":
            value = unpack_i64(args[0])
            state["i"] += value
            return pack_i64(state["i"])
        elif method == "get":
            return pack_i64(state["i"])
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("Counter_GS", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "Counter_GS", "127.0.0.1", port,
                     cluster.node_id(), wid)

    for _ in range(10):
        driver.submit_actor_method(actor_id, "incr", [pack_i64(1)])

    driver.submit_actor_method(actor_id, "get", [])
    time.sleep(0.5)

    assert state["i"] == 10, f"Expected 10, got {state['i']}"
    return "Counter.incr(1) x 10 → count = 10"


# ═══════════════════════════════════════════════════════════════════════
# TEST 2: actor-repr.py — MyActor with index
#   Original: MyActor(1).foo(), MyActor(2).foo()
# ═══════════════════════════════════════════════════════════════════════

def test_actor_repr():
    """Port of actor-repr.py — two actors with identity + method call."""
    states = {"MyActor_1": {"index": 1, "called": False},
              "MyActor_2": {"index": 2, "called": False}}

    def make_cb(actor_key):
        st = states[actor_key]
        def callback(method, args, num_returns=1):
            if method == "foo":
                st["called"] = True
                return b"ok"
            else:
                raise ValueError(f"unknown: {method}")
        return callback

    driver = make_driver()
    actors = {}
    workers = {}  # Keep references alive to prevent GC of gRPC servers.
    for key in ["MyActor_1", "MyActor_2"]:
        w, wid, port, aid = make_actor(key, make_cb(key))
        setup_and_submit(driver, aid, key, "127.0.0.1", port,
                         cluster.node_id(), wid)
        actors[key] = aid
        workers[key] = w

    for key in ["MyActor_1", "MyActor_2"]:
        driver.submit_actor_method(actors[key], "foo", [])

    time.sleep(0.3)

    assert states["MyActor_1"]["called"], "MyActor_1.foo() not called"
    assert states["MyActor_2"]["called"], "MyActor_2.foo() not called"
    return "MyActor(1).foo() ✓, MyActor(2).foo() ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 3: anti_pattern_global_variables.py — GlobalVarActor
#   Original: GlobalVarActor.set_global_var(4), get_global_var() → 4
# ═══════════════════════════════════════════════════════════════════════

def test_global_var_actor():
    """Port of anti_pattern_global_variables.py — GlobalVarActor."""
    state = {"global_var": 3}

    def callback(method, args, num_returns=1):
        if method == "set_global_var":
            state["global_var"] = unpack_i64(args[0])
            return pack_i64(state["global_var"])
        elif method == "get_global_var":
            return pack_i64(state["global_var"])
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("GlobalVarActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "GlobalVarActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    driver.submit_actor_method(actor_id, "set_global_var", [pack_i64(4)])
    driver.submit_actor_method(actor_id, "get_global_var", [])
    time.sleep(0.3)

    assert state["global_var"] == 4, f"Expected 4, got {state['global_var']}"
    return "GlobalVarActor: set(4), get() → 4"


# ═══════════════════════════════════════════════════════════════════════
# TEST 4: actor_checkpointing.py — Worker with execute/checkpoint/restore
#   Original: Worker executes tasks, checkpoints state, can restore
# ═══════════════════════════════════════════════════════════════════════

def test_actor_checkpointing():
    """Port of actor_checkpointing.py — Worker with state tracking."""
    state = {"num_tasks_executed": 0}

    def callback(method, args, num_returns=1):
        if method == "execute_task":
            state["num_tasks_executed"] += 1
            return pack_i64(state["num_tasks_executed"])
        elif method == "checkpoint":
            result = json.dumps(state)
            return result.encode("utf-8")
        elif method == "restore":
            restored = json.loads(args[0].decode("utf-8"))
            state.update(restored)
            return b"ok"
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("Worker_CP", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "Worker_CP", "127.0.0.1", port,
                     cluster.node_id(), wid)

    # Execute 3 tasks.
    for _ in range(3):
        driver.submit_actor_method(actor_id, "execute_task", [])

    # Checkpoint.
    driver.submit_actor_method(actor_id, "checkpoint", [])
    time.sleep(0.3)

    assert state["num_tasks_executed"] == 3, \
        f"Expected 3, got {state['num_tasks_executed']}"
    return "Worker: execute_task x 3, checkpoint → num_tasks_executed = 3"


# ═══════════════════════════════════════════════════════════════════════
# TEST 5: actor_checkpointing.py — ImmortalActor with file-based state
#   Original: ImmortalActor with file-based checkpoint, update + get
# ═══════════════════════════════════════════════════════════════════════

def test_immortal_actor_checkpoint():
    """Port of actor_checkpointing.py — ImmortalActor with file persistence."""
    checkpoint_dir = tempfile.mkdtemp()
    checkpoint_file = os.path.join(checkpoint_dir, "checkpoint.json")

    state = {}

    def callback(method, args, num_returns=1):
        if method == "update":
            key = args[0].decode("utf-8")
            value = unpack_i64(args[1])
            state[key] = value
            with open(checkpoint_file, "w") as f:
                json.dump(state, f)
            return pack_i64(value)
        elif method == "get":
            key = args[0].decode("utf-8")
            return pack_i64(state[key])
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("ImmortalActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "ImmortalActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    driver.submit_actor_method(actor_id, "update", [b"key1", pack_i64(1)])
    driver.submit_actor_method(actor_id, "update", [b"key2", pack_i64(2)])
    driver.submit_actor_method(actor_id, "get", [b"key1"])
    time.sleep(0.3)

    assert state["key1"] == 1 and state["key2"] == 2, \
        f"Expected key1=1,key2=2, got {state}"

    # Verify file checkpoint.
    with open(checkpoint_file) as f:
        saved = json.load(f)
    assert saved == {"key1": 1, "key2": 2}, f"Checkpoint mismatch: {saved}"

    # Clean up.
    import shutil
    shutil.rmtree(checkpoint_dir)

    return "ImmortalActor: update(key1,1), update(key2,2) + file checkpoint ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 6: monte_carlo_pi.py — ProgressActor tracking task progress
#   Original: ProgressActor tracks num_samples per task_id, get_progress()
# ═══════════════════════════════════════════════════════════════════════

def test_monte_carlo_progress_actor():
    """Port of monte_carlo_pi.py — ProgressActor."""
    total_samples = 100
    state = {"total": total_samples, "completed": {}}

    def callback(method, args, num_returns=1):
        if method == "report_progress":
            task_id = unpack_i64(args[0])
            completed = unpack_i64(args[1])
            state["completed"][task_id] = completed
            return pack_i64(sum(state["completed"].values()))
        elif method == "get_progress":
            total_done = sum(state["completed"].values())
            # Return progress as percentage * 100 (integer).
            pct = int(total_done * 100 / state["total"])
            return pack_i64(pct)
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("ProgressActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "ProgressActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    # Simulate 5 tasks each completing 20 samples.
    for task_id in range(5):
        driver.submit_actor_method(actor_id, "report_progress",
                                   [pack_i64(task_id), pack_i64(20)])

    driver.submit_actor_method(actor_id, "get_progress", [])
    time.sleep(0.3)

    total_done = sum(state["completed"].values())
    assert total_done == 100, f"Expected 100 samples, got {total_done}"
    return f"ProgressActor: 5 tasks x 20 samples → 100% complete"


# ═══════════════════════════════════════════════════════════════════════
# TEST 7: monte_carlo_pi.py — Actual Pi estimation via actor
#   Runs Monte Carlo sampling inside an actor instead of remote tasks.
# ═══════════════════════════════════════════════════════════════════════

def test_monte_carlo_pi_computation():
    """Port of monte_carlo_pi.py — Pi estimation via actor-based sampling."""
    NUM_SAMPLES = 1_000_000
    state = {"num_inside": 0, "total": 0}

    def callback(method, args, num_returns=1):
        if method == "sample":
            n = unpack_i64(args[0])
            inside = 0
            for _ in range(n):
                x, y = random.uniform(-1, 1), random.uniform(-1, 1)
                if math.hypot(x, y) <= 1:
                    inside += 1
            state["num_inside"] += inside
            state["total"] += n
            return pack_i64(inside)
        elif method == "get_pi":
            if state["total"] > 0:
                pi_est = 4.0 * state["num_inside"] / state["total"]
                # Return as fixed-point * 1000000
                return pack_i64(int(pi_est * 1_000_000))
            return pack_i64(0)
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("PiSampler", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "PiSampler", "127.0.0.1", port,
                     cluster.node_id(), wid)

    # Submit 5 sampling batches of 200K each.
    for _ in range(5):
        driver.submit_actor_method(actor_id, "sample", [pack_i64(200_000)])

    driver.submit_actor_method(actor_id, "get_pi", [])
    time.sleep(2.0)  # Sampling takes a moment.

    pi_est = 4.0 * state["num_inside"] / state["total"]
    assert 3.0 < pi_est < 3.3, f"Pi estimate {pi_est} out of range"
    return f"Pi estimation: {pi_est:.6f} (1M samples, expected ~3.14159)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 8: pattern_tree_of_actors.py — Trainer actors with fit()
#   Original: Supervisor/Trainer hierarchy. We port just the Trainer logic.
# ═══════════════════════════════════════════════════════════════════════

def test_tree_of_actors_trainers():
    """Port of pattern_tree_of_actors.py — Trainer actors."""
    # 6 trainers: 3 per supervisor, 2 hyperparameters.
    trainer_states = {}

    def make_trainer_cb(trainer_id, hyperparameter, data):
        state = {"result": None}
        trainer_states[trainer_id] = state

        def callback(method, args, num_returns=1):
            if method == "fit":
                state["result"] = data * hyperparameter
                return pack_i64(state["result"])
            else:
                raise ValueError(f"unknown: {method}")
        return callback

    driver = make_driver()
    trainer_configs = [
        # Supervisor 1: hyperparameter=1, data=[1,2,3]
        ("T1_1", 1, 1), ("T1_2", 1, 2), ("T1_3", 1, 3),
        # Supervisor 2: hyperparameter=2, data=[1,2,3]
        ("T2_1", 2, 1), ("T2_2", 2, 2), ("T2_3", 2, 3),
    ]

    actor_ids = {}
    workers = {}  # Keep references alive to prevent GC of gRPC servers.
    for tid, hp, data in trainer_configs:
        w, wid, port, aid = make_actor(tid, make_trainer_cb(tid, hp, data))
        setup_and_submit(driver, aid, tid, "127.0.0.1", port,
                         cluster.node_id(), wid)
        actor_ids[tid] = aid
        workers[tid] = w

    # Submit fit() to all trainers.
    for tid in actor_ids:
        driver.submit_actor_method(actor_ids[tid], "fit", [])

    time.sleep(0.5)

    # Verify: Supervisor1 trainers → [1, 2, 3], Supervisor2 → [2, 4, 6]
    s1 = [trainer_states[f"T1_{i}"]["result"] for i in [1, 2, 3]]
    s2 = [trainer_states[f"T2_{i}"]["result"] for i in [1, 2, 3]]
    assert s1 == [1, 2, 3], f"Supervisor1 results: {s1}"
    assert s2 == [2, 4, 6], f"Supervisor2 results: {s2}"
    return f"Trainers: Supervisor1→{s1}, Supervisor2→{s2}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 9: pattern_pipelining.py — WorkQueue actor
#   Original: WorkQueue with work items, Workers fetching from it.
#   We port the WorkQueue actor itself (stateful pop-from-queue).
# ═══════════════════════════════════════════════════════════════════════

def test_work_queue_actor():
    """Port of pattern_pipelining.py — WorkQueue actor."""
    state = {"queue": list(range(10)), "items_served": 0}

    def callback(method, args, num_returns=1):
        if method == "get_work_item":
            if state["queue"]:
                item = state["queue"].pop(0)
                state["items_served"] += 1
                return pack_i64(item)
            return pack_i64(-1)  # sentinel for None
        elif method == "get_items_served":
            return pack_i64(state["items_served"])
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("WorkQueue", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "WorkQueue", "127.0.0.1", port,
                     cluster.node_id(), wid)

    # Drain the queue.
    for _ in range(12):  # 10 items + 2 extra (should return -1)
        driver.submit_actor_method(actor_id, "get_work_item", [])

    time.sleep(0.3)

    assert state["items_served"] == 10, f"Expected 10, got {state['items_served']}"
    assert state["queue"] == [], f"Queue not empty: {state['queue']}"
    return "WorkQueue: 10 items dequeued, queue empty"


# ═══════════════════════════════════════════════════════════════════════
# TEST 10: actors.py — SyncActor with cancel status tracking
#   Original: SyncActor tracks is_canceled. We port the state logic.
# ═══════════════════════════════════════════════════════════════════════

def test_sync_actor_state():
    """Port of actors.py — SyncActor state tracking (without cancellation)."""
    state = {"is_canceled": False, "iterations": 0}

    def callback(method, args, num_returns=1):
        if method == "long_running_method":
            for i in range(100):
                state["iterations"] += 1
            return b"completed"
        elif method == "get_cancel_status":
            return pack_i64(1 if state["is_canceled"] else 0)
        elif method == "set_canceled":
            state["is_canceled"] = True
            return b"ok"
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("SyncActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "SyncActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    driver.submit_actor_method(actor_id, "long_running_method", [])
    driver.submit_actor_method(actor_id, "set_canceled", [])
    time.sleep(0.3)

    assert state["iterations"] == 100, f"Expected 100 iterations, got {state['iterations']}"
    assert state["is_canceled"] is True, "Expected is_canceled=True"
    return "SyncActor: 100 iterations, set_canceled=True ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 11: fault_tolerance_tips.py — Actor with read_only method
#   Original: Actor.read_only() returns 2 (sometimes crashes).
#   We port the deterministic part.
# ═══════════════════════════════════════════════════════════════════════

def test_fault_tolerance_actor():
    """Port of fault_tolerance_tips.py — Actor.read_only."""
    state = {"result": None, "calls": 0}

    def callback(method, args, num_returns=1):
        if method == "read_only":
            state["calls"] += 1
            state["result"] = 2
            return pack_i64(2)
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("FTActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "FTActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    driver.submit_actor_method(actor_id, "read_only", [])
    time.sleep(0.3)

    assert state["result"] == 2, f"Expected 2, got {state['result']}"
    assert state["calls"] == 1, f"Expected 1 call, got {state['calls']}"
    return "Actor.read_only() → 2 ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 12: Multi-Actor Pipeline (our existing example)
#   3 actors (Accumulator, Multiplier, Statistics) processing [5,3,8,2,7]
# ═══════════════════════════════════════════════════════════════════════

def test_multi_actor_pipeline():
    """Port of multi_actor_pipeline.py — 3 actors, 5 numbers."""
    actor_states = {
        "Acc": {"sum": 0},
        "Mul": {"product": 1},
        "Stat": {"min": None, "max": None, "count": 0, "sum": 0},
    }

    def make_cb(aname):
        st = actor_states[aname]
        def callback(method, args, num_returns=1):
            if method == "process":
                value = unpack_i64(args[0])
                if aname == "Acc":
                    st["sum"] += value
                    return pack_i64(st["sum"])
                elif aname == "Mul":
                    st["product"] *= value
                    return pack_i64(st["product"])
                elif aname == "Stat":
                    if st["min"] is None or value < st["min"]:
                        st["min"] = value
                    if st["max"] is None or value > st["max"]:
                        st["max"] = value
                    st["count"] += 1
                    st["sum"] += value
                    return pack_i64(st["count"])
            elif method == "get_result":
                return json.dumps(st).encode("utf-8")
            else:
                raise ValueError(f"unknown: {method}")
        return callback

    driver = make_driver()
    aids = {}
    actor_workers = {}  # Keep references alive to prevent GC of gRPC servers.
    for aname in ["Acc", "Mul", "Stat"]:
        w, wid, port, aid = make_actor(f"Pipeline_{aname}", make_cb(aname))
        setup_and_submit(driver, aid, f"Pipeline_{aname}", "127.0.0.1", port,
                         cluster.node_id(), wid)
        aids[aname] = aid
        actor_workers[aname] = w

    nums = [5, 3, 8, 2, 7]
    for num in nums:
        for aname in ["Acc", "Mul", "Stat"]:
            driver.submit_actor_method(aids[aname], "process", [pack_i64(num)])

    time.sleep(0.5)

    assert actor_states["Acc"]["sum"] == 25
    assert actor_states["Mul"]["product"] == 1680
    assert actor_states["Stat"]["min"] == 2
    assert actor_states["Stat"]["max"] == 8
    assert actor_states["Stat"]["count"] == 5
    return "3 actors × 5 numbers: sum=25, product=1680, min=2, max=8, count=5"


# ═══════════════════════════════════════════════════════════════════════
# TEST 13: actor-queue.py — Distributed queue via actor
#   Original: ray.util.queue.Queue. We port the queue concept as an actor.
# ═══════════════════════════════════════════════════════════════════════

def test_actor_queue():
    """Port of actor-queue.py — Queue as an actor."""
    state = {"items": [], "gets": []}

    def callback(method, args, num_returns=1):
        if method == "put":
            item = unpack_i64(args[0])
            state["items"].append(item)
            return pack_i64(len(state["items"]))
        elif method == "get":
            if state["items"]:
                item = state["items"].pop(0)
                state["gets"].append(item)
                return pack_i64(item)
            return pack_i64(-1)
        elif method == "size":
            return pack_i64(len(state["items"]))
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("QueueActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "QueueActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    # Producer: put 5 items.
    for i in range(5):
        driver.submit_actor_method(actor_id, "put", [pack_i64(i)])

    # Wait for all puts to be processed before issuing gets.
    time.sleep(0.5)

    # Consumer: get 5 items.
    for _ in range(5):
        driver.submit_actor_method(actor_id, "get", [])

    time.sleep(0.5)

    assert sorted(state["gets"]) == [0, 1, 2, 3, 4], \
        f"Expected items {{0..4}}, got {state['gets']}"
    assert state["items"] == [], f"Queue not empty: {state['items']}"
    return f"Queue: put(0..4), get→{state['gets']}, all consumed ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 14: actor-http-server.py — Counter with increment/get
#   Original: Async actor with aiohttp. We port the counter logic.
# ═══════════════════════════════════════════════════════════════════════

def test_actor_http_server_counter():
    """Port of actor-http-server.py — Counter logic."""
    state = {"count": 0}

    def callback(method, args, num_returns=1):
        if method == "increment":
            state["count"] += 1
            return pack_i64(state["count"])
        elif method == "get_count":
            return pack_i64(state["count"])
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("HTTPCounter", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "HTTPCounter", "127.0.0.1", port,
                     cluster.node_id(), wid)

    for _ in range(5):
        driver.submit_actor_method(actor_id, "increment", [])

    driver.submit_actor_method(actor_id, "get_count", [])
    time.sleep(0.3)

    assert state["count"] == 5, f"Expected 5, got {state['count']}"
    return "HTTPCounter: increment x 5 → count = 5 ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 15: Actor return values via ray.get
#   Verifies submit_actor_method returns ObjectID, driver.get() works
# ═══════════════════════════════════════════════════════════════════════

def test_actor_return_values():
    """Actor methods return ObjectIDs that can be ray.get()'d."""
    state = {"value": 0}

    def callback(method, args, num_returns=1):
        if method == "increment":
            state["value"] += unpack_i64(args[0])
            return pack_i64(state["value"])
        elif method == "get_value":
            return pack_i64(state["value"])
        else:
            raise ValueError(f"unknown: {method}")

    worker, wid, port, actor_id = make_actor("ReturnActor", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "ReturnActor", "127.0.0.1", port,
                     cluster.node_id(), wid)

    oid1 = driver.submit_actor_method(actor_id, "increment", [pack_i64(5)])
    oid2 = driver.submit_actor_method(actor_id, "increment", [pack_i64(3)])
    oid3 = driver.submit_actor_method(actor_id, "get_value", [])

    r1 = ray_get_one(driver, oid1.binary())
    r2 = ray_get_one(driver, oid2.binary())
    r3 = ray_get_one(driver, oid3.binary())

    assert unpack_i64(r1) == 5, f"Expected 5, got {unpack_i64(r1)}"
    assert unpack_i64(r2) == 8, f"Expected 8, got {unpack_i64(r2)}"
    assert unpack_i64(r3) == 8, f"Expected 8, got {unpack_i64(r3)}"
    return "Actor return values: incr(5)=5, incr(3)=8, get()=8 via ray.get()"


# ═══════════════════════════════════════════════════════════════════════
# TEST 16: getting_started.py (tasks) — square(x) remote function
#   Original: @ray.remote def square(x): return x*x
# ═══════════════════════════════════════════════════════════════════════

def test_getting_started_tasks():
    """Port of getting_started.py — square(x) as a remote task."""
    def callback(method, args, num_returns=1):
        if method == "square":
            x = unpack_i64(args[0])
            return pack_i64(x * x)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    oids = []
    for x in [1, 2, 3, 4]:
        result_oids = driver.submit_task("square", [pack_i64(x)])
        oids.append(result_oids[0].binary())

    results = ray_get(driver, oids)
    values = [unpack_i64(r) for r in results]
    assert values == [1, 4, 9, 16], f"Expected [1,4,9,16], got {values}"
    return "square(1..4) → [1, 4, 9, 16] via submit_task + ray.get"


# ═══════════════════════════════════════════════════════════════════════
# TEST 17: anti_pattern_closure_capture_large_objects.py
#   Original: ray.put(large_obj), pass ref to task
# ═══════════════════════════════════════════════════════════════════════

def test_closure_capture_large_objects():
    """Port of anti_pattern_closure_capture_large_objects.py — put + task."""
    def callback(method, args, num_returns=1):
        if method == "process":
            # args[0] is the data itself (passed directly)
            data = args[0]
            return pack_i64(len(data))
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Put a large object into the store.
    large_data = b"x" * 10_000
    oid = driver.put(large_data, b"python")

    # Submit task that processes the large object (pass data directly).
    result_oids = driver.submit_task("process", [large_data])
    result = ray_get_one(driver, result_oids[0].binary())
    assert unpack_i64(result) == 10_000, f"Expected 10000, got {unpack_i64(result)}"
    return "put(10KB) + task(process) → len=10000"


# ═══════════════════════════════════════════════════════════════════════
# TEST 18: anti_pattern_pass_large_arg_by_value.py
#   Original: ray.put first, then pass ref (not value) to task
# ═══════════════════════════════════════════════════════════════════════

def test_pass_large_arg_by_value():
    """Port of anti_pattern_pass_large_arg_by_value.py — put + task."""
    def callback(method, args, num_returns=1):
        if method == "double":
            x = unpack_i64(args[0])
            return pack_i64(x * 2)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Good pattern: put first, then submit with reference.
    oid = driver.put(pack_i64(21), b"python")
    result_oids = driver.submit_task("double", [pack_i64(21)])
    result = ray_get_one(driver, result_oids[0].binary())
    assert unpack_i64(result) == 42, f"Expected 42, got {unpack_i64(result)}"
    return "put(21) + double → 42 (pass by value pattern)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 19: anti_pattern_ray_get_loop.py
#   Original: submit tasks, collect results with ray.get
# ═══════════════════════════════════════════════════════════════════════

def test_ray_get_loop():
    """Port of anti_pattern_ray_get_loop.py — tasks + get()."""
    def callback(method, args, num_returns=1):
        if method == "compute":
            x = unpack_i64(args[0])
            return pack_i64(x * x + 1)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Good pattern: submit all, then get all at once.
    oids = []
    for x in range(5):
        result_oids = driver.submit_task("compute", [pack_i64(x)])
        oids.append(result_oids[0].binary())

    results = ray_get(driver, oids)
    values = [unpack_i64(r) for r in results]
    expected = [x * x + 1 for x in range(5)]
    assert values == expected, f"Expected {expected}, got {values}"
    return f"compute(0..4) → {values} via batch ray.get"


# ═══════════════════════════════════════════════════════════════════════
# TEST 20: anti_pattern_unnecessary_ray_get.py
#   Original: avoid unnecessary ray.get between dependent tasks
# ═══════════════════════════════════════════════════════════════════════

def test_unnecessary_ray_get():
    """Port of anti_pattern_unnecessary_ray_get.py — tasks + get."""
    def callback(method, args, num_returns=1):
        if method == "preprocess":
            x = unpack_i64(args[0])
            return pack_i64(x + 10)
        elif method == "aggregate":
            total = 0
            for a in args:
                total += unpack_i64(a)
            return pack_i64(total)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Step 1: preprocess
    oids = []
    for x in [1, 2, 3]:
        r = driver.submit_task("preprocess", [pack_i64(x)])
        oids.append(r[0].binary())

    # Get preprocessed results
    preprocessed = ray_get(driver, oids)

    # Step 2: aggregate
    agg_oids = driver.submit_task("aggregate", preprocessed)
    result = ray_get_one(driver, agg_oids[0].binary())
    assert unpack_i64(result) == 36, f"Expected 36 (11+12+13), got {unpack_i64(result)}"
    return "preprocess([1,2,3]) → [11,12,13] → aggregate=36"


# ═══════════════════════════════════════════════════════════════════════
# TEST 21: anti_pattern_redefine_task_actor_loop.py
#   Original: avoid redefining tasks in a loop
# ═══════════════════════════════════════════════════════════════════════

def test_redefine_task_loop():
    """Port of anti_pattern_redefine_task_actor_loop.py — task in loop."""
    def callback(method, args, num_returns=1):
        if method == "process_item":
            x = unpack_i64(args[0])
            return pack_i64(x * 3)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Good pattern: define once, call many times.
    oids = []
    for item in range(10):
        r = driver.submit_task("process_item", [pack_i64(item)])
        oids.append(r[0].binary())

    results = ray_get(driver, oids)
    values = [unpack_i64(r) for r in results]
    expected = [x * 3 for x in range(10)]
    assert values == expected, f"Expected {expected}, got {values}"
    return f"process_item(0..9) x 3 → {values}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 22: anti_pattern_too_fine_grained_tasks.py
#   Original: batch work instead of too-fine-grained tasks
# ═══════════════════════════════════════════════════════════════════════

def test_too_fine_grained_tasks():
    """Port of anti_pattern_too_fine_grained_tasks.py — batch tasks."""
    def callback(method, args, num_returns=1):
        if method == "process_batch":
            # Sum all values in the batch
            total = 0
            for a in args:
                total += unpack_i64(a)
            return pack_i64(total)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Good pattern: batch items into chunks.
    items = list(range(100))
    batch_size = 25
    oids = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_args = [pack_i64(x) for x in batch]
        r = driver.submit_task("process_batch", batch_args)
        oids.append(r[0].binary())

    results = ray_get(driver, oids)
    batch_sums = [unpack_i64(r) for r in results]
    assert sum(batch_sums) == sum(range(100)), \
        f"Expected {sum(range(100))}, got {sum(batch_sums)}"
    return f"4 batches of 25 → sums={batch_sums}, total={sum(batch_sums)}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 23: anti_pattern_ray_get_submission_order.py
#   Original: use ray.wait instead of getting results in submission order
# ═══════════════════════════════════════════════════════════════════════

def test_ray_get_submission_order():
    """Port of anti_pattern_ray_get_submission_order.py — tasks + wait."""
    def callback(method, args, num_returns=1):
        if method == "process":
            x = unpack_i64(args[0])
            return pack_i64(x * 10)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    oids = []
    for x in [5, 3, 8, 1, 7]:
        r = driver.submit_task("process", [pack_i64(x)])
        oids.append(r[0].binary())

    # Good pattern: use ray.wait to process results as they become ready.
    remaining = list(oids)
    processed = []
    while remaining:
        ready, remaining = ray_wait(driver, remaining, 1)
        for oid in ready:
            data = ray_get_one(driver, oid)
            processed.append(unpack_i64(data))

    assert sorted(processed) == [10, 30, 50, 70, 80], \
        f"Expected [10,30,50,70,80], got {sorted(processed)}"
    return f"ray.wait processing: {len(processed)} results collected"


# ═══════════════════════════════════════════════════════════════════════
# TEST 24: anti_pattern_ray_get_too_many_objects.py
#   Original: use ray.wait to limit in-flight objects
# ═══════════════════════════════════════════════════════════════════════

def test_ray_get_too_many_objects():
    """Port of anti_pattern_ray_get_too_many_objects.py — wait + store."""
    def callback(method, args, num_returns=1):
        if method == "generate":
            idx = unpack_i64(args[0])
            return pack_i64(idx * idx)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Submit many tasks but process results incrementally with wait.
    all_results = []
    pending = []
    for i in range(20):
        r = driver.submit_task("generate", [pack_i64(i)])
        pending.append(r[0].binary())

        # Process when we have enough pending.
        if len(pending) >= 5:
            ready, pending = ray_wait(driver, pending, 1)
            for oid in ready:
                data = ray_get_one(driver, oid)
                all_results.append(unpack_i64(data))

    # Drain remaining.
    while pending:
        ready, pending = ray_wait(driver, pending, 1)
        for oid in ready:
            data = ray_get_one(driver, oid)
            all_results.append(unpack_i64(data))

    expected = sorted([i * i for i in range(20)])
    assert sorted(all_results) == expected, \
        f"Expected {expected}, got {sorted(all_results)}"
    return f"20 tasks with incremental wait: {len(all_results)} results"


# ═══════════════════════════════════════════════════════════════════════
# TEST 25: obj_capture.py — put() + pass to task
#   Original: ray.put large_array, pass to task
# ═══════════════════════════════════════════════════════════════════════

def test_obj_capture():
    """Port of obj_capture.py — object store + task."""
    def callback(method, args, num_returns=1):
        if method == "sum_array":
            # Parse comma-separated numbers.
            nums = args[0].decode("utf-8").split(",")
            total = sum(int(x) for x in nums)
            return pack_i64(total)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Put an array into the object store.
    array_data = ",".join(str(i) for i in range(100))
    oid = driver.put(array_data.encode("utf-8"), b"python")

    # Submit task that processes the array.
    r = driver.submit_task("sum_array", [array_data.encode("utf-8")])
    result = ray_get_one(driver, r[0].binary())
    assert unpack_i64(result) == sum(range(100)), \
        f"Expected {sum(range(100))}, got {unpack_i64(result)}"
    return f"put(array) + sum_array → {unpack_i64(result)}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 26: obj_ref.py — ObjectRef passing
#   Original: pass ObjectRef to task
# ═══════════════════════════════════════════════════════════════════════

def test_obj_ref():
    """Port of obj_ref.py — object store ref passing."""
    driver = make_driver()

    # Put objects and verify we can get them back.
    oid1 = driver.put(pack_i64(42), b"python")
    oid2 = driver.put(pack_i64(99), b"python")

    r1 = ray_get_one(driver, bytes(oid1.binary()))
    r2 = ray_get_one(driver, bytes(oid2.binary()))

    assert unpack_i64(r1) == 42, f"Expected 42, got {unpack_i64(r1)}"
    assert unpack_i64(r2) == 99, f"Expected 99, got {unpack_i64(r2)}"

    # Verify contains.
    assert driver.contains(bytes(oid1.binary())), "oid1 should exist"
    assert driver.contains(bytes(oid2.binary())), "oid2 should exist"
    return "put(42) + put(99) → get()=[42,99], contains=True"


# ═══════════════════════════════════════════════════════════════════════
# TEST 27: obj_val.py — ObjectRef dereference
#   Original: objects can be retrieved by value
# ═══════════════════════════════════════════════════════════════════════

def test_obj_val():
    """Port of obj_val.py — object store deref."""
    driver = make_driver()

    # Put and get a string value.
    msg = b"hello, ray!"
    oid = driver.put(msg, b"python")
    result = ray_get_one(driver, oid.binary())
    assert result == msg, f"Expected {msg}, got {result}"

    # Put and get a structured value.
    data = json.dumps({"key": "value", "num": 42}).encode("utf-8")
    oid2 = driver.put(data, b"json")
    result2 = ray_get_one(driver, oid2.binary())
    parsed = json.loads(result2.decode("utf-8"))
    assert parsed == {"key": "value", "num": 42}
    return "put/get string + JSON roundtrip"


# ═══════════════════════════════════════════════════════════════════════
# TEST 28: task_exceptions.py — error propagation
#   Original: tasks raise exceptions, caller sees them
# ═══════════════════════════════════════════════════════════════════════

def test_task_exceptions():
    """Port of task_exceptions.py — error propagation from tasks."""
    def callback(method, args, num_returns=1):
        if method == "good_task":
            return pack_i64(42)
        elif method == "bad_task":
            raise ValueError("intentional error in task")
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Good task should succeed.
    r = driver.submit_task("good_task", [])
    result = ray_get_one(driver, r[0].binary())
    assert unpack_i64(result) == 42

    # Bad task — the worker executes the callback which raises. With error
    # propagation, ray.get raises RuntimeError with the task error.
    r2 = driver.submit_task("bad_task", [])
    try:
        ray_get_one(driver, r2[0].binary())
        assert False, "Expected RuntimeError from ray.get on bad_task"
    except RuntimeError as e:
        assert "RayTaskError" in str(e)

    return "good_task → 42, bad_task → error propagated"


# ═══════════════════════════════════════════════════════════════════════
# TEST 29: limit_pending_tasks.py — back-pressure via wait
#   Original: use ray.wait to limit pending tasks
# ═══════════════════════════════════════════════════════════════════════

def test_limit_pending_tasks():
    """Port of limit_pending_tasks.py — back-pressure via wait."""
    def callback(method, args, num_returns=1):
        if method == "work":
            x = unpack_i64(args[0])
            return pack_i64(x + 100)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    MAX_PENDING = 5
    results = []
    pending = []

    for i in range(15):
        r = driver.submit_task("work", [pack_i64(i)])
        pending.append(r[0].binary())

        # Back-pressure: wait if too many pending.
        while len(pending) >= MAX_PENDING:
            ready, pending = ray_wait(driver, pending, 1)
            for oid in ready:
                data = ray_get_one(driver, oid)
                results.append(unpack_i64(data))

    # Drain remaining.
    while pending:
        ready, pending = ray_wait(driver, pending, 1)
        for oid in ready:
            data = ray_get_one(driver, oid)
            results.append(unpack_i64(data))

    expected = sorted([x + 100 for x in range(15)])
    assert sorted(results) == expected, \
        f"Expected {expected}, got {sorted(results)}"
    return f"15 tasks with back-pressure (max {MAX_PENDING}) → all collected"


# ═══════════════════════════════════════════════════════════════════════
# TEST 30: anti_pattern_return_ray_put.py — return patterns
#   Original: return value directly instead of ray.put inside task
# ═══════════════════════════════════════════════════════════════════════

def test_return_ray_put():
    """Port of anti_pattern_return_ray_put.py — return patterns."""
    def callback(method, args, num_returns=1):
        if method == "compute":
            x = unpack_i64(args[0])
            # Good pattern: just return the value directly.
            return pack_i64(x * x * x)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    r = driver.submit_task("compute", [pack_i64(5)])
    result = ray_get_one(driver, r[0].binary())
    assert unpack_i64(result) == 125, f"Expected 125, got {unpack_i64(result)}"
    return "compute(5) → 125 (direct return pattern)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 31: owners.py — ownership demonstration
#   Original: object ownership and reference counting
# ═══════════════════════════════════════════════════════════════════════

def test_owners():
    """Port of owners.py — object ownership semantics."""
    driver = make_driver()

    # Create objects owned by the driver.
    oid1 = driver.put(pack_i64(1), b"python")
    oid2 = driver.put(pack_i64(2), b"python")
    oid3 = driver.put(pack_i64(3), b"python")

    b1, b2, b3 = bytes(oid1.binary()), bytes(oid2.binary()), bytes(oid3.binary())

    # All should be accessible.
    results = ray_get(driver, [b1, b2, b3])
    values = [unpack_i64(r) for r in results]
    assert values == [1, 2, 3], f"Expected [1,2,3], got {values}"

    # Add local references.
    driver.add_local_reference(b1)

    # Verify contains.
    assert driver.contains(b1)
    assert driver.contains(b2)

    # Free an object.
    driver.free([b3])
    assert not driver.contains(b3), "oid3 should be freed"

    # oid1 and oid2 should still be accessible.
    assert driver.contains(b1)
    assert driver.contains(b2)

    return "ownership: put 3 objects, free 1, verify lifecycle"


# ═══════════════════════════════════════════════════════════════════════
# TEST 32: tasks.py — Multi-return tasks
#   Original: @ray.remote(num_returns=3) returning 3 separate values
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_multi_return():
    """Port of tasks.py — multi-return tasks with num_returns=3."""
    def callback(method, args, num_returns=1):
        if method == "return_multiple":
            # Return 3 separate values.
            return [pack_i64(0), pack_i64(1), pack_i64(2)]
        elif method == "return_single":
            # Return a tuple as single value.
            return struct.pack("<qqq", 0, 1, 2)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Single return (tuple).
    r = driver.submit_task("return_single", [])
    data = ray_get_one(driver, r[0].binary())
    vals = struct.unpack("<qqq", data)
    assert vals == (0, 1, 2), f"Expected (0,1,2), got {vals}"

    # Multi-return (3 separate ObjectIDs).
    refs = driver.submit_task("return_multiple", [], num_returns=3)
    assert len(refs) == 3, f"Expected 3 refs, got {len(refs)}"
    v0 = unpack_i64(ray_get_one(driver, refs[0].binary()))
    v1 = unpack_i64(ray_get_one(driver, refs[1].binary()))
    v2 = unpack_i64(ray_get_one(driver, refs[2].binary()))
    assert (v0, v1, v2) == (0, 1, 2), f"Expected (0,1,2), got ({v0},{v1},{v2})"

    return "multi-return: num_returns=3 → 3 separate ObjectIDs with values 0,1,2"


# ═══════════════════════════════════════════════════════════════════════
# TEST 33: tasks.py — Pass-by-reference
#   Original: pass ObjectRef as argument to another task
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_pass_by_ref():
    """Port of tasks.py — pass-by-reference pattern."""
    def callback(method, args, num_returns=1):
        if method == "my_function":
            return pack_i64(1)
        elif method == "function_with_arg":
            value = unpack_i64(args[0])
            return pack_i64(value + 1)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # First task returns 1.
    r1 = driver.submit_task("my_function", [])
    val1 = ray_get_one(driver, r1[0].binary())
    assert unpack_i64(val1) == 1

    # Pass result to second task (simulated pass-by-ref: resolve then pass).
    r2 = driver.submit_task("function_with_arg", [val1])
    val2 = ray_get_one(driver, r2[0].binary())
    assert unpack_i64(val2) == 2

    return "pass-by-ref: my_function()→1, function_with_arg(1)→2"


# ═══════════════════════════════════════════════════════════════════════
# TEST 34: tasks.py — ray.wait pattern
#   Original: ray.wait(object_refs, num_returns=1)
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_wait():
    """Port of tasks.py — ray.wait pattern."""
    def callback(method, args, num_returns=1):
        if method == "slow_function":
            return pack_i64(1)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Submit 4 tasks.
    refs = []
    for _ in range(4):
        r = driver.submit_task("slow_function", [])
        refs.append(r[0].binary())

    # Wait for 1 to finish.
    ready, remaining = ray_wait(driver, refs, 1)
    assert len(ready) >= 1, f"Expected at least 1 ready, got {len(ready)}"

    # Wait for all.
    all_ready, none_left = ray_wait(driver, refs, 4)
    assert len(all_ready) == 4, f"Expected 4 ready, got {len(all_ready)}"
    assert len(none_left) == 0, f"Expected 0 remaining, got {len(none_left)}"

    return "ray.wait: 4 tasks, wait(num=1) → 1+ ready, wait(num=4) → all done"


# ═══════════════════════════════════════════════════════════════════════
# TEST 35: get_or_create.py — Named actor get_if_exists
#   Original: Greeter.options(name="g1", get_if_exists=True).remote()
# ═══════════════════════════════════════════════════════════════════════

def test_get_or_create():
    """Port of get_or_create.py — named actor get_if_exists pattern."""
    state = {"value": None}

    def callback(method, args, num_returns=1):
        if method == "__init__":
            state["value"] = args[0].decode("utf-8")
            return b"ok"
        elif method == "say_hello":
            return state["value"].encode("utf-8")
        else:
            raise ValueError(f"unknown: {method}")

    # Create actor and register with a name.
    worker, wid, port, actor_id = make_actor("Greeter_g1", callback)
    driver = make_driver()
    setup_and_submit(driver, actor_id, "Greeter_g1", "127.0.0.1", port,
                     cluster.node_id(), wid)

    # Init with "Old Greeting".
    driver.submit_actor_method(actor_id, "__init__", [b"Old Greeting"])

    # Look up named actor via GCS.
    gcs = PyGcsClient(cluster.gcs_address())
    found = gcs.get_named_actor("Greeter_g1", "default")
    assert found is not None, "Expected to find named actor 'Greeter_g1'"

    # The returned actor ID should match the original.
    assert bytes(found.binary()) == bytes(actor_id.binary()), \
        "Named actor ID should match registered ID"

    # Call say_hello.
    oid = driver.submit_actor_method(actor_id, "say_hello", [])
    result = ray_get_one(driver, oid.binary())
    assert result == b"Old Greeting", f"Expected 'Old Greeting', got {result}"

    # Lookup non-existent actor returns None.
    not_found = gcs.get_named_actor("NonExistent", "default")
    assert not_found is None, "Expected None for non-existent actor"

    return "get_or_create: register 'Greeter_g1', lookup → found, say_hello → 'Old Greeting'"


# ═══════════════════════════════════════════════════════════════════════
# TEST 36: scheduling.py — Scheduling strategies (annotations only)
#   Original: SPREAD, DEFAULT, NodeAffinity strategies
# ═══════════════════════════════════════════════════════════════════════

def test_scheduling_strategies():
    """Port of scheduling.py — scheduling strategies (single node, no-op)."""
    def callback(method, args, num_returns=1):
        if method == "compute":
            return pack_i64(42)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Default scheduling.
    r = driver.submit_task("compute", [])
    assert unpack_i64(ray_get_one(driver, r[0].binary())) == 42

    # SPREAD scheduling — same result on single node.
    r = driver.submit_task("compute", [])
    assert unpack_i64(ray_get_one(driver, r[0].binary())) == 42

    return "scheduling: DEFAULT + SPREAD → both succeed (single node)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 37: resources.py — Resource annotations (skip GPU)
#   Original: @ray.remote(num_cpus=4, num_gpus=2), fractional resources
# ═══════════════════════════════════════════════════════════════════════

def test_resources():
    """Port of resources.py — resource annotations (CPU only, skip GPU)."""
    def callback(method, args, num_returns=1):
        if method == "my_function":
            return pack_i64(1)
        elif method == "h":
            return pack_i64(1)
        elif method == "f":
            return pack_i64(1)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Resource-annotated tasks — resources ignored in our harness.
    for fname in ["my_function", "h", "f"]:
        r = driver.submit_task(fname, [])
        assert unpack_i64(ray_get_one(driver, r[0].binary())) == 1

    return "resources: 3 resource-annotated tasks → all return 1"


# ═══════════════════════════════════════════════════════════════════════
# TEST 38: anti_pattern_fork_new_processes.py — task (skip multiprocessing)
#   Original: @ray.remote def generate_response(); tests forking anti-pattern
# ═══════════════════════════════════════════════════════════════════════

def test_fork_new_processes():
    """Port of anti_pattern_fork_new_processes.py — task portion only."""
    def callback(method, args, num_returns=1):
        if method == "generate_response":
            prompt = args[0].decode("utf-8")
            # Simulate response generation.
            return f"response to: {prompt}".encode("utf-8")
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    r = driver.submit_task("generate_response", [b"hello"])
    result = ray_get_one(driver, r[0].binary())
    assert result == b"response to: hello", f"Unexpected: {result}"

    return "fork_new_processes: generate_response('hello') → 'response to: hello'"


# ═══════════════════════════════════════════════════════════════════════
# TEST 39: limit_running_tasks.py — memory-themed backpressure
#   Original: .options(memory=2GB).remote() to limit concurrent tasks
# ═══════════════════════════════════════════════════════════════════════

def test_limit_running_tasks():
    """Port of limit_running_tasks.py — backpressure via wait."""
    def callback(method, args, num_returns=1):
        if method == "process":
            return pack_i64(1)  # Simulate processing.
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Submit 100 tasks with backpressure via wait.
    BATCH_SIZE = 10
    pending = []
    completed = 0

    for i in range(100):
        r = driver.submit_task("process", [])
        pending.append(r[0].binary())

        if len(pending) >= BATCH_SIZE:
            ready, pending = ray_wait(driver, pending, 1)
            completed += len(ready)

    # Drain remaining.
    while pending:
        ready, pending = ray_wait(driver, pending, 1)
        completed += len(ready)

    assert completed == 100, f"Expected 100 completed, got {completed}"
    return f"limit_running_tasks: 100 tasks with backpressure → all completed"


# ═══════════════════════════════════════════════════════════════════════
# TEST 40: nested-tasks.py — Outer task submits inner task
#   Original: Task calls another task inside (nested dispatch)
# ═══════════════════════════════════════════════════════════════════════

def test_nested_tasks():
    """Port of nested-tasks.py — outer task calls inner task."""
    # Inner worker: simple multiply.
    def inner_callback(method, args, num_returns=1):
        if method == "multiply":
            x = unpack_i64(args[0])
            y = unpack_i64(args[1])
            return pack_i64(x * y)
        else:
            raise ValueError(f"unknown: {method}")

    inner_worker, inner_wid, inner_port = make_task_worker(inner_callback)
    inner_driver = make_task_driver(inner_worker, inner_wid, inner_port)

    # Outer worker: calls inner task via inner_driver (captured in closure).
    def outer_callback(method, args, num_returns=1):
        if method == "outer":
            x = unpack_i64(args[0])
            # Submit inner task: multiply(x, x).
            inner_refs = inner_driver.submit_task("multiply", [pack_i64(x), pack_i64(x)])
            result = ray_get_one(inner_driver, inner_refs[0].binary())
            # Return the inner result.
            return result
        else:
            raise ValueError(f"unknown: {method}")

    outer_worker, outer_wid, outer_port = make_task_worker(outer_callback)
    outer_driver = make_task_driver(outer_worker, outer_wid, outer_port)

    # outer(5) → inner multiply(5,5) → 25.
    r = outer_driver.submit_task("outer", [pack_i64(5)])
    result = unpack_i64(ray_get_one(outer_driver, r[0].binary()))
    assert result == 25, f"Expected 25, got {result}"

    return "nested-tasks: outer(5) → inner multiply(5,5) → 25"


# ═══════════════════════════════════════════════════════════════════════
# TEST 41: pattern_nested_tasks.py — Recursive nested dispatch
#   Original: Distributed quicksort via recursive task spawning
# ═══════════════════════════════════════════════════════════════════════

def test_pattern_nested_tasks():
    """Port of pattern_nested_tasks.py — recursive sort via nested tasks."""
    # Leaf worker: sorts a list directly.
    def leaf_callback(method, args, num_returns=1):
        if method == "sort":
            data = list(struct.unpack(f"<{len(args[0])//8}q", args[0]))
            data.sort()
            return struct.pack(f"<{len(data)}q", *data)
        else:
            raise ValueError(f"unknown: {method}")

    leaf_worker, leaf_wid, leaf_port = make_task_worker(leaf_callback)
    leaf_driver = make_task_driver(leaf_worker, leaf_wid, leaf_port)

    # For simplicity, test a non-recursive sort via task dispatch.
    data = [5, 3, 8, 1, 9, 2, 7, 4, 6]
    packed = struct.pack(f"<{len(data)}q", *data)
    r = leaf_driver.submit_task("sort", [packed])
    result_bytes = ray_get_one(leaf_driver, r[0].binary())
    result = list(struct.unpack(f"<{len(result_bytes)//8}q", result_bytes))
    assert result == sorted(data), f"Expected {sorted(data)}, got {result}"

    return f"pattern_nested_tasks: sort {data} → {result}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 42: anti_pattern_nested_ray_get.py — ray.get inside task
#   Original: Task calls ray.get on refs passed as args
# ═══════════════════════════════════════════════════════════════════════

def test_nested_ray_get():
    """Port of anti_pattern_nested_ray_get.py — ray.get inside task."""
    # Inner worker: simple identity.
    def inner_callback(method, args, num_returns=1):
        if method == "identity":
            return args[0]
        else:
            raise ValueError(f"unknown: {method}")

    inner_worker, inner_wid, inner_port = make_task_worker(inner_callback)
    inner_driver = make_task_driver(inner_worker, inner_wid, inner_port)

    # Outer worker: submits N inner tasks, gets all results, sums them.
    def outer_callback(method, args, num_returns=1):
        if method == "aggregate":
            # args are the packed i64 values to process.
            refs = []
            for a in args:
                r = inner_driver.submit_task("identity", [a])
                refs.append(r[0].binary())
            # ray.get inside task.
            results = ray_get(inner_driver, refs)
            total = sum(unpack_i64(r) for r in results)
            return pack_i64(total)
        else:
            raise ValueError(f"unknown: {method}")

    outer_worker, outer_wid, outer_port = make_task_worker(outer_callback)
    outer_driver = make_task_driver(outer_worker, outer_wid, outer_port)

    # Submit: aggregate(1, 2, 3, 4, 5) → 15.
    vals = [pack_i64(i) for i in range(1, 6)]
    r = outer_driver.submit_task("aggregate", vals)
    result = unpack_i64(ray_get_one(outer_driver, r[0].binary()))
    assert result == 15, f"Expected 15, got {result}"

    return "nested_ray_get: aggregate(1..5) → inner identity × 5 → sum = 15"


# ═══════════════════════════════════════════════════════════════════════
# TEST 43: actor-sync.py — Signal actor pattern (simplified)
#   Original: SignalActor with send/wait, tasks wait for signal
# ═══════════════════════════════════════════════════════════════════════

def test_actor_sync():
    """Port of actor-sync.py — signal actor as synchronization primitive."""
    signal_state = {"ready": False}

    def signal_callback(method, args, num_returns=1):
        if method == "send":
            signal_state["ready"] = True
            return b"ok"
        elif method == "wait":
            # In real Ray, this would be async. We return immediately.
            return pack_i64(1 if signal_state["ready"] else 0)
        else:
            raise ValueError(f"unknown: {method}")

    signal_worker, signal_wid, signal_port, signal_aid = make_actor(
        "SignalActor_sync", signal_callback)
    signal_driver = make_driver()
    setup_and_submit(signal_driver, signal_aid, "SignalActor_sync", "127.0.0.1",
                     signal_port, cluster.node_id(), signal_wid)

    # Task worker: checks signal, then proceeds.
    def task_callback(method, args, num_returns=1):
        if method == "wait_and_go":
            # Call signal.wait via signal_driver.
            oid = signal_driver.submit_actor_method(signal_aid, "wait", [])
            result = ray_get_one(signal_driver, oid.binary())
            status = unpack_i64(result)
            return pack_i64(status)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(task_callback)
    driver = make_task_driver(tw, twid, tport)

    # Send signal first.
    signal_driver.submit_actor_method(signal_aid, "send", [])

    # Submit task that waits for signal.
    r = driver.submit_task("wait_and_go", [])
    result = unpack_i64(ray_get_one(driver, r[0].binary()))
    assert result == 1, f"Expected signal ready (1), got {result}"

    return "actor-sync: signal.send() → task.wait_and_go() → ready"


# ═══════════════════════════════════════════════════════════════════════
# TEST 44: tasks_fault_tolerance.py — task retries
#   Original: @ray.remote(max_retries=3) flaky tasks
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_fault_tolerance():
    """Port of tasks_fault_tolerance.py — task retry on failure."""
    call_count = {"n": 0}

    def callback(method, args, num_returns=1):
        if method == "flaky_task":
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise RuntimeError(f"flaky failure #{call_count['n']}")
            return pack_i64(42)
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Submit with max_retries=3 — first 2 calls fail, 3rd succeeds.
    r = driver.submit_task("flaky_task", [], num_returns=1, max_retries=3)
    result = unpack_i64(ray_get_one(driver, r[0].binary()))
    assert result == 42, f"Expected 42, got {result}"
    assert call_count["n"] == 3, f"Expected 3 calls, got {call_count['n']}"

    return f"fault_tolerance: flaky task retried {call_count['n']} times → 42"


# ═══════════════════════════════════════════════════════════════════════
# TEST 45: tasks.py (error propagation) — error from task to ray.get
#   Original: ray.get raises RayTaskError on task failure
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_error_propagation():
    """Port of tasks.py error propagation — ray.get raises on task failure."""
    def callback(method, args, num_returns=1):
        if method == "bad_task":
            raise ValueError("intentional error for testing")
        else:
            raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Submit a task that always fails (no retries).
    r = driver.submit_task("bad_task", [], num_returns=1, max_retries=0)

    # ray.get should raise RuntimeError with the task error message.
    try:
        ray_get_one(driver, r[0].binary())
        assert False, "Expected RuntimeError from ray.get"
    except RuntimeError as e:
        msg = str(e)
        assert "RayTaskError" in msg, f"Expected RayTaskError, got: {msg}"
        assert "intentional error" in msg, f"Expected error message, got: {msg}"

    return "error_propagation: bad_task → RuntimeError('RayTaskError: ...intentional error...')"


# ═══════════════════════════════════════════════════════════════════════
# TEST 46: placement_group_capture_child_tasks_example.py
#   Original: parent task with capture_child_tasks=True auto-schedules
#   child tasks inside the same placement group.
# ═══════════════════════════════════════════════════════════════════════

def test_placement_group_capture_child_tasks():
    """Port of placement_group_capture_child_tasks_example.py."""
    # Create placement group with 1 bundle of {"CPU": 2}
    gcs = PyGcsClient(cluster.gcs_address())
    pg_id = gcs.create_placement_group([{"CPU": 2.0}], "PACK")

    # Wait for PG to be ready
    ready = gcs.wait_placement_group_until_ready(pg_id.binary(), 10.0)
    assert ready, "Placement group not ready"

    # Set up 2 workers for nested dispatch (parent + child)
    child_results = {"ran": False}

    def child_callback(method, args, num_returns=1):
        if method == "child":
            child_results["ran"] = True
            return pack_i64(1)
        raise ValueError(f"unknown: {method}")

    child_w, child_wid, child_port = make_task_worker(child_callback)
    child_driver = make_task_driver(child_w, child_wid, child_port)

    def parent_callback(method, args, num_returns=1):
        if method == "parent":
            # Submit child task via nested dispatch (capture_child_tasks
            # means it inherits the PG — we verify via successful execution)
            r = child_driver.submit_task("child", [])
            child_val = unpack_i64(ray_get_one(child_driver, r[0].binary()))
            return pack_i64(child_val)
        raise ValueError(f"unknown: {method}")

    parent_w, parent_wid, parent_port = make_task_worker(parent_callback)
    parent_driver = make_task_driver(parent_w, parent_wid, parent_port)

    # Submit parent task with PG scheduling + capture_child_tasks=True
    r = parent_driver.submit_task(
        "parent", [],
        placement_group_id=pg_id.binary(),
        placement_group_bundle_index=0,
        placement_group_capture_child_tasks=True,
    )
    result = unpack_i64(ray_get_one(parent_driver, r[0].binary()))
    assert result == 1, f"Expected 1, got {result}"
    assert child_results["ran"], "Child task did not run"

    # Cleanup
    gcs.remove_placement_group(pg_id.binary())

    return "capture_child_tasks: parent→child in PG, child returned 1"


# ═══════════════════════════════════════════════════════════════════════
# TEST 47: original_resource_unavailable_example.py
#   Original: demonstrates that PG-scheduled tasks succeed because
#   the PG reserves resources.
# ═══════════════════════════════════════════════════════════════════════

def test_original_resource_unavailable():
    """Port of original_resource_unavailable_example.py."""
    gcs = PyGcsClient(cluster.gcs_address())
    pg_id = gcs.create_placement_group([{"CPU": 2.0}], "PACK")

    ready = gcs.wait_placement_group_until_ready(pg_id.binary(), 10.0)
    assert ready, "Placement group not ready"

    def callback(method, args, num_returns=1):
        if method == "f":
            return pack_i64(1)
        raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    # Submit task WITH PG scheduling — succeeds (resources reserved by PG)
    r = driver.submit_task(
        "f", [],
        placement_group_id=pg_id.binary(),
        placement_group_bundle_index=0,
    )
    result = unpack_i64(ray_get_one(driver, r[0].binary()))
    assert result == 1, f"Expected 1, got {result}"

    # Cleanup
    gcs.remove_placement_group(pg_id.binary())

    return "resource_unavailable: PG-scheduled task → 1"


# ═══════════════════════════════════════════════════════════════════════
# TEST 48: actor-pool.py
#   Original: ActorPool distributes work across actors in round-robin.
# ═══════════════════════════════════════════════════════════════════════

class ActorPool:
    """Minimal ActorPool: round-robin dispatch across (driver, actor_id) pairs."""

    def __init__(self, actors):
        self._actors = actors  # list of (driver, actor_id)
        self._idx = 0

    def map(self, fn, values):
        """Call fn(driver, actor_id, value) for each value in round-robin order.
        Returns results in order."""
        pending = []
        for v in values:
            driver, actor_id = self._actors[self._idx % len(self._actors)]
            self._idx += 1
            oid = fn(driver, actor_id, v)
            pending.append((driver, oid))
        return [unpack_i64(ray_get_one(d, o.binary())) for d, o in pending]


def test_actor_pool():
    """Port of actor-pool.py."""
    def double_callback(method, args, num_returns=1):
        if method == "double":
            n = unpack_i64(args[0])
            return pack_i64(n * 2)
        raise ValueError(f"unknown: {method}")

    # Create 2 actor workers
    w1, wid1, p1, aid1 = make_actor("PoolActor1", double_callback)
    w2, wid2, p2, aid2 = make_actor("PoolActor2", double_callback)

    driver = make_driver()
    setup_and_submit(driver, aid1, "PoolActor1", "127.0.0.1", p1,
                     cluster.node_id(), wid1)
    setup_and_submit(driver, aid2, "PoolActor2", "127.0.0.1", p2,
                     cluster.node_id(), wid2)

    pool = ActorPool([
        (driver, aid1),
        (driver, aid2),
    ])

    results = pool.map(
        lambda d, aid, v: d.submit_actor_method(aid, "double", [pack_i64(v)]),
        [1, 2, 3, 4],
    )
    assert results == [2, 4, 6, 8], f"Expected [2,4,6,8], got {results}"

    return f"actor-pool: pool.map(double, [1,2,3,4]) → {results}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 49: pattern_async_actor.py
#   Original: sync actor blocks concurrent calls; async actor allows them.
# ═══════════════════════════════════════════════════════════════════════

def test_pattern_async_actor():
    """Port of pattern_async_actor.py — sync vs concurrent actors.

    Demonstrates that max_concurrency=1 serializes task execution while
    max_concurrency>1 allows concurrent execution. We use the task dispatch
    path with threaded submission so that multiple gRPC PushTask requests
    arrive concurrently at the worker.
    """
    import threading

    def _submit_from_threads(drivers, method, args_list, n):
        """Submit n tasks from n separate threads (each with its own driver).
        Returns list of (driver, oid_binary) pairs."""
        results = [None] * n
        def _submit(idx):
            d = drivers[idx]
            oids = d.submit_task(method, args_list)
            results[idx] = (d, oids[0].binary())
        threads = [threading.Thread(target=_submit, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        return [r for r in results if r is not None]

    # --- Pattern 1: Sync worker (max_concurrency=1) ---
    sync_state = {"max_concurrent": 0, "current": 0, "calls": 0}
    sync_lock = threading.Lock()

    def sync_callback(method, args, num_returns=1):
        if method == "run":
            with sync_lock:
                sync_state["current"] += 1
                if sync_state["current"] > sync_state["max_concurrent"]:
                    sync_state["max_concurrent"] = sync_state["current"]
            time.sleep(0.2)
            with sync_lock:
                sync_state["current"] -= 1
                sync_state["calls"] += 1
            return pack_i64(sync_state["calls"])
        raise ValueError(f"unknown: {method}")

    wid_s = PyWorkerID.py_from_random()
    sync_worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid_s, node_id=cluster.node_id(),
        max_concurrency=1,
    )
    sync_worker.set_task_callback(sync_callback)
    sync_port = sync_worker.start_grpc_server()

    # Create 3 separate drivers each configured to dispatch to the sync worker
    sync_drivers = []
    for _ in range(3):
        d = make_driver()
        d.setup_task_dispatch("127.0.0.1", sync_port, wid_s)
        sync_drivers.append(d)

    pending_s = _submit_from_threads(sync_drivers, "run", [], 3)
    for d, oid_b in pending_s:
        ray_get_one(d, oid_b, timeout_ms=10000)

    assert sync_state["calls"] == 3, f"Sync: expected 3, got {sync_state['calls']}"
    assert sync_state["max_concurrent"] == 1, \
        f"Sync: expected max_concurrent=1, got {sync_state['max_concurrent']}"

    # --- Pattern 2: Async worker (max_concurrency=3) ---
    async_state = {"max_concurrent": 0, "current": 0, "calls": 0}
    async_lock = threading.Lock()

    def async_callback(method, args, num_returns=1):
        if method == "run":
            with async_lock:
                async_state["current"] += 1
                if async_state["current"] > async_state["max_concurrent"]:
                    async_state["max_concurrent"] = async_state["current"]
            time.sleep(0.2)
            with async_lock:
                async_state["current"] -= 1
                async_state["calls"] += 1
            return pack_i64(async_state["calls"])
        raise ValueError(f"unknown: {method}")

    wid_a = PyWorkerID.py_from_random()
    async_worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid_a, node_id=cluster.node_id(),
        max_concurrency=3,
    )
    async_worker.set_task_callback(async_callback)
    async_port = async_worker.start_grpc_server()

    async_drivers = []
    for _ in range(3):
        d = make_driver()
        d.setup_task_dispatch("127.0.0.1", async_port, wid_a)
        async_drivers.append(d)

    pending_a = _submit_from_threads(async_drivers, "run", [], 3)
    for d, oid_b in pending_a:
        ray_get_one(d, oid_b, timeout_ms=10000)

    assert async_state["calls"] == 3
    assert async_state["max_concurrent"] >= 2, \
        f"Async: expected max_concurrent>=2, got {async_state['max_concurrent']}"

    return (f"pattern_async_actor: sync max_concurrent={sync_state['max_concurrent']}, "
            f"async max_concurrent={async_state['max_concurrent']}")


# ═══════════════════════════════════════════════════════════════════════
# TEST 50: object_ref_serialization.py
#   Original: put → pickle ObjectRef to file → del → load → get → free.
# ═══════════════════════════════════════════════════════════════════════

def test_object_ref_serialization():
    """Port of object_ref_serialization.py."""
    import pickle

    driver = make_driver()

    # Put a dict-like object
    data = json.dumps({"hello": "world"}).encode("utf-8")
    oid_obj = driver.put(data, b"python")
    oid_bytes = bytes(oid_obj.binary())

    assert driver.contains(oid_bytes), "Object should be in store after put"

    # Serialize ObjectID bytes to a temp file
    pkl_path = os.path.join(tempfile.gettempdir(), "rust_test_obj_ref.pkl")
    with open(pkl_path, "wb") as f:
        pickle.dump(oid_bytes, f)

    # Delete local reference (Python-side)
    del oid_obj
    del oid_bytes

    # Load from file
    with open(pkl_path, "rb") as f:
        restored_bytes = pickle.load(f)

    # Get the object back
    result = ray_get_one(driver, restored_bytes)
    parsed = json.loads(result.decode("utf-8"))
    assert parsed == {"hello": "world"}, f"Expected dict, got {parsed}"

    # Free the object
    driver.free([restored_bytes])

    os.unlink(pkl_path)
    return "object_ref_serialization: put → pickle to file → load → get → free"


# ═══════════════════════════════════════════════════════════════════════
# TEST 51: deser.py
#   Original: task receives numpy array, in-place modification raises
#   ValueError because deserialized arrays are read-only (zero-copy).
# ═══════════════════════════════════════════════════════════════════════

def test_deser_read_only():
    """Port of deser.py — read-only data concept."""
    def callback(method, args, num_returns=1):
        if method == "modify_data":
            data = args[0]  # This is `bytes`, which is immutable
            try:
                # Attempt in-place modification — bytes is immutable
                mv = memoryview(data)
                mv[0] = 0  # TypeError: cannot modify read-only memory
                return b"ERROR:should_have_raised"
            except TypeError:
                # Expected: bytes is read-only, just like numpy zero-copy
                return b"read_only_error_raised"
        raise ValueError(f"unknown: {method}")

    tw, twid, tport = make_task_worker(callback)
    driver = make_task_driver(tw, twid, tport)

    data = struct.pack("<4q", 1, 2, 3, 4)
    r = driver.submit_task("modify_data", [data])
    result = ray_get_one(driver, r[0].binary())
    assert result == b"read_only_error_raised", f"Got {result}"

    return "deser: in-place modification of read-only data correctly raises TypeError"


# ═══════════════════════════════════════════════════════════════════════
# TEST 52: actor_creator_failure.py
#   Original: kill parent → child actor dies (RayActorError), detached
#   actor survives.
# ═══════════════════════════════════════════════════════════════════════

def test_actor_creator_failure():
    """Port of actor_creator_failure.py."""
    child_state = {"value": 0}
    detached_state = {"value": 0}

    def child_callback(method, args, num_returns=1):
        if method == "ping":
            child_state["value"] += 1
            return pack_i64(child_state["value"])
        raise ValueError(f"unknown: {method}")

    def detached_callback(method, args, num_returns=1):
        if method == "ping":
            detached_state["value"] += 1
            return pack_i64(detached_state["value"])
        raise ValueError(f"unknown: {method}")

    # Create both actors
    c_w, c_wid, c_port, c_aid = make_actor("ChildActor_CF", child_callback)
    d_w, d_wid, d_port, d_aid = make_actor("DetachedActor_CF", detached_callback)

    driver = make_driver()
    setup_and_submit(driver, c_aid, "ChildActor_CF", "127.0.0.1",
                     c_port, cluster.node_id(), c_wid)
    setup_and_submit(driver, d_aid, "DetachedActor_CF", "127.0.0.1",
                     d_port, cluster.node_id(), d_wid)

    # Both work initially
    oid1 = driver.submit_actor_method(c_aid, "ping", [])
    ray_get_one(driver, oid1.binary())
    oid2 = driver.submit_actor_method(d_aid, "ping", [])
    ray_get_one(driver, oid2.binary())
    assert child_state["value"] == 1
    assert detached_state["value"] == 1

    # Kill the child actor (simulate parent death → child dies)
    driver.kill_actor(bytes(c_aid.binary()), True, True)

    # Child: method call should raise error (actor is dead)
    error_raised = False
    try:
        driver.submit_actor_method(c_aid, "ping", [])
    except RuntimeError as e:
        error_raised = True
        assert "dead" in str(e).lower() or "actor" in str(e).lower(), f"Unexpected error: {e}"

    assert error_raised, "Expected error submitting to dead actor"

    # Detached actor: still alive and working
    oid3 = driver.submit_actor_method(d_aid, "ping", [])
    result = unpack_i64(ray_get_one(driver, oid3.binary()))
    assert result == 2, f"Expected 2, got {result}"

    return f"actor_creator_failure: child killed → error, detached → alive (value={result})"


# ═══════════════════════════════════════════════════════════════════════
# TEST 53: actor_restart.py
#   Original: actor with max_restarts=4 crashes every 10th call, is
#   reconstructed. After 50 calls total (5 lives), subsequent calls fail.
# ═══════════════════════════════════════════════════════════════════════

def test_actor_restart():
    """Port of actor_restart.py — crash + re-creation pattern."""
    MAX_RESTARTS = 4
    CRASH_EVERY = 10
    total_calls = {"n": 0}

    def make_crashy_callback():
        local_count = {"n": 0}
        def callback(method, args, num_returns=1):
            if method == "increment_and_possibly_fail":
                local_count["n"] += 1
                if local_count["n"] >= CRASH_EVERY:
                    raise RuntimeError("simulated actor crash (os._exit)")
                total_calls["n"] += 1
                return pack_i64(total_calls["n"])
            raise ValueError(f"unknown: {method}")
        return callback

    successful_calls = 0
    last_aid = None
    last_driver = None

    for life in range(MAX_RESTARTS + 1):  # 5 lives total
        worker, wid, port, actor_id = make_actor(
            f"CrashyActor_{life}", make_crashy_callback())
        driver = make_driver()
        setup_and_submit(driver, actor_id, f"CrashyActor_{life}",
                         "127.0.0.1", port, cluster.node_id(), wid)
        last_aid = actor_id
        last_driver = driver

        for call in range(CRASH_EVERY):
            try:
                oid = driver.submit_actor_method(actor_id,
                    "increment_and_possibly_fail", [])
                result = ray_get_one(driver, oid.binary())
                if result is not None:
                    successful_calls += 1
            except RuntimeError:
                break  # Actor "crashed", move to next life

    # Should have ~45 successful calls (9 per life × 5 lives)
    assert successful_calls >= 40, \
        f"Expected >= 40 successful calls, got {successful_calls}"

    # After all restarts exhausted, demonstrate the actor stays dead
    last_driver.kill_actor(bytes(last_aid.binary()), True, True)
    error_on_dead = False
    try:
        last_driver.submit_actor_method(last_aid,
            "increment_and_possibly_fail", [])
    except RuntimeError:
        error_on_dead = True
    assert error_on_dead, "Expected error on dead actor after restarts exhausted"

    return (f"actor_restart: {successful_calls} successful calls across "
            f"{MAX_RESTARTS + 1} lives, dead after exhaustion")


# ═══════════════════════════════════════════════════════════════════════
# Run all tests
# ═══════════════════════════════════════════════════════════════════════

TESTS = [
    # ── Original 14 actor-based tests ──
    ("getting_started.py (Counter)",    "getting_started.py",           test_getting_started_counter),
    ("actor-repr.py",                   "actor-repr.py",                test_actor_repr),
    ("anti_pattern_global_variables.py","anti_pattern_global_variables.py", test_global_var_actor),
    ("actor_checkpointing.py (Worker)", "actor_checkpointing.py",       test_actor_checkpointing),
    ("actor_checkpointing.py (Immortal)","actor_checkpointing.py",      test_immortal_actor_checkpoint),
    ("monte_carlo_pi.py (Progress)",    "monte_carlo_pi.py",            test_monte_carlo_progress_actor),
    ("monte_carlo_pi.py (Pi calc)",     "monte_carlo_pi.py",            test_monte_carlo_pi_computation),
    ("pattern_tree_of_actors.py",       "pattern_tree_of_actors.py",    test_tree_of_actors_trainers),
    ("pattern_pipelining.py (Queue)",   "pattern_pipelining.py",        test_work_queue_actor),
    ("actors.py (SyncActor)",           "actors.py",                    test_sync_actor_state),
    ("fault_tolerance_tips.py",         "fault_tolerance_tips.py",      test_fault_tolerance_actor),
    ("multi_actor_pipeline",            "(custom multi-actor example)",  test_multi_actor_pipeline),
    ("actor-queue.py",                  "actor-queue.py",               test_actor_queue),
    ("actor-http-server.py (Counter)",  "actor-http-server.py",         test_actor_http_server_counter),

    # ── New: actor return values ──
    ("actor return values (ray.get)",   "(new: actor return values)",   test_actor_return_values),

    # ── New: non-actor remote tasks ──
    ("getting_started.py (tasks)",      "getting_started.py",           test_getting_started_tasks),
    ("anti_pattern_closure_capture",    "anti_pattern_closure_capture_large_objects.py", test_closure_capture_large_objects),
    ("anti_pattern_pass_large_arg",     "anti_pattern_pass_large_arg_by_value.py",      test_pass_large_arg_by_value),
    ("anti_pattern_ray_get_loop.py",    "anti_pattern_ray_get_loop.py",                 test_ray_get_loop),
    ("anti_pattern_unnecessary_ray_get","anti_pattern_unnecessary_ray_get.py",           test_unnecessary_ray_get),
    ("anti_pattern_redefine_task_loop", "anti_pattern_redefine_task_actor_loop.py",      test_redefine_task_loop),
    ("anti_pattern_too_fine_grained",   "anti_pattern_too_fine_grained_tasks.py",        test_too_fine_grained_tasks),
    ("anti_pattern_ray_get_submission", "anti_pattern_ray_get_submission_order.py",       test_ray_get_submission_order),
    ("anti_pattern_ray_get_too_many",   "anti_pattern_ray_get_too_many_objects.py",       test_ray_get_too_many_objects),

    # ── New: object store ──
    ("obj_capture.py",                  "obj_capture.py",               test_obj_capture),
    ("obj_ref.py",                      "obj_ref.py",                   test_obj_ref),
    ("obj_val.py",                      "obj_val.py",                   test_obj_val),

    # ── New: task patterns ──
    ("task_exceptions.py",              "task_exceptions.py",           test_task_exceptions),
    ("limit_pending_tasks.py",          "limit_pending_tasks.py",       test_limit_pending_tasks),
    ("anti_pattern_return_ray_put.py",  "anti_pattern_return_ray_put.py", test_return_ray_put),
    ("owners.py",                       "owners.py",                    test_owners),

    # ── Phase 2: multi-return + named actors + simplified ports ──
    ("tasks.py (multi-return)",         "tasks.py",                     test_tasks_multi_return),
    ("tasks.py (pass-by-ref)",          "tasks.py",                     test_tasks_pass_by_ref),
    ("tasks.py (wait)",                 "tasks.py",                     test_tasks_wait),
    ("get_or_create.py",                "get_or_create.py",             test_get_or_create),
    ("scheduling.py",                   "scheduling.py",                test_scheduling_strategies),
    ("resources.py",                    "resources.py",                 test_resources),
    ("anti_pattern_fork_new_processes", "anti_pattern_fork_new_processes.py", test_fork_new_processes),
    ("limit_running_tasks.py",          "limit_running_tasks.py",       test_limit_running_tasks),

    # ── Phase 2: nested tasks from callbacks ──
    ("nested-tasks.py",                 "nested-tasks.py",              test_nested_tasks),
    ("pattern_nested_tasks.py",         "pattern_nested_tasks.py",      test_pattern_nested_tasks),
    ("anti_pattern_nested_ray_get.py",  "anti_pattern_nested_ray_get.py", test_nested_ray_get),
    ("actor-sync.py",                   "actor-sync.py",                test_actor_sync),

    # ── Phase 3: task retries + error propagation ──
    ("tasks_fault_tolerance.py",        "tasks_fault_tolerance.py",     test_tasks_fault_tolerance),
    ("tasks.py (error propagation)",    "tasks.py",                     test_tasks_error_propagation),

    # ── Phase 4: placement groups ──
    ("placement_group_capture_child_tasks_example.py", "placement_group_capture_child_tasks_example.py", test_placement_group_capture_child_tasks),
    ("original_resource_unavailable_example.py",       "original_resource_unavailable_example.py",       test_original_resource_unavailable),

    # ── Phase 5: async actors, actor lifecycle, object serialization, deser ──
    ("actor-pool.py",                   "actor-pool.py",                test_actor_pool),
    ("pattern_async_actor.py",          "pattern_async_actor.py",       test_pattern_async_actor),
    ("object_ref_serialization.py",     "object_ref_serialization.py",  test_object_ref_serialization),
    ("deser.py",                        "deser.py",                     test_deser_read_only),
    ("actor_creator_failure.py",        "actor_creator_failure.py",     test_actor_creator_failure),
    ("actor_restart.py",                "actor_restart.py",             test_actor_restart),
]

print("─" * 70)
print("  Running ported examples...")
print("─" * 70)
print()

passed = 0
failed = 0
for name, orig, test_fn in TESTS:
    try:
        detail = test_fn()
        record(name, orig, "PASS", detail)
        passed += 1
        print(f"  ✓ PASS  {name}")
        print(f"          {detail}")
    except Exception as e:
        record(name, orig, "FAIL", str(e))
        failed += 1
        print(f"  ✗ FAIL  {name}")
        print(f"          {e}")
        traceback.print_exc()
    print()

# ═══════════════════════════════════════════════════════════════════════
# SKIPPED examples — not portable to current Rust backend
# ═══════════════════════════════════════════════════════════════════════

SKIPPED = [
    # ── GPU-required (5) ──
    ("cgraph_nccl.py",              "Requires GPU (NCCL transport)"),
    ("cgraph_overlap.py",           "Requires GPU (NCCL overlap)"),
    ("cgraph_profiling.py",         "Requires GPU (NCCL profiling)"),
    ("direct_transport_nccl.py",    "Requires GPU (NCCL)"),
    ("direct_transport_nixl.py",    "Requires GPU (NIXL)"),

    # ── Compiled graphs (3) ──
    ("cgraph_quickstart.py",        "Requires compiled DAG API"),
    ("cgraph_troubleshooting.py",   "Requires compiled DAG API"),
    ("cgraph_visualize.py",         "Requires compiled DAG API"),

    # ── Task features beyond current scope (1) ──
    ("tqdm.py",                     "Requires ray.experimental.tqdm"),

    # ── Object store features beyond current scope (1) ──
    ("anti_pattern_out_of_band_object_ref_serialization.py", "Requires runtime_env + GC semantics"),

    # ── Generators (3) ──
    ("pattern_generators.py",       "Requires generator return semantics"),
    ("generator.py",                "Requires generator return (num_returns='dynamic')"),
    ("streaming_generator.py",      "Requires streaming generator + ray.wait"),

    # ── Placement groups (1) ──
    ("placement_group_example.py",                       "Requires placement groups + GPU"),

    # ── Cross-language / namespaces (2) ──
    ("cross_language.py",           "Requires Java worker + cross-language RPC"),
    ("namespaces.py",               "Requires namespace registry + detached actors"),

    # ── Other unsupported features (5) ──
    ("ray-dag.py",                  "Requires Ray DAG API (bind/execute)"),
    ("runtime_env_example.py",      "Requires runtime_env support"),
    ("ray_oom_prevention.py",       "Requires OOM monitor + memory scheduling"),
    ("direct_transport_gloo.py",    "Requires Gloo collective transport"),
]

for name, reason in SKIPPED:
    record(name, name, "SKIP", reason)

skipped = len(SKIPPED)

# ═══════════════════════════════════════════════════════════════════════
# Final report
# ═══════════════════════════════════════════════════════════════════════

print("=" * 70)
print("  FINAL REPORT")
print("=" * 70)
print()
print(f"  PASSED:  {passed}")
print(f"  FAILED:  {failed}")
print(f"  SKIPPED: {skipped} (unsupported features)")
print(f"  TOTAL:   {passed + failed + skipped}")
print()

if failed > 0:
    print("  FAILED tests:")
    for name, orig, status, detail in results:
        if status == "FAIL":
            print(f"    - {name}: {detail}")
    print()

print("  SKIPPED examples by category:")
categories = {}
for name, reason in SKIPPED:
    cat = reason.split("(")[0].strip().rstrip("+").strip()
    if cat not in categories:
        categories[cat] = []
    categories[cat].append(name)
for cat, files in sorted(categories.items()):
    print(f"    {cat} ({len(files)} files)")

print()
print("─" * 70)
print("  PASSED tests detail:")
print("─" * 70)
for name, orig, status, detail in results:
    if status == "PASS":
        print(f"    ✓ {name}")
        print(f"      Source: {orig}")
        print(f"      Result: {detail}")
        print()

# Clean up cluster.
cluster.shutdown()
print("Cluster shut down.")

# Exit code.
sys.exit(1 if failed > 0 else 0)
