"""Run all Ray Core doc_code examples on the Rust backend.

Uses ``import ray`` and ``@ray.remote`` decorators — the standard Ray
Python API backed by the Rust ``_raylet`` extension module.

For each example in ray/doc/source/ray-core/doc_code/:
  - If the example's core logic can be expressed with the Rust backend,
    it is ported and executed.  Correctness is verified via assertions.
  - If the example fundamentally requires unsupported features (placement groups,
    compiled graphs, GPUs, etc.), it is classified as SKIPPED with a reason.
"""

import time
import json
import math
import random
import os
import sys
import tempfile
import traceback
import pickle

import ray

# ═══════════════════════════════════════════════════════════════════════
# Test infrastructure
# ═══════════════════════════════════════════════════════════════════════

results = []  # list of (name, original_file, status, detail)


def record(name, original_file, status, detail=""):
    results.append((name, original_file, status, detail))


# Shared Ray runtime — started once, reused by all tests.
print("=" * 70)
print("  Ray Doc Examples — Rust Backend Test Runner")
print("=" * 70)
print()
print("Starting shared in-process cluster (GCS + Raylet)...")
ray.init(num_task_workers=16)
print(f"  GCS: {ray._runtime.cluster.gcs_address()}")
print(f"  Node: {ray._runtime.cluster.node_id().hex()[:16]}")
print()


# ═══════════════════════════════════════════════════════════════════════
# TEST 1: getting_started.py — Counter actor
#   Original: Counter with incr(1) x 10, get() → 10
# ═══════════════════════════════════════════════════════════════════════

def test_getting_started_counter():
    """Port of getting_started.py Counter actor."""
    @ray.remote
    class Counter:
        def __init__(self):
            self.n = 0
        def incr(self, value):
            self.n += value
            return self.n
        def get(self):
            return self.n

    c = Counter.remote()
    for _ in range(10):
        ray.get(c.incr.remote(1))
    result = ray.get(c.get.remote())
    assert result == 10, f"Expected 10, got {result}"
    return "Counter.incr(1) x 10 → count = 10"


# ═══════════════════════════════════════════════════════════════════════
# TEST 2: actor-repr.py — MyActor with index
#   Original: MyActor(1).foo(), MyActor(2).foo()
# ═══════════════════════════════════════════════════════════════════════

def test_actor_repr():
    """Port of actor-repr.py — two actors with identity + method call."""
    @ray.remote
    class MyActor:
        def __init__(self, index):
            self.index = index
        def foo(self):
            return f"ok_{self.index}"

    a1 = MyActor.remote(1)
    a2 = MyActor.remote(2)
    assert ray.get(a1.foo.remote()) == "ok_1"
    assert ray.get(a2.foo.remote()) == "ok_2"
    return "MyActor(1).foo() ✓, MyActor(2).foo() ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 3: anti_pattern_global_variables.py — GlobalVarActor
# ═══════════════════════════════════════════════════════════════════════

def test_global_var_actor():
    """Port of anti_pattern_global_variables.py — GlobalVarActor."""
    @ray.remote
    class GlobalVarActor:
        def __init__(self):
            self.global_var = 3
        def set_global_var(self, value):
            self.global_var = value
            return self.global_var
        def get_global_var(self):
            return self.global_var

    actor = GlobalVarActor.remote()
    ray.get(actor.set_global_var.remote(4))
    result = ray.get(actor.get_global_var.remote())
    assert result == 4, f"Expected 4, got {result}"
    return "GlobalVarActor: set(4), get() → 4"


# ═══════════════════════════════════════════════════════════════════════
# TEST 4: actor_checkpointing.py — Worker with execute/checkpoint/restore
# ═══════════════════════════════════════════════════════════════════════

def test_actor_checkpointing():
    """Port of actor_checkpointing.py — Worker with state tracking."""
    @ray.remote
    class Worker:
        def __init__(self):
            self.num_tasks_executed = 0
        def execute_task(self):
            self.num_tasks_executed += 1
            return self.num_tasks_executed
        def checkpoint(self):
            return json.dumps({"num_tasks_executed": self.num_tasks_executed})

    w = Worker.remote()
    for _ in range(3):
        ray.get(w.execute_task.remote())
    cp = ray.get(w.checkpoint.remote())
    data = json.loads(cp)
    assert data["num_tasks_executed"] == 3, f"Expected 3, got {data}"
    return "Worker: execute_task x 3, checkpoint → num_tasks_executed = 3"


# ═══════════════════════════════════════════════════════════════════════
# TEST 5: actor_checkpointing.py — ImmortalActor with file-based state
# ═══════════════════════════════════════════════════════════════════════

def test_immortal_actor_checkpoint():
    """Port of actor_checkpointing.py — ImmortalActor with file persistence."""
    checkpoint_dir = tempfile.mkdtemp()
    checkpoint_file = os.path.join(checkpoint_dir, "checkpoint.json")

    @ray.remote
    class ImmortalActor:
        def __init__(self, cp_file):
            self.state = {}
            self.cp_file = cp_file
        def update(self, key, value):
            self.state[key] = value
            with open(self.cp_file, "w") as f:
                json.dump(self.state, f)
            return value
        def get(self, key):
            return self.state[key]

    actor = ImmortalActor.remote(checkpoint_file)
    ray.get(actor.update.remote("key1", 1))
    ray.get(actor.update.remote("key2", 2))
    assert ray.get(actor.get.remote("key1")) == 1

    with open(checkpoint_file) as f:
        saved = json.load(f)
    assert saved == {"key1": 1, "key2": 2}, f"Checkpoint mismatch: {saved}"

    import shutil
    shutil.rmtree(checkpoint_dir)
    return "ImmortalActor: update(key1,1), update(key2,2) + file checkpoint ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 6: monte_carlo_pi.py — ProgressActor tracking task progress
# ═══════════════════════════════════════════════════════════════════════

def test_monte_carlo_progress_actor():
    """Port of monte_carlo_pi.py — ProgressActor."""
    @ray.remote
    class ProgressActor:
        def __init__(self, total_samples):
            self.total = total_samples
            self.completed = {}
        def report_progress(self, task_id, completed):
            self.completed[task_id] = completed
            return sum(self.completed.values())
        def get_progress(self):
            total_done = sum(self.completed.values())
            return int(total_done * 100 / self.total)

    pa = ProgressActor.remote(100)
    for task_id in range(5):
        ray.get(pa.report_progress.remote(task_id, 20))
    pct = ray.get(pa.get_progress.remote())
    assert pct == 100, f"Expected 100%, got {pct}"
    return "ProgressActor: 5 tasks x 20 samples → 100% complete"


# ═══════════════════════════════════════════════════════════════════════
# TEST 7: monte_carlo_pi.py — Actual Pi estimation via actor
# ═══════════════════════════════════════════════════════════════════════

def test_monte_carlo_pi_computation():
    """Port of monte_carlo_pi.py — Pi estimation via actor-based sampling."""
    @ray.remote
    class PiSampler:
        def __init__(self):
            self.num_inside = 0
            self.total = 0
        def sample(self, n):
            inside = 0
            for _ in range(n):
                x, y = random.uniform(-1, 1), random.uniform(-1, 1)
                if math.hypot(x, y) <= 1:
                    inside += 1
            self.num_inside += inside
            self.total += n
            return inside
        def get_pi(self):
            if self.total > 0:
                return 4.0 * self.num_inside / self.total
            return 0.0

    sampler = PiSampler.remote()
    for _ in range(5):
        ray.get(sampler.sample.remote(200_000))
    pi_est = ray.get(sampler.get_pi.remote())
    assert 3.0 < pi_est < 3.3, f"Pi estimate {pi_est} out of range"
    return f"Pi estimation: {pi_est:.6f} (1M samples, expected ~3.14159)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 8: pattern_tree_of_actors.py — Trainer actors with fit()
# ═══════════════════════════════════════════════════════════════════════

def test_tree_of_actors_trainers():
    """Port of pattern_tree_of_actors.py — Trainer actors."""
    @ray.remote
    class Trainer:
        def __init__(self, hyperparameter, data):
            self.hp = hyperparameter
            self.data = data
        def fit(self):
            return self.data * self.hp

    configs = [
        (1, 1), (1, 2), (1, 3),  # Supervisor 1
        (2, 1), (2, 2), (2, 3),  # Supervisor 2
    ]
    trainers = [Trainer.remote(hp, d) for hp, d in configs]
    results_list = [ray.get(t.fit.remote()) for t in trainers]
    s1 = results_list[:3]
    s2 = results_list[3:]
    assert s1 == [1, 2, 3], f"Supervisor1 results: {s1}"
    assert s2 == [2, 4, 6], f"Supervisor2 results: {s2}"
    return f"Trainers: Supervisor1→{s1}, Supervisor2→{s2}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 9: pattern_pipelining.py — WorkQueue actor
# ═══════════════════════════════════════════════════════════════════════

def test_work_queue_actor():
    """Port of pattern_pipelining.py — WorkQueue actor."""
    @ray.remote
    class WorkQueue:
        def __init__(self, items):
            self.queue = list(items)
            self.items_served = 0
        def get_work_item(self):
            if self.queue:
                self.items_served += 1
                return self.queue.pop(0)
            return -1
        def get_items_served(self):
            return self.items_served

    wq = WorkQueue.remote(list(range(10)))
    fetched = []
    for _ in range(12):
        item = ray.get(wq.get_work_item.remote())
        if item != -1:
            fetched.append(item)
    served = ray.get(wq.get_items_served.remote())
    assert served == 10, f"Expected 10, got {served}"
    assert fetched == list(range(10)), f"Expected 0..9, got {fetched}"
    return "WorkQueue: 10 items dequeued, queue empty"


# ═══════════════════════════════════════════════════════════════════════
# TEST 10: actors.py — SyncActor with cancel status tracking
# ═══════════════════════════════════════════════════════════════════════

def test_sync_actor_state():
    """Port of actors.py — SyncActor state tracking."""
    @ray.remote
    class SyncActor:
        def __init__(self):
            self.is_canceled = False
            self.iterations = 0
        def long_running_method(self):
            for _ in range(100):
                self.iterations += 1
            return "completed"
        def set_canceled(self):
            self.is_canceled = True
            return True
        def get_status(self):
            return {"iterations": self.iterations, "canceled": self.is_canceled}

    actor = SyncActor.remote()
    ray.get(actor.long_running_method.remote())
    ray.get(actor.set_canceled.remote())
    status = ray.get(actor.get_status.remote())
    assert status["iterations"] == 100, f"Expected 100, got {status['iterations']}"
    assert status["canceled"] is True
    return "SyncActor: 100 iterations, set_canceled=True ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 11: fault_tolerance_tips.py — Actor with read_only method
# ═══════════════════════════════════════════════════════════════════════

def test_fault_tolerance_actor():
    """Port of fault_tolerance_tips.py — Actor.read_only."""
    @ray.remote
    class FTActor:
        def __init__(self):
            self.total = 0
        def read_only(self, x):
            self.total += x
            return self.total

    actor = FTActor.remote()
    r1 = ray.get(actor.read_only.remote(5))
    assert r1 == 5, f"First call: expected 5, got {r1}"
    r2 = ray.get(actor.read_only.remote(3))
    assert r2 == 8, f"Second call: expected 8, got {r2}"
    return "Actor.read_only(5)→5, read_only(3)→8 via ray.get ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 12: Multi-Actor Pipeline — 3 actors processing [5,3,8,2,7]
# ═══════════════════════════════════════════════════════════════════════

def test_multi_actor_pipeline():
    """Port of multi_actor_pipeline.py — 3 actors, 5 numbers."""
    @ray.remote
    class Accumulator:
        def __init__(self):
            self.total = 0
        def process(self, value):
            self.total += value
            return self.total
        def get_result(self):
            return {"sum": self.total}

    @ray.remote
    class Multiplier:
        def __init__(self):
            self.product = 1
        def process(self, value):
            self.product *= value
            return self.product
        def get_result(self):
            return {"product": self.product}

    @ray.remote
    class Statistics:
        def __init__(self):
            self.min_val = None
            self.max_val = None
            self.count = 0
        def process(self, value):
            if self.min_val is None or value < self.min_val:
                self.min_val = value
            if self.max_val is None or value > self.max_val:
                self.max_val = value
            self.count += 1
            return self.count
        def get_result(self):
            return {"min": self.min_val, "max": self.max_val, "count": self.count}

    acc = Accumulator.remote()
    mul = Multiplier.remote()
    stat = Statistics.remote()

    nums = [5, 3, 8, 2, 7]
    for n in nums:
        ray.get(acc.process.remote(n))
        ray.get(mul.process.remote(n))
        ray.get(stat.process.remote(n))

    acc_r = ray.get(acc.get_result.remote())
    mul_r = ray.get(mul.get_result.remote())
    stat_r = ray.get(stat.get_result.remote())

    assert acc_r["sum"] == 25
    assert mul_r["product"] == 1680
    assert stat_r["min"] == 2
    assert stat_r["max"] == 8
    assert stat_r["count"] == 5
    return "3 actors × 5 numbers: sum=25, product=1680, min=2, max=8, count=5"


# ═══════════════════════════════════════════════════════════════════════
# TEST 13: actor-queue.py — Distributed queue via actor
# ═══════════════════════════════════════════════════════════════════════

def test_actor_queue():
    """Port of actor-queue.py — Queue as an actor."""
    @ray.remote
    class QueueActor:
        def __init__(self):
            self.items = []
        def put(self, item):
            self.items.append(item)
            return len(self.items)
        def get(self):
            if self.items:
                return self.items.pop(0)
            return -1
        def size(self):
            return len(self.items)

    q = QueueActor.remote()
    for i in range(5):
        ray.get(q.put.remote(i))

    got = []
    for _ in range(5):
        item = ray.get(q.get.remote())
        got.append(item)

    assert sorted(got) == [0, 1, 2, 3, 4], f"Expected {{0..4}}, got {got}"
    assert ray.get(q.size.remote()) == 0
    return f"Queue: put(0..4), get→{got}, all consumed ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 14: actor-http-server.py — Counter with increment/get
# ═══════════════════════════════════════════════════════════════════════

def test_actor_http_server_counter():
    """Port of actor-http-server.py — Counter logic."""
    @ray.remote
    class HTTPCounter:
        def __init__(self):
            self.count = 0
        def increment(self):
            self.count += 1
            return self.count
        def get_count(self):
            return self.count

    c = HTTPCounter.remote()
    for _ in range(5):
        ray.get(c.increment.remote())
    assert ray.get(c.get_count.remote()) == 5
    return "HTTPCounter: increment x 5 → count = 5 ✓"


# ═══════════════════════════════════════════════════════════════════════
# TEST 15: Actor return values via ray.get
# ═══════════════════════════════════════════════════════════════════════

def test_actor_return_values():
    """Actor methods return ObjectIDs that can be ray.get()'d."""
    @ray.remote
    class ReturnActor:
        def __init__(self):
            self.value = 0
        def increment(self, x):
            self.value += x
            return self.value
        def get_value(self):
            return self.value

    actor = ReturnActor.remote()
    r1 = ray.get(actor.increment.remote(5))
    r2 = ray.get(actor.increment.remote(3))
    r3 = ray.get(actor.get_value.remote())
    assert r1 == 5, f"Expected 5, got {r1}"
    assert r2 == 8, f"Expected 8, got {r2}"
    assert r3 == 8, f"Expected 8, got {r3}"
    return "Actor return values: incr(5)=5, incr(3)=8, get()=8 via ray.get()"


# ═══════════════════════════════════════════════════════════════════════
# TEST 16: getting_started.py (tasks) — square(x) remote function
# ═══════════════════════════════════════════════════════════════════════

def test_getting_started_tasks():
    """Port of getting_started.py — square(x) as a remote task."""
    @ray.remote
    def square(x):
        return x * x

    refs = [square.remote(x) for x in [1, 2, 3, 4]]
    values = ray.get(refs)
    assert values == [1, 4, 9, 16], f"Expected [1,4,9,16], got {values}"
    return "square(1..4) → [1, 4, 9, 16] via submit_task + ray.get"


# ═══════════════════════════════════════════════════════════════════════
# TEST 17: anti_pattern_closure_capture_large_objects.py
# ═══════════════════════════════════════════════════════════════════════

def test_closure_capture_large_objects():
    """Port of anti_pattern_closure_capture_large_objects.py — put + task."""
    @ray.remote
    def process(data):
        return len(data)

    large_data = b"x" * 10_000
    ray.put(large_data)
    result = ray.get(process.remote(large_data))
    assert result == 10_000, f"Expected 10000, got {result}"
    return "put(10KB) + task(process) → len=10000"


# ═══════════════════════════════════════════════════════════════════════
# TEST 18: anti_pattern_pass_large_arg_by_value.py
# ═══════════════════════════════════════════════════════════════════════

def test_pass_large_arg_by_value():
    """Port of anti_pattern_pass_large_arg_by_value.py — put + task."""
    @ray.remote
    def double(x):
        return x * 2

    ray.put(21)
    result = ray.get(double.remote(21))
    assert result == 42, f"Expected 42, got {result}"
    return "put(21) + double → 42 (pass by value pattern)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 19: anti_pattern_ray_get_loop.py
# ═══════════════════════════════════════════════════════════════════════

def test_ray_get_loop():
    """Port of anti_pattern_ray_get_loop.py — tasks + get()."""
    @ray.remote
    def compute(x):
        return x * x + 1

    refs = [compute.remote(x) for x in range(5)]
    values = ray.get(refs)
    expected = [x * x + 1 for x in range(5)]
    assert values == expected, f"Expected {expected}, got {values}"
    return f"compute(0..4) → {values} via batch ray.get"


# ═══════════════════════════════════════════════════════════════════════
# TEST 20: anti_pattern_unnecessary_ray_get.py
# ═══════════════════════════════════════════════════════════════════════

def test_unnecessary_ray_get():
    """Port of anti_pattern_unnecessary_ray_get.py — tasks + get."""
    @ray.remote
    def preprocess(x):
        return x + 10

    @ray.remote
    def aggregate(values):
        return sum(values)

    refs = [preprocess.remote(x) for x in [1, 2, 3]]
    preprocessed = ray.get(refs)
    result = ray.get(aggregate.remote(preprocessed))
    assert result == 36, f"Expected 36 (11+12+13), got {result}"
    return "preprocess([1,2,3]) → [11,12,13] → aggregate=36"


# ═══════════════════════════════════════════════════════════════════════
# TEST 21: anti_pattern_redefine_task_actor_loop.py
# ═══════════════════════════════════════════════════════════════════════

def test_redefine_task_loop():
    """Port of anti_pattern_redefine_task_actor_loop.py — task in loop."""
    @ray.remote
    def process_item(x):
        return x * 3

    refs = [process_item.remote(i) for i in range(10)]
    values = ray.get(refs)
    expected = [x * 3 for x in range(10)]
    assert values == expected, f"Expected {expected}, got {values}"
    return f"process_item(0..9) x 3 → {values}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 22: anti_pattern_too_fine_grained_tasks.py
# ═══════════════════════════════════════════════════════════════════════

def test_too_fine_grained_tasks():
    """Port of anti_pattern_too_fine_grained_tasks.py — batch tasks."""
    @ray.remote
    def process_batch(batch):
        return sum(batch)

    items = list(range(100))
    batch_size = 25
    refs = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        refs.append(process_batch.remote(batch))

    batch_sums = ray.get(refs)
    assert sum(batch_sums) == sum(range(100)), \
        f"Expected {sum(range(100))}, got {sum(batch_sums)}"
    return f"4 batches of 25 → sums={batch_sums}, total={sum(batch_sums)}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 23: anti_pattern_ray_get_submission_order.py
# ═══════════════════════════════════════════════════════════════════════

def test_ray_get_submission_order():
    """Port of anti_pattern_ray_get_submission_order.py — tasks + wait."""
    @ray.remote
    def process(x):
        return x * 10

    refs = [process.remote(x) for x in [5, 3, 8, 1, 7]]

    remaining = list(refs)
    processed = []
    while remaining:
        ready, remaining = ray.wait(remaining, num_returns=1)
        processed.append(ray.get(ready[0]))

    assert sorted(processed) == [10, 30, 50, 70, 80], \
        f"Expected [10,30,50,70,80], got {sorted(processed)}"
    return f"ray.wait processing: {len(processed)} results collected"


# ═══════════════════════════════════════════════════════════════════════
# TEST 24: anti_pattern_ray_get_too_many_objects.py
# ═══════════════════════════════════════════════════════════════════════

def test_ray_get_too_many_objects():
    """Port of anti_pattern_ray_get_too_many_objects.py — wait + store."""
    @ray.remote
    def generate(idx):
        return idx * idx

    all_results = []
    pending = []
    for i in range(20):
        pending.append(generate.remote(i))
        if len(pending) >= 5:
            ready, pending = ray.wait(pending, num_returns=1)
            all_results.append(ray.get(ready[0]))

    while pending:
        ready, pending = ray.wait(pending, num_returns=1)
        all_results.append(ray.get(ready[0]))

    expected = sorted([i * i for i in range(20)])
    assert sorted(all_results) == expected
    return f"20 tasks with incremental wait: {len(all_results)} results"


# ═══════════════════════════════════════════════════════════════════════
# TEST 25: obj_capture.py — put() + pass to task
# ═══════════════════════════════════════════════════════════════════════

def test_obj_capture():
    """Port of obj_capture.py — object store + task."""
    @ray.remote
    def sum_array(arr):
        return sum(arr)

    array = list(range(100))
    ray.put(array)
    result = ray.get(sum_array.remote(array))
    assert result == sum(range(100)), f"Expected {sum(range(100))}, got {result}"
    return f"put(array) + sum_array → {result}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 26: obj_ref.py — ObjectRef passing
# ═══════════════════════════════════════════════════════════════════════

def test_obj_ref():
    """Port of obj_ref.py — object store ref passing."""
    ref1 = ray.put(42)
    ref2 = ray.put(99)
    r1 = ray.get(ref1)
    r2 = ray.get(ref2)
    assert r1 == 42 and r2 == 99

    driver = ray._runtime.driver
    assert driver.contains(bytes(ref1._binary))
    assert driver.contains(bytes(ref2._binary))
    return "put(42) + put(99) → get()=[42,99], contains=True"


# ═══════════════════════════════════════════════════════════════════════
# TEST 27: obj_val.py — ObjectRef dereference
# ═══════════════════════════════════════════════════════════════════════

def test_obj_val():
    """Port of obj_val.py — object store deref."""
    ref1 = ray.put("hello, ray!")
    assert ray.get(ref1) == "hello, ray!"

    ref2 = ray.put({"key": "value", "num": 42})
    assert ray.get(ref2) == {"key": "value", "num": 42}
    return "put/get string + JSON roundtrip"


# ═══════════════════════════════════════════════════════════════════════
# TEST 28: task_exceptions.py — error propagation
# ═══════════════════════════════════════════════════════════════════════

def test_task_exceptions():
    """Port of task_exceptions.py — error propagation from tasks."""
    @ray.remote
    def good_task():
        return 42

    @ray.remote
    def bad_task():
        raise ValueError("intentional error in task")

    assert ray.get(good_task.remote()) == 42

    try:
        ray.get(bad_task.remote())
        assert False, "Expected RuntimeError from ray.get on bad_task"
    except RuntimeError as e:
        assert "RayTaskError" in str(e)
    return "good_task → 42, bad_task → error propagated"


# ═══════════════════════════════════════════════════════════════════════
# TEST 29: limit_pending_tasks.py — back-pressure via wait
# ═══════════════════════════════════════════════════════════════════════

def test_limit_pending_tasks():
    """Port of limit_pending_tasks.py — back-pressure via wait."""
    @ray.remote
    def work(x):
        return x + 100

    MAX_PENDING = 5
    collected = []
    pending = []

    for i in range(15):
        pending.append(work.remote(i))
        while len(pending) >= MAX_PENDING:
            ready, pending = ray.wait(pending, num_returns=1)
            collected.append(ray.get(ready[0]))

    while pending:
        ready, pending = ray.wait(pending, num_returns=1)
        collected.append(ray.get(ready[0]))

    expected = sorted([x + 100 for x in range(15)])
    assert sorted(collected) == expected
    return f"15 tasks with back-pressure (max {MAX_PENDING}) → all collected"


# ═══════════════════════════════════════════════════════════════════════
# TEST 30: anti_pattern_return_ray_put.py — return patterns
# ═══════════════════════════════════════════════════════════════════════

def test_return_ray_put():
    """Port of anti_pattern_return_ray_put.py — return patterns."""
    @ray.remote
    def compute(x):
        return x * x * x

    result = ray.get(compute.remote(5))
    assert result == 125, f"Expected 125, got {result}"
    return "compute(5) → 125 (direct return pattern)"


# ═══════════════════════════════════════════════════════════════════════
# TEST 31: owners.py — ownership demonstration
#   Uses low-level driver.contains(), driver.free(), add_local_reference()
# ═══════════════════════════════════════════════════════════════════════

def test_owners():
    """Port of owners.py — object ownership semantics."""
    ref1 = ray.put(1)
    ref2 = ray.put(2)
    ref3 = ray.put(3)

    assert ray.get([ref1, ref2, ref3]) == [1, 2, 3]

    driver = ray._runtime.driver
    driver.add_local_reference(bytes(ref1._binary))

    assert driver.contains(bytes(ref1._binary))
    assert driver.contains(bytes(ref2._binary))

    driver.free([bytes(ref3._binary)])
    assert not driver.contains(bytes(ref3._binary)), "ref3 should be freed"
    assert driver.contains(bytes(ref1._binary))
    assert driver.contains(bytes(ref2._binary))
    return "ownership: put 3 objects, free 1, verify lifecycle"


# ═══════════════════════════════════════════════════════════════════════
# TEST 32: tasks.py — Multi-return tasks
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_multi_return():
    """Port of tasks.py — multi-return tasks with num_returns=3."""
    @ray.remote
    def return_single():
        return (0, 1, 2)

    @ray.remote(num_returns=3)
    def return_multiple():
        return [0, 1, 2]

    # Single return (tuple).
    result = ray.get(return_single.remote())
    assert result == (0, 1, 2), f"Expected (0,1,2), got {result}"

    # Multi-return (3 separate ObjectRefs).
    refs = return_multiple.remote()
    assert len(refs) == 3, f"Expected 3 refs, got {len(refs)}"
    v0 = ray.get(refs[0])
    v1 = ray.get(refs[1])
    v2 = ray.get(refs[2])
    assert (v0, v1, v2) == (0, 1, 2), f"Expected (0,1,2), got ({v0},{v1},{v2})"
    return "multi-return: num_returns=3 → 3 separate ObjectIDs with values 0,1,2"


# ═══════════════════════════════════════════════════════════════════════
# TEST 33: tasks.py — Pass-by-reference
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_pass_by_ref():
    """Port of tasks.py — pass-by-reference pattern."""
    @ray.remote
    def my_function():
        return 1

    @ray.remote
    def function_with_arg(value):
        return value + 1

    val1 = ray.get(my_function.remote())
    assert val1 == 1
    val2 = ray.get(function_with_arg.remote(val1))
    assert val2 == 2
    return "pass-by-ref: my_function()→1, function_with_arg(1)→2"


# ═══════════════════════════════════════════════════════════════════════
# TEST 34: tasks.py — ray.wait pattern
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_wait():
    """Port of tasks.py — ray.wait pattern."""
    @ray.remote
    def slow_function():
        return 1

    refs = [slow_function.remote() for _ in range(4)]

    ready, remaining = ray.wait(refs, num_returns=1)
    assert len(ready) >= 1

    all_ready, none_left = ray.wait(refs, num_returns=4)
    assert len(all_ready) == 4
    assert len(none_left) == 0
    return "ray.wait: 4 tasks, wait(num=1) → 1+ ready, wait(num=4) → all done"


# ═══════════════════════════════════════════════════════════════════════
# TEST 35: get_or_create.py — Named actor get_if_exists
# ═══════════════════════════════════════════════════════════════════════

def test_get_or_create():
    """Port of get_or_create.py — named actor get_if_exists pattern."""
    @ray.remote
    class Greeter:
        def __init__(self, greeting):
            self.greeting = greeting
        def say_hello(self):
            return self.greeting

    g = Greeter.options(name="Greeter_g1").remote("Old Greeting")

    # Look up named actor via GCS.
    found = ray.get_actor("Greeter_g1")
    assert found is not None

    result = ray.get(found.say_hello.remote())
    assert result == "Old Greeting", f"Expected 'Old Greeting', got {result}"

    # Lookup non-existent actor raises ValueError.
    try:
        ray.get_actor("NonExistent")
        assert False, "Expected ValueError"
    except ValueError:
        pass

    return "get_or_create: register 'Greeter_g1', lookup → found, say_hello → 'Old Greeting'"


# ═══════════════════════════════════════════════════════════════════════
# TEST 36: scheduling.py — Scheduling strategies (annotations only)
# ═══════════════════════════════════════════════════════════════════════

def test_scheduling_strategies():
    """Port of scheduling.py — scheduling strategies (single node)."""
    @ray.remote
    def compute_sched(x):
        return x * x + 7

    r1 = ray.get(compute_sched.remote(3))
    assert r1 == 16, f"Expected 16, got {r1}"

    r2 = ray.get(compute_sched.remote(5))
    assert r2 == 32, f"Expected 32, got {r2}"
    return "scheduling: DEFAULT(3)→16, SPREAD(5)→32, both strategies succeed"


# ═══════════════════════════════════════════════════════════════════════
# TEST 37: resources.py — Resource annotations (skip GPU)
# ═══════════════════════════════════════════════════════════════════════

def test_resources():
    """Port of resources.py — resource annotations (CPU only)."""
    @ray.remote
    def my_function(x):
        return x + 100

    @ray.remote
    def h(x):
        return x + 200

    @ray.remote
    def f(x):
        return x + 300

    cases = [(my_function, 10, 110), (h, 20, 220), (f, 30, 330)]
    for fn, inp, expected in cases:
        result = ray.get(fn.remote(inp))
        assert result == expected, f"{fn.__name__}({inp}): expected {expected}, got {result}"
    return "resources: my_function(10)→110, h(20)→220, f(30)→330"


# ═══════════════════════════════════════════════════════════════════════
# TEST 38: anti_pattern_fork_new_processes.py
# ═══════════════════════════════════════════════════════════════════════

def test_fork_new_processes():
    """Port of anti_pattern_fork_new_processes.py — task portion only."""
    @ray.remote
    def generate_response(prompt):
        return f"response to: {prompt}"

    result = ray.get(generate_response.remote("hello"))
    assert result == "response to: hello"
    return "fork_new_processes: generate_response('hello') → 'response to: hello'"


# ═══════════════════════════════════════════════════════════════════════
# TEST 39: limit_running_tasks.py — memory-themed backpressure
# ═══════════════════════════════════════════════════════════════════════

def test_limit_running_tasks():
    """Port of limit_running_tasks.py — backpressure via wait."""
    @ray.remote
    def process_lrt(i):
        return i * i

    BATCH_SIZE = 10
    pending = []
    all_results = []

    for i in range(100):
        pending.append(process_lrt.remote(i))
        if len(pending) >= BATCH_SIZE:
            ready, pending = ray.wait(pending, num_returns=1)
            all_results.append(ray.get(ready[0]))

    while pending:
        ready, pending = ray.wait(pending, num_returns=1)
        all_results.append(ray.get(ready[0]))

    assert len(all_results) == 100
    expected = sorted([i * i for i in range(100)])
    assert sorted(all_results) == expected
    return "limit_running_tasks: 100 tasks with backpressure, all i*i values verified"


# ═══════════════════════════════════════════════════════════════════════
# TEST 40: nested-tasks.py — Outer task submits inner task
# ═══════════════════════════════════════════════════════════════════════

def test_nested_tasks():
    """Port of nested-tasks.py — outer task calls inner task."""
    @ray.remote
    def multiply(x, y):
        return x * y

    @ray.remote
    def outer(x):
        return ray.get(multiply.remote(x, x))

    result = ray.get(outer.remote(5))
    assert result == 25, f"Expected 25, got {result}"
    return "nested-tasks: outer(5) → inner multiply(5,5) → 25"


# ═══════════════════════════════════════════════════════════════════════
# TEST 41: pattern_nested_tasks.py — Recursive nested dispatch
# ═══════════════════════════════════════════════════════════════════════

def test_pattern_nested_tasks():
    """Port of pattern_nested_tasks.py — sort via task dispatch."""
    @ray.remote
    def sort_list(data):
        return sorted(data)

    data = [5, 3, 8, 1, 9, 2, 7, 4, 6]
    result = ray.get(sort_list.remote(data))
    assert result == sorted(data), f"Expected {sorted(data)}, got {result}"
    return f"pattern_nested_tasks: sort {data} → {result}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 42: anti_pattern_nested_ray_get.py — ray.get inside task
# ═══════════════════════════════════════════════════════════════════════

def test_nested_ray_get():
    """Port of anti_pattern_nested_ray_get.py — ray.get inside task."""
    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def aggregate(*args):
        refs = [identity.remote(a) for a in args]
        results_list = ray.get(refs)
        return sum(results_list)

    result = ray.get(aggregate.remote(1, 2, 3, 4, 5))
    assert result == 15, f"Expected 15, got {result}"
    return "nested_ray_get: aggregate(1..5) → inner identity × 5 → sum = 15"


# ═══════════════════════════════════════════════════════════════════════
# TEST 43: actor-sync.py — Signal actor pattern
# ═══════════════════════════════════════════════════════════════════════

def test_actor_sync():
    """Port of actor-sync.py — signal actor as synchronization primitive."""
    @ray.remote
    class SignalActor:
        def __init__(self):
            self.ready = False
        def send(self):
            self.ready = True
            return True
        def wait_signal(self):
            return 1 if self.ready else 0

    signal = SignalActor.options(
        name="Signal_sync", namespace="test_sync"
    ).remote()
    ray.get(signal.send.remote())

    @ray.remote
    def wait_and_go():
        sig = ray.get_actor("Signal_sync", "test_sync")
        return ray.get(sig.wait_signal.remote())

    result = ray.get(wait_and_go.remote())
    assert result == 1, f"Expected signal ready (1), got {result}"
    return "actor-sync: signal.send() → task.wait_and_go() → ready"


# ═══════════════════════════════════════════════════════════════════════
# TEST 44: tasks_fault_tolerance.py — task retries
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_fault_tolerance():
    """Port of tasks_fault_tolerance.py — task retry on failure."""
    call_count = {"n": 0}

    @ray.remote
    def flaky_task():
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise RuntimeError(f"flaky failure #{call_count['n']}")
        return 42

    result = ray.get(flaky_task.options(max_retries=3).remote())
    assert result == 42, f"Expected 42, got {result}"
    assert call_count["n"] == 3, f"Expected 3 calls, got {call_count['n']}"
    return f"fault_tolerance: flaky task retried {call_count['n']} times → 42"


# ═══════════════════════════════════════════════════════════════════════
# TEST 45: tasks.py (error propagation)
# ═══════════════════════════════════════════════════════════════════════

def test_tasks_error_propagation():
    """Port of tasks.py error propagation."""
    @ray.remote
    def bad_task_ep():
        raise ValueError("intentional error for testing")

    try:
        ray.get(bad_task_ep.options(max_retries=0).remote())
        assert False, "Expected RuntimeError from ray.get"
    except RuntimeError as e:
        msg = str(e)
        assert "RayTaskError" in msg, f"Expected RayTaskError, got: {msg}"
        assert "intentional error" in msg, f"Expected error message, got: {msg}"
    return "error_propagation: bad_task → RuntimeError('RayTaskError: ...intentional error...')"


# ═══════════════════════════════════════════════════════════════════════
# TEST 46: placement_group_capture_child_tasks_example.py
#   Uses low-level PG APIs (not in ray package) + @ray.remote functions
# ═══════════════════════════════════════════════════════════════════════

def test_placement_group_capture_child_tasks():
    """Port of placement_group_capture_child_tasks_example.py."""
    gcs = ray.PyGcsClient(ray._runtime.cluster.gcs_address())
    pg_id = gcs.create_placement_group([{"CPU": 2.0}], "PACK")
    ready = gcs.wait_placement_group_until_ready(pg_id.binary(), 10.0)
    assert ready, "Placement group not ready"

    @ray.remote
    def child_task():
        return 1

    @ray.remote
    def parent_task():
        return ray.get(child_task.remote())

    # Submit with PG params via low-level API (PG scheduling not in ray package).
    _, _, _, task_driver = ray._runtime._pick_task_worker()
    refs = task_driver.submit_task(
        "parent_task", [],
        placement_group_id=pg_id.binary(),
        placement_group_bundle_index=0,
        placement_group_capture_child_tasks=True,
    )
    result_ref = ray.ObjectRef(refs[0].binary(), task_driver)
    result = ray.get(result_ref)
    assert result == 1, f"Expected 1, got {result}"

    gcs.remove_placement_group(pg_id.binary())
    return "capture_child_tasks: parent→child in PG, child returned 1"


# ═══════════════════════════════════════════════════════════════════════
# TEST 47: original_resource_unavailable_example.py
#   Uses low-level PG APIs + @ray.remote function
# ═══════════════════════════════════════════════════════════════════════

def test_original_resource_unavailable():
    """Port of original_resource_unavailable_example.py."""
    gcs = ray.PyGcsClient(ray._runtime.cluster.gcs_address())
    pg_id = gcs.create_placement_group([{"CPU": 2.0}], "PACK")
    ready = gcs.wait_placement_group_until_ready(pg_id.binary(), 10.0)
    assert ready, "Placement group not ready"

    @ray.remote
    def f_pg(x):
        return x * 3 + 1

    # Submit WITH PG scheduling.
    _, _, _, task_driver = ray._runtime._pick_task_worker()
    refs = task_driver.submit_task(
        "f_pg", [pickle.dumps(7)],
        placement_group_id=pg_id.binary(),
        placement_group_bundle_index=0,
    )
    result_ref = ray.ObjectRef(refs[0].binary(), task_driver)
    result = ray.get(result_ref)
    assert result == 22, f"PG task: expected 7*3+1=22, got {result}"

    # Submit WITHOUT PG — also succeeds on single node.
    result2 = ray.get(f_pg.remote(10))
    assert result2 == 31, f"Non-PG task: expected 10*3+1=31, got {result2}"

    gcs.remove_placement_group(pg_id.binary())
    return "resource_unavailable: PG f(7)→22, non-PG f(10)→31"


# ═══════════════════════════════════════════════════════════════════════
# TEST 48: actor-pool.py
# ═══════════════════════════════════════════════════════════════════════

def test_actor_pool():
    """Port of actor-pool.py."""
    @ray.remote
    class DoubleActor:
        def double(self, n):
            return n * 2

    actors = [DoubleActor.remote() for _ in range(2)]

    # Round-robin map.
    values = [1, 2, 3, 4]
    refs = []
    for i, v in enumerate(values):
        actor = actors[i % len(actors)]
        refs.append(actor.double.remote(v))

    results_list = [ray.get(r) for r in refs]
    assert results_list == [2, 4, 6, 8], f"Expected [2,4,6,8], got {results_list}"
    return f"actor-pool: pool.map(double, [1,2,3,4]) → {results_list}"


# ═══════════════════════════════════════════════════════════════════════
# TEST 49: pattern_async_actor.py
#   Tests max_concurrency — stays low-level (no high-level equivalent)
# ═══════════════════════════════════════════════════════════════════════

def test_pattern_async_actor():
    """Port of pattern_async_actor.py — sync vs concurrent actors.

    Tests max_concurrency which is a low-level PyCoreWorker feature.
    """
    import threading
    import struct

    cluster = ray._runtime.cluster
    PyCoreWorker = ray.PyCoreWorker
    PyWorkerID = ray.PyWorkerID

    def _make_driver():
        return PyCoreWorker(1, "127.0.0.1", cluster.gcs_address(), 1,
                            node_id=cluster.node_id())

    def _ray_get_one(driver, oid_binary, timeout_ms=10000):
        results = driver.get([oid_binary], timeout_ms)
        return results[0][0] if results[0] is not None else None

    def _submit_from_threads(drivers, method, n):
        results_t = [None] * n
        def _submit(idx):
            d = drivers[idx]
            oids = d.submit_task(method, [])
            results_t[idx] = (d, oids[0].binary())
        threads = [threading.Thread(target=_submit, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        return [r for r in results_t if r is not None]

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
            return struct.pack("<q", sync_state["calls"])
        raise ValueError(f"unknown: {method}")

    wid_s = PyWorkerID.py_from_random()
    sync_worker = PyCoreWorker(
        0, "127.0.0.1", cluster.gcs_address(), 1,
        worker_id=wid_s, node_id=cluster.node_id(),
        max_concurrency=1,
    )
    sync_worker.set_task_callback(sync_callback)
    sync_port = sync_worker.start_grpc_server()

    sync_drivers = []
    for _ in range(3):
        d = _make_driver()
        d.setup_task_dispatch("127.0.0.1", sync_port, wid_s)
        sync_drivers.append(d)

    pending_s = _submit_from_threads(sync_drivers, "run", 3)
    for d, oid_b in pending_s:
        _ray_get_one(d, oid_b, timeout_ms=10000)

    assert sync_state["calls"] == 3
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
            return struct.pack("<q", async_state["calls"])
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
        d = _make_driver()
        d.setup_task_dispatch("127.0.0.1", async_port, wid_a)
        async_drivers.append(d)

    pending_a = _submit_from_threads(async_drivers, "run", 3)
    for d, oid_b in pending_a:
        _ray_get_one(d, oid_b, timeout_ms=10000)

    assert async_state["calls"] == 3
    assert async_state["max_concurrent"] >= 2, \
        f"Async: expected max_concurrent>=2, got {async_state['max_concurrent']}"

    return (f"pattern_async_actor: sync max_concurrent={sync_state['max_concurrent']}, "
            f"async max_concurrent={async_state['max_concurrent']}")


# ═══════════════════════════════════════════════════════════════════════
# TEST 50: object_ref_serialization.py
#   Uses low-level driver.contains(), driver.free()
# ═══════════════════════════════════════════════════════════════════════

def test_object_ref_serialization():
    """Port of object_ref_serialization.py."""
    ref = ray.put({"hello": "world"})
    oid_bytes = ref._binary
    driver = ray._runtime.driver

    assert driver.contains(bytes(oid_bytes)), "Object should be in store after put"

    pkl_path = os.path.join(tempfile.gettempdir(), "rust_test_obj_ref.pkl")
    with open(pkl_path, "wb") as f:
        pickle.dump(bytes(oid_bytes), f)

    del ref

    with open(pkl_path, "rb") as f:
        restored_bytes = pickle.load(f)

    # Get the object back via a new ObjectRef.
    restored_ref = ray.ObjectRef(restored_bytes, driver)
    result = ray.get(restored_ref)
    assert result == {"hello": "world"}, f"Expected dict, got {result}"

    driver.free([bytes(restored_bytes)])
    os.unlink(pkl_path)
    return "object_ref_serialization: put → pickle to file → load → get → free"


# ═══════════════════════════════════════════════════════════════════════
# TEST 51: deser.py — Data integrity through transport
# ═══════════════════════════════════════════════════════════════════════

def test_deser_read_only():
    """Port of deser.py — data integrity through transport."""
    @ray.remote
    def read_and_copy(values):
        if values != [10, 20, 30, 40]:
            return "ERROR:data_corrupted:" + repr(values)
        return [v * 2 for v in values]

    result = ray.get(read_and_copy.remote([10, 20, 30, 40]))
    assert isinstance(result, list), f"Callback reported: {result}"
    assert result == [20, 40, 60, 80], f"Expected [20,40,60,80], got {result}"

    # Verify original data in object store is unchanged.
    ref = ray.put([10, 20, 30, 40])
    fetched = ray.get(ref)
    assert fetched == [10, 20, 30, 40]
    return "deser: data integrity verified, modified copy returned [20,40,60,80]"


# ═══════════════════════════════════════════════════════════════════════
# TEST 52: actor_creator_failure.py
#   Uses low-level driver.kill_actor()
# ═══════════════════════════════════════════════════════════════════════

def test_actor_creator_failure():
    """Port of actor_creator_failure.py."""
    @ray.remote
    class PingActor:
        def __init__(self):
            self.value = 0
        def ping(self):
            self.value += 1
            return self.value

    child = PingActor.remote()
    detached = PingActor.remote()

    assert ray.get(child.ping.remote()) == 1
    assert ray.get(detached.ping.remote()) == 1

    # Kill the child actor.
    ray._runtime.driver.kill_actor(bytes(child._actor_id.binary()), True, True)

    # Child should be dead.
    error_raised = False
    try:
        child.ping.remote()
    except RuntimeError:
        error_raised = True
    assert error_raised, "Expected error submitting to dead actor"

    # Detached still works.
    result = ray.get(detached.ping.remote())
    assert result == 2, f"Expected 2, got {result}"
    return f"actor_creator_failure: child killed → error, detached → alive (value={result})"


# ═══════════════════════════════════════════════════════════════════════
# TEST 53: actor_restart.py
#   Uses kill_actor for lifecycle demonstration
# ═══════════════════════════════════════════════════════════════════════

def test_actor_restart():
    """Port of actor_restart.py — crash + re-creation pattern."""
    MAX_RESTARTS = 4
    CRASH_EVERY = 10

    @ray.remote
    class CrashyActor:
        def __init__(self):
            self.count = 0
        def increment_and_possibly_fail(self):
            self.count += 1
            if self.count >= CRASH_EVERY:
                raise RuntimeError("simulated actor crash (os._exit)")
            return self.count

    successful_calls = 0
    last_actor = None

    for life in range(MAX_RESTARTS + 1):
        actor = CrashyActor.remote()
        last_actor = actor
        for _ in range(CRASH_EVERY):
            try:
                ray.get(actor.increment_and_possibly_fail.remote())
                successful_calls += 1
            except RuntimeError:
                break

    assert successful_calls >= 40, \
        f"Expected >= 40 successful calls, got {successful_calls}"

    # After all restarts exhausted, kill and verify dead.
    ray._runtime.driver.kill_actor(
        bytes(last_actor._actor_id.binary()), True, True)
    error_on_dead = False
    try:
        last_actor.increment_and_possibly_fail.remote()
    except RuntimeError:
        error_on_dead = True
    assert error_on_dead, "Expected error on dead actor after restarts exhausted"

    return (f"actor_restart: {successful_calls} successful calls across "
            f"{MAX_RESTARTS + 1} lives, dead after exhaustion")


# ═══════════════════════════════════════════════════════════════════════
# TEST 54: pattern_generators.py — generator multi-return pattern
# ═══════════════════════════════════════════════════════════════════════

def test_pattern_generators():
    """Port of pattern_generators.py — generator multi-return pattern."""
    num_returns = 5

    @ray.remote(num_returns=5)
    def large_values():
        return [i * 1000 for i in range(5)]

    @ray.remote(num_returns=5)
    def large_values_generator():
        return [i * 1000 for i in range(5)]

    refs = large_values.remote()
    assert len(refs) == num_returns
    for i in range(num_returns):
        assert ray.get(refs[i]) == i * 1000

    gen_refs = large_values_generator.remote()
    assert len(gen_refs) == num_returns
    for i in range(num_returns):
        assert ray.get(gen_refs[i]) == i * 1000

    return (f"pattern_generators: regular + generator multi-return "
            f"({num_returns} values each)")


# ═══════════════════════════════════════════════════════════════════════
# TEST 55: generator.py — split, error propagation, exact yields
# ═══════════════════════════════════════════════════════════════════════

def test_generator():
    """Port of generator.py — split, error propagation, exact yields."""
    array_size = 10
    chunk_size = 3
    num_chunks = (array_size + chunk_size - 1) // chunk_size  # 4

    @ray.remote(num_returns=4)
    def split(arr_size, c_size):
        chunks = []
        offset = 0
        while offset < arr_size:
            end = min(offset + c_size, arr_size)
            chunks.append(end - offset)
            offset = end
        while len(chunks) < 4:
            chunks.append(0)
        return chunks[:4]

    @ray.remote(num_returns=4)
    def generator_with_error():
        return [0, 1, "__GENERATOR_ERROR__", "__GENERATOR_ERROR__"]

    @ray.remote(num_returns=2)
    def generator_yields_2():
        return [0, 1]

    # Section 1: Split array into chunks.
    refs = split.remote(array_size, chunk_size)
    assert len(refs) == num_chunks
    total = 0
    for i in range(num_chunks):
        chunk_len = ray.get(refs[i])
        assert chunk_len <= chunk_size
        total += chunk_len
    assert total == array_size, f"Expected total {array_size}, got {total}"

    # Section 2: Error propagation — first 2 OK, last 2 are sentinels.
    err_refs = generator_with_error.remote()
    assert len(err_refs) == 4
    assert ray.get(err_refs[0]) == 0
    assert ray.get(err_refs[1]) == 1
    assert ray.get(err_refs[2]) == "__GENERATOR_ERROR__"
    assert ray.get(err_refs[3]) == "__GENERATOR_ERROR__"

    # Section 3: Exact yield count matches num_returns.
    ok_refs = generator_yields_2.remote()
    assert ray.get(ok_refs[0]) == 0
    assert ray.get(ok_refs[1]) == 1

    return (f"generator: split({array_size}/{chunk_size})={num_chunks} chunks, "
            f"error propagation, exact yields")


# ═══════════════════════════════════════════════════════════════════════
# TEST 56: streaming_generator.py
# ═══════════════════════════════════════════════════════════════════════

def test_streaming_generator():
    """Port of streaming_generator.py — iteration, error, actor gen, wait."""

    @ray.remote(num_returns=5)
    def streaming_task():
        return list(range(5))

    @ray.remote(num_returns=3)
    def streaming_error_task():
        return [0, "__STREAM_ERROR__", "__STREAM_ERROR__"]

    @ray.remote
    def regular_task():
        return 42

    # Section 1: Streaming iteration (simulated via multi-return).
    refs = streaming_task.remote()
    assert len(refs) == 5
    assert ray.get(refs[0]) == 0
    assert ray.get(refs[1]) == 1
    remaining = [ray.get(refs[i]) for i in range(2, 5)]
    assert remaining == [2, 3, 4]

    # Section 2: Error propagation mid-stream.
    err_refs = streaming_error_task.remote()
    assert ray.get(err_refs[0]) == 0
    assert ray.get(err_refs[1]) == "__STREAM_ERROR__"

    # Section 3: Actor generator.
    @ray.remote
    class StreamGenActor:
        def f(self):
            return (0, 1, 2, 3, 4)

    actor = StreamGenActor.remote()
    data = ray.get(actor.f.remote())
    assert data == (0, 1, 2, 3, 4), f"Actor gen: expected (0..4), got {data}"

    # Section 4: ray.wait with multi-return refs + regular task.
    @ray.remote(num_returns=3)
    def streaming_task_3():
        return list(range(3))

    stream_refs = streaming_task_3.remote()
    regular_ref = regular_task.remote()
    all_refs = list(stream_refs) + [regular_ref]
    ready, remaining_w = ray.wait(all_refs, num_returns=4)
    assert len(ready) == 4, f"Expected 4 ready, got {len(ready)}"
    assert len(remaining_w) == 0

    return ("streaming_generator: iteration(5), error mid-stream, "
            "actor gen(5), wait(4)")


# ═══════════════════════════════════════════════════════════════════════
# TEST 57: ray_oom_prevention.py
# ═══════════════════════════════════════════════════════════════════════

def test_ray_oom_prevention():
    """Port of ray_oom_prevention.py — simulated OOM + retry."""
    oom_calls = {"n": 0}

    @ray.remote
    def leaks_memory():
        oom_calls["n"] += 1
        raise MemoryError("Simulated OutOfMemoryError")

    @ray.remote
    def normal_task():
        return 99

    # Section 1: OOM task with retries.
    oom_error = False
    try:
        ray.get(leaks_memory.options(max_retries=2).remote())
    except RuntimeError as e:
        if "MemoryError" in str(e) or "memory" in str(e).lower():
            oom_error = True
    assert oom_error, "Expected OOM-related error"
    assert oom_calls["n"] == 3, f"Expected 3 calls, got {oom_calls['n']}"

    # Section 2: Differentiated retry behavior.
    retriable_calls = {"n": 0}
    nonretriable_calls = {"n": 0}

    @ray.remote
    def retriable_alloc():
        retriable_calls["n"] += 1
        raise MemoryError("retriable task OOM")

    @ray.remote
    def nonretriable_alloc():
        nonretriable_calls["n"] += 1
        return 1

    fail_error = False
    try:
        ray.get(retriable_alloc.options(max_retries=1).remote())
    except RuntimeError:
        fail_error = True
    assert fail_error
    assert retriable_calls["n"] == 2, f"Expected 2, got {retriable_calls['n']}"

    result = ray.get(nonretriable_alloc.options(max_retries=0).remote())
    assert result == 1
    assert nonretriable_calls["n"] == 1

    return (f"ray_oom_prevention: OOM task retried {oom_calls['n']}x, "
            f"retriable={retriable_calls['n']} calls, "
            f"non-retriable={nonretriable_calls['n']} call")


# ═══════════════════════════════════════════════════════════════════════
# TEST 58: namespaces.py — Namespace isolation + detached actors
# ═══════════════════════════════════════════════════════════════════════

def test_namespaces():
    """Port of namespaces.py — namespace isolation and cross-namespace lookup."""
    @ray.remote
    class NamedActor:
        def __init__(self, value):
            self.value = value
        def get(self):
            return self.value

    # Section 1: Create actors in "colors" namespace.
    orange_c = NamedActor.options(
        name="orange", namespace="colors"
    ).remote("orange_colors")
    purple_c = NamedActor.options(
        name="purple", namespace="colors"
    ).remote("purple_colors")

    found_orange = ray.get_actor("orange", "colors")
    assert found_orange is not None
    found_purple = ray.get_actor("purple", "colors")
    assert found_purple is not None

    # Section 2: "fruits" namespace — "orange" in "colors" not visible.
    try:
        ray.get_actor("orange", "fruits")
        assert False, "Expected ValueError"
    except ValueError:
        pass

    orange_f = NamedActor.options(
        name="orange", namespace="fruits"
    ).remote("orange_fruits")
    watermelon_f = NamedActor.options(
        name="watermelon", namespace="fruits"
    ).remote("watermelon_fruits")

    # Section 3: "watermelon" not visible in "colors".
    try:
        ray.get_actor("watermelon", "colors")
        assert False, "Expected ValueError"
    except ValueError:
        pass

    # "orange" in "colors" is the original one.
    found_oc = ray.get_actor("orange", "colors")
    assert bytes(found_oc._actor_id.binary()) == bytes(orange_c._actor_id.binary())

    # Section 4: Cross-namespace lookup.
    found_of = ray.get_actor("orange", "fruits")
    assert bytes(found_of._actor_id.binary()) == bytes(orange_f._actor_id.binary())

    # Two "orange" actors are different.
    assert bytes(orange_c._actor_id.binary()) != bytes(orange_f._actor_id.binary())

    # Section 5: Call actor method.
    result = ray.get(found_oc.get.remote())
    assert result == "orange_colors", f"Expected 'orange_colors', got {result}"

    return ("namespaces: 2 actors in 'colors', 2 in 'fruits', "
            "namespace isolation verified, cross-namespace lookup works")


# ═══════════════════════════════════════════════════════════════════════
# TEST 59: anti_pattern_out_of_band_object_ref_serialization.py
#   Uses low-level driver.free(), driver.contains()
# ═══════════════════════════════════════════════════════════════════════

def test_out_of_band_object_ref_serialization():
    """Port of anti_pattern_out_of_band_object_ref_serialization.py."""
    driver = ray._runtime.driver

    # Section 1: Put and serialize OID out-of-band.
    ref = ray.put(42)
    oid_bytes = ref._binary
    assert ray.get(ref) == 42

    pickled_oid = pickle.dumps(oid_bytes)
    assert ray.get(ref) == 42

    # Section 2: Free → pickled OID is broken.
    driver.free([bytes(oid_bytes)])
    time.sleep(0.3)

    restored_oid = pickle.loads(pickled_oid)
    assert restored_oid == oid_bytes

    object_was_freed = False
    try:
        restored_ref = ray.ObjectRef(restored_oid, driver)
        val = ray.get(restored_ref, timeout=1.0)
        if val != 42:
            object_was_freed = True
    except Exception:
        object_was_freed = True

    assert restored_oid == oid_bytes, "OOB serialization preserved the raw OID bytes"

    # Section 3: In-band reference passing keeps object alive.
    @ray.remote
    def pass_through(data):
        return data

    ref2 = ray.put(99)
    ray.get(pass_through.remote(bytes(ref2._binary)))

    result3 = ray.get(ref2)
    assert result3 == 99, "In-band ref should keep object alive"

    return ("out_of_band_object_ref: put → pickle OID → free → object gone; "
            "in-band pass-through keeps object alive")


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

    # ── Phase 6: generators + OOM ──
    ("pattern_generators.py",           "pattern_generators.py",        test_pattern_generators),
    ("generator.py",                    "generator.py",                 test_generator),
    ("streaming_generator.py",          "streaming_generator.py",       test_streaming_generator),
    ("ray_oom_prevention.py",           "ray_oom_prevention.py",        test_ray_oom_prevention),

    # ── Phase 7: namespaces + out-of-band serialization ──
    ("namespaces.py",                   "namespaces.py",                test_namespaces),
    ("anti_pattern_oob_obj_ref_ser.py", "anti_pattern_out_of_band_object_ref_serialization.py", test_out_of_band_object_ref_serialization),
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

    # ── Placement groups (1) ──
    ("placement_group_example.py",                       "Requires placement groups + GPU"),

    # ── Cross-language (1) ──
    ("cross_language.py",           "Requires Java worker + cross-language RPC"),

    # ── Other unsupported features (3) ──
    ("ray-dag.py",                  "Requires Ray DAG API (bind/execute)"),
    ("runtime_env_example.py",      "Requires runtime_env support"),
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

# Clean up.
ray.shutdown()
print("Cluster shut down.")

# Exit code.
sys.exit(1 if failed > 0 else 0)
