#!/usr/bin/env python3
"""Stress test: producer-consumer patterns, streaming, pipelines.

Tests:
  1. Single producer -> single consumer via ray.wait loop (500 items)
  2. 3 producers -> 1 consumer (interleaved, verify total count)
  3. Pipeline: Producer -> Transformer -> Collector (200 items)
  4. Bounded buffer: producer waits when buffer is full
  5. Work-stealing: multiple consumers pulling from shared queue
"""

import ray

ray.init(num_task_workers=4)

passed = 0
total = 0

# ── Test 1: Single producer -> consumer via ray.wait ────────────────

@ray.remote
class Producer:
    def __init__(self, producer_id):
        self.producer_id = producer_id
        self.seq = 0
    def produce(self):
        item = (self.producer_id, self.seq)
        self.seq += 1
        return item
    def get_produced_count(self):
        return self.seq

total += 1
producer = Producer.remote("P0")
# Producer generates 500 items
item_refs = [producer.produce.remote() for _ in range(500)]

# Consumer collects via ray.wait batches
collected = []
remaining = list(item_refs)
while remaining:
    batch_size = min(50, len(remaining))
    ready, remaining = ray.wait(remaining, num_returns=batch_size, timeout=10.0)
    collected.extend(ray.get(ready))

# Verify all 500 items received from producer P0
p0_items = [item for item in collected if item[0] == "P0"]
p0_seqs = sorted([item[1] for item in p0_items])
expected_seqs = list(range(500))

if p0_seqs == expected_seqs:
    print(f"  PASS  Single producer->consumer: {len(p0_items)} items, all sequences correct")
    passed += 1
else:
    print(f"  FAIL  Single producer: got {len(p0_items)} items, sequences mismatch")

# ── Test 2: 3 producers -> 1 consumer ──────────────────────────────

total += 1
producers = [Producer.remote(f"P{i}") for i in range(3)]
all_refs = []
items_per_producer = 200

for p in producers:
    for _ in range(items_per_producer):
        all_refs.append(p.produce.remote())

# Collect all
collected = []
remaining = list(all_refs)
while remaining:
    batch_size = min(100, len(remaining))
    ready, remaining = ray.wait(remaining, num_returns=batch_size, timeout=10.0)
    collected.extend(ray.get(ready))

# Verify: 600 total items, 200 from each producer
by_producer = {}
for pid, seq in collected:
    by_producer.setdefault(pid, []).append(seq)

all_correct = True
for pid in ["P0", "P1", "P2"]:
    seqs = sorted(by_producer.get(pid, []))
    if seqs != list(range(items_per_producer)):
        all_correct = False
        print(f"  FAIL  Producer {pid}: expected {items_per_producer} items, got {len(seqs)}")

if all_correct and len(collected) == 3 * items_per_producer:
    print(f"  PASS  3 producers -> consumer: {len(collected)} total items, all sequences correct")
    passed += 1

# ── Test 3: Pipeline: Producer -> Transformer -> Collector ──────────

@ray.remote
def transform(item):
    pid, seq = item
    return (pid, seq, seq * seq)  # Add square of sequence number

@ray.remote
class Collector:
    def __init__(self):
        self.items = []
    def add(self, item):
        self.items.append(item)
        return len(self.items)
    def get_all(self):
        return self.items
    def get_count(self):
        return len(self.items)

total += 1
pipeline_producer = Producer.remote("PIPE")
collector = Collector.remote()

# Generate 200 items
produce_refs = [pipeline_producer.produce.remote() for _ in range(200)]

# Transform each item
transform_refs = []
for ref in produce_refs:
    item = ray.get(ref)
    transform_refs.append(transform.remote(item))

# Collect transformed items
for ref in transform_refs:
    transformed = ray.get(ref)
    collector.add.remote(transformed)

# Wait for collector to finish
count = ray.get(collector.get_count.remote())
all_items = ray.get(collector.get_all.remote())

# Verify
all_correct = True
if count != 200:
    all_correct = False

# Check that all items have the expected shape
for item in all_items:
    pid, seq, sq = item
    if pid != "PIPE" or sq != seq * seq:
        all_correct = False
        break

if all_correct:
    print(f"  PASS  Pipeline (produce->transform->collect): {count} items processed")
    passed += 1
else:
    print(f"  FAIL  Pipeline: count={count}, expected 200")

# ── Test 4: Bounded buffer pattern ──────────────────────────────────

@ray.remote
class BoundedBuffer:
    def __init__(self, capacity):
        self.capacity = capacity
        self.buffer = []
        self.total_produced = 0
        self.total_consumed = 0
    def try_put(self, item):
        if len(self.buffer) >= self.capacity:
            return False
        self.buffer.append(item)
        self.total_produced += 1
        return True
    def try_get(self):
        if not self.buffer:
            return None
        item = self.buffer.pop(0)
        self.total_consumed += 1
        return item
    def get_stats(self):
        return {
            "produced": self.total_produced,
            "consumed": self.total_consumed,
            "buffered": len(self.buffer),
        }

total += 1
buf = BoundedBuffer.remote(10)
produced = 0
consumed = 0
items_to_produce = 300
consumed_items = []

# Produce and consume interleaved
i = 0
while produced < items_to_produce or consumed < items_to_produce:
    # Try to produce
    if produced < items_to_produce:
        success = ray.get(buf.try_put.remote(produced))
        if success:
            produced += 1

    # Try to consume
    item = ray.get(buf.try_get.remote())
    if item is not None:
        consumed_items.append(item)
        consumed += 1

    i += 1
    if i > items_to_produce * 10:
        # Safety valve
        break

stats = ray.get(buf.get_stats.remote())
if stats["produced"] == items_to_produce and stats["consumed"] == items_to_produce and stats["buffered"] == 0:
    print(f"  PASS  Bounded buffer: {stats['produced']} produced, {stats['consumed']} consumed, buffer empty")
    passed += 1
else:
    print(f"  FAIL  Bounded buffer: {stats}")

# ── Test 5: Work-stealing consumers (driver-coordinated) ────────────

@ray.remote
class WorkQueue:
    def __init__(self, items):
        self.items = list(items)
        self.taken = {}
    def take(self, consumer_id):
        if not self.items:
            return None
        item = self.items.pop(0)
        self.taken.setdefault(consumer_id, []).append(item)
        return item
    def get_stats(self):
        return {cid: len(items) for cid, items in self.taken.items()}

@ray.remote
def process_item(item):
    return item * 2

total += 1
queue = WorkQueue.remote(list(range(100)))
consumer_ids = [f"C{i}" for i in range(4)]
all_processed = []

# Driver coordinates: round-robin consumers take items from queue
all_done = False
rounds = 0
while not all_done and rounds < 200:
    any_got_work = False
    for cid in consumer_ids:
        item = ray.get(queue.take.remote(cid))
        if item is not None:
            result = ray.get(process_item.remote(item))
            all_processed.append(result)
            any_got_work = True
    if not any_got_work:
        all_done = True
    rounds += 1

stats = ray.get(queue.get_stats.remote())
total_taken = sum(stats.values())
expected_processed = sorted([i * 2 for i in range(100)])
actual_processed = sorted(all_processed)

if total_taken == 100 and actual_processed == expected_processed:
    print(f"  PASS  Work-stealing: 100 items across {len(stats)} consumers, distribution={stats}")
    passed += 1
else:
    print(f"  FAIL  Work-stealing: taken={total_taken}, processed={len(all_processed)}")

# ── Summary ──────────────────────────────────────────────────────────

ray.shutdown()
print(f"\n{'ALL' if passed == total else 'SOME'} STRESS TESTS {'PASSED' if passed == total else 'FAILED'} ({passed}/{total})")
