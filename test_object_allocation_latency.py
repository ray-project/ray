"""Drive CreateObjectInternal LRU eviction using true secondary copies.

Two-node cluster on one machine via ray.cluster_utils.Cluster:

  owner node     : driver runs here; ray.put creates primaries in this
                   plasma store, pinned by driver-held ObjectRefs
  consumer node  : Consumer actor pool transfers data over and produces
                   secondary copies that land on consumer plasma's LRU
                   the moment each pull method returns

Phase 1: driver ray.puts each small object on owner plasma. A pool of
         Consumer actors pulls batches of refs; each Consumer.pull
         receives ObjectRefs as args, Ray auto-resolves them (transferring
         the data into consumer plasma as a secondary copy and pinning it
         for the method body). When pull returns, the function locals
         drop and the secondaries land on consumer plasma's LRU.
Phase 2: not needed -- secondaries are already on the LRU.
Phase 3: a Consumer actor's big_put method allocates one 800 MiB primary.
         Plasma's CreateObjectInternal evicts the secondaries via LRU to
         make room and we time end-to-end latency.

Why ray.put on the driver instead of an Owner actor: small task return
values (<= max_direct_call_object_size, default 100 KiB) get inlined
into the task return message and never go through plasma at all. With
OBJ_SIZE = 10 KiB an Owner.create() return is inlined, so the supposed
"primaries" are phantom refs and no secondary copy is ever transferred.
ray.put always materializes in plasma regardless of size.

Run:
    RAY_BACKEND_LOG_LEVEL=debug python fill_plasma_secondary.py

Watch the instrumented timing:
    tail -f /tmp/ray/session_latest/logs/raylet.err | grep CreateObjectInternal
"""

import time

import numpy as np

import ray
from ray.cluster_utils import Cluster


def print_plasma_per_node(label):
    """Print per-node plasma usage including secondary copies.

    NOTE: ray._private.state.available_resources_per_node() reflects only
    the scheduler's logical object_store_memory budget (primary copies).
    Secondaries don't take placement budget so they don't show up there.
    To see actual plasma bytes per node we ask memory_summary to group
    by node address; that path counts every sealed object in each node's
    plasma store, primaries and secondaries alike.
    """
    print(f"-- per-node plasma usage [{label}] --")
    print(
        ray._private.internal_api.memory_summary(
            group_by="NODE_ADDRESS",
            sort_by="OBJECT_SIZE",
            num_entries=0,
        )
    )


OBJ_SIZE = 2 * 1024 * 5  # 10 KiB per small object
STORE_SIZE = 200 * 1024 * 1024 * 5  # 1000 MiB consumer plasma
PHASE1_COUNT = 60 * 1024  # ~600 MiB of secondaries
BIG_OBJ_SIZE = STORE_SIZE * 4 // 5  # 800 MiB primary on consumer
BATCH_SIZE = 1024  # refs per pull call
N_CONSUMERS = 8  # consumer-actor pool size
SETTLE_SECONDS = 5  # let eager free propagate after del primaries


@ray.remote(resources={"consumer": 1}, num_cpus=0)
class Consumer:
    def pull(self, *_values):
        # Each ObjectRef arg is auto-resolved by Ray before invocation:
        # the data is transferred from owner plasma to consumer plasma
        # and pinned for the duration of this method. When it returns,
        # the locals drop and the secondaries land on the consumer
        # plasma's LRU.
        return None

    def big_put(self):
        return np.zeros(BIG_OBJ_SIZE, dtype=np.uint8)


def main():
    cluster = Cluster()
    cluster.add_node(
        num_cpus=2,
        resources={"owner": 100},
        object_store_memory=2 * STORE_SIZE,
    )
    cluster.add_node(
        num_cpus=2,
        resources={"consumer": 100},
        object_store_memory=STORE_SIZE,
    )
    ray.init(address=cluster.address)

    consumers = [Consumer.remote() for _ in range(N_CONSUMERS)]

    # Driver runs on owner_node (head). ray.put always goes to plasma,
    # so each call creates a real primary in owner-node plasma.
    primaries = [
        ray.put(np.zeros(OBJ_SIZE, dtype=np.uint8)) for _ in range(PHASE1_COUNT)
    ]

    pull_handles = []
    for batch_idx, start in enumerate(range(0, len(primaries), BATCH_SIZE)):
        batch = primaries[start : start + BATCH_SIZE]
        actor = consumers[batch_idx % N_CONSUMERS]
        pull_handles.append(actor.pull.remote(*batch))
    ray.get(pull_handles)

    print(f"Phase 1: {PHASE1_COUNT} secondaries on consumer plasma")
    print(ray._private.internal_api.memory_summary(stats_only=True))
    print_plasma_per_node("after pull, primaries still held")

    del primaries
    print(f"Primaries dropped; sleeping {SETTLE_SECONDS}s for eager free to propagate")
    time.sleep(SETTLE_SECONDS)
    print(ray._private.internal_api.memory_summary(stats_only=True))
    print_plasma_per_node(f"after del primaries + {SETTLE_SECONDS}s settle")

    t0 = time.perf_counter()
    big_ref = consumers[0].big_put.remote()
    ray.wait([big_ref], fetch_local=False)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"Phase 3 big put ({BIG_OBJ_SIZE} bytes)  {elapsed_ms:.1f} ms")
    del big_ref

    print()
    print(ray._private.internal_api.memory_summary(stats_only=True))

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
