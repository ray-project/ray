import sys, time
import ray

ray.init(num_cpus=24, log_to_driver=False, logging_level="WARNING", include_dashboard=False, object_store_memory=6*1024**3)

from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool

_orig_find = _ActorPool._find_actor_with_locality
_calls = [0]; _returned_actor = [0]; _returned_none = [0]; _iter_ops = [0]; _no_pl = [0]

def _instr(self, bundle):
    _calls[0] += 1
    pl = bundle.get_preferred_object_locations()
    if not pl:
        _no_pl[0] += 1
    else:
        for nid in pl:
            actors = self._alive_node_to_actor_map.get(nid)
            if actors:
                _iter_ops[0] += len(actors)
    r = _orig_find(self, bundle)
    if r is None: _returned_none[0] += 1
    else: _returned_actor[0] += 1
    return r

_ActorPool._find_actor_with_locality = _instr


class TinyActor:
    def __init__(self, work_ms=0):
        self._w = work_ms
        time.sleep(0.05)
    def __call__(self, b):
        if self._w > 0: time.sleep(self._w/1000.0)
        return b


N_BLOCKS = 2_000
BATCH = 100_000   # ~800KB per block (1 int64 col) > inline threshold
M = 80

ds = ray.data.range(N_BLOCKS * BATCH)
ds = ds.repartition(N_BLOCKS)
ds = ds.map_batches(
    TinyActor, fn_constructor_kwargs={"work_ms":0},
    batch_size=BATCH, num_cpus=0.05, concurrency=(M, M)
)
t0 = time.time()
for _ in ds.iter_internal_ref_bundles(): pass
wall = time.time() - t0
print(f"\nN_BLOCKS={N_BLOCKS} BATCH={BATCH} M={M}")
print(f"wall: {wall:.2f}s")
print(f"locality_calls: {_calls[0]}")
print(f"  no_preferred_loc: {_no_pl[0]}")
print(f"  returned_actor: {_returned_actor[0]}")
print(f"  returned_none: {_returned_none[0]}")
print(f"  total glia iter ops: {_iter_ops[0]}")
