from typing import Dict

import ray


@ray.remote
class _MemActor:
    def __init__(self):
        self.allocated: Dict[ray.ObjectRef, dict] = {}
        self.deallocated: Dict[ray.ObjectRef, dict] = {}
        self.skip_dealloc: Dict[ray.ObjectRef, str] = {}
        self.peak_mem = 0
        self.cur_mem = 0

    def trace_alloc(self, ref, loc):
        ref = ref[0]
        if ref not in self.allocated:
            meta = ray.experimental.get_object_locations([ref])
            size_bytes = meta.get("object_size", 0)
            if not size_bytes:
                size_bytes = -1
                from ray import cloudpickle as pickle

                try:
                    obj = ray.get(ref, timeout=5.0)
                    size_bytes = len(pickle.dumps(obj))
                except Exception:
                    print("[mem_tracing] ERROR getting size")
                    size_bytes = -1
            print(f"[mem_tracing] Allocated {size_bytes} bytes at {loc}: {ref}")
            entry = {
                "size_bytes": size_bytes,
                "loc": loc,
            }
            self.allocated[ref] = entry
            self.cur_mem += size_bytes
            self.peak_mem = max(self.cur_mem, self.peak_mem)

    def trace_dealloc(self, ref, loc, freed):
        ref = ref[0]
        size_bytes = self.allocated.get(ref, {}).get("size_bytes", 0)
        if freed:
            print(f"[mem_tracing] Freed {size_bytes} bytes at {loc}: {ref}")
            if ref in self.allocated:
                self.cur_mem -= size_bytes
                self.deallocated[ref] = self.allocated[ref]
                self.deallocated[ref]["dealloc_loc"] = loc
                del self.allocated[ref]
            else:
                print(f"[mem_tracing] WARNING: allocation of {ref} was not traced!")
        else:
            print(f"[mem_tracing] Skipped freeing {size_bytes} bytes at {loc}: {ref}")
            self.skip_dealloc[ref] = loc

    def leak_report(self):
        print("[mem_tracing] ===== Leaked objects =====")
        for ref in self.allocated:
            size_bytes = self.allocated[ref].get("size_bytes")
            loc = self.allocated[ref].get("loc")
            if ref in self.skip_dealloc:
                dealloc_loc = self.skip_dealloc[ref]
                print(
                    f"[mem_tracing] Leaked object, created at {loc}, size "
                    f"{size_bytes}, skipped dealloc at {dealloc_loc}: {ref}"
                )
            else:
                print(
                    f"[mem_tracing] Leaked object, created at {loc}, "
                    f"size {size_bytes}: {ref}"
                )
        print("[mem_tracing] ===== End leaked objects =====")
        print("[mem_tracing] ===== Freed objects =====")
        for ref in self.deallocated:
            size_bytes = self.deallocated[ref].get("size_bytes")
            loc = self.deallocated[ref].get("loc")
            dealloc_loc = self.deallocated[ref].get("dealloc_loc")
            print(
                f"[mem_tracing] Freed obj from {loc} at {dealloc_loc}, "
                f"size {size_bytes}: {ref}"
            )
        print("[mem_tracing] ===== End freed objects =====")
        print(f"[mem_tracing] Peak size bytes {self.peak_mem}")
        print(f"[mem_tracing] Current size bytes {self.cur_mem}")


def _get_mem_actor():
    return _MemActor.options(
        name="mem_tracing_actor", get_if_exists=True, lifetime="detached"
    ).remote()


def _trace_allocation(ref: ray.ObjectRef, loc: str) -> None:
    ctx = DatasetContext.get_current()
    if ctx.trace_allocations:
        tracer = _get_mem_actor()
        ray.get(tracer.trace_alloc.remote([ref], loc))


def _trace_deallocation(ref: ray.ObjectRef, loc: str, freed: bool = True) -> None:
    if freed:
        ray._private.internal_api.free(ref, local_only=False)
    ctx = DatasetContext.get_current()
    if ctx.trace_allocations:
        tracer = _get_mem_actor()
        ray.get(tracer.trace_dealloc.remote([ref], loc, freed))


def _leak_report() -> None:
    tracer = _get_mem_actor()
    ray.get(tracer.leak_report.remote())
