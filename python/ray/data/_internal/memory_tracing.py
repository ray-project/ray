"""Utility for debugging object store memory eager deletion in Datasets.

Enable with RAY_DATASET_TRACE_ALLOCATIONS=1.

Basic usage is to call `trace_allocation` each time a new object is created, and call
`trace_deallocation` when an object should be disposed of. When the workload is
complete, call `leak_report` to view possibly leaked objects.

Note that so called "leaked" objects will be reclaimed eventually by reference counting
in Ray. This is just to debug the eager deletion protocol which is more efficient.
"""

from io import StringIO
from typing import Dict

import ray
from ray.data.context import DatasetContext


def trace_allocation(ref: ray.ObjectRef, loc: str) -> None:
    """Record that an object has been created.

    Args:
        ref: The object created.
        loc: A human-readable string identifying the call site.
    """
    ctx = DatasetContext.get_current()
    if ctx.trace_allocations:
        tracer = _get_mem_actor()
        ray.get(tracer.trace_alloc.remote([ref], loc))


def trace_deallocation(ref: ray.ObjectRef, loc: str, free: bool = True) -> None:
    """Record that an object has been deleted (and delete if if free=True).

    Args:
        ref: The object we no longer need.
        loc: A human-readable string identifying the call site.
        free: Whether to eagerly destroy the object instead of waiting for Ray
            reference counting to kick in.
    """
    if free:
        ray._private.internal_api.free(ref, local_only=False)
    ctx = DatasetContext.get_current()
    if ctx.trace_allocations:
        tracer = _get_mem_actor()
        ray.get(tracer.trace_dealloc.remote([ref], loc, free))


def leak_report() -> str:
    tracer = _get_mem_actor()
    return ray.get(tracer.leak_report.remote())


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

    def leak_report(self) -> str:
        output = StringIO()
        output.write("[mem_tracing] ===== Leaked objects =====\n")
        for ref in self.allocated:
            size_bytes = self.allocated[ref].get("size_bytes")
            loc = self.allocated[ref].get("loc")
            if ref in self.skip_dealloc:
                dealloc_loc = self.skip_dealloc[ref]
                output.write(
                    f"[mem_tracing] Leaked object, created at {loc}, size "
                    f"{size_bytes}, skipped dealloc at {dealloc_loc}: {ref}\n"
                )
            else:
                output.write(
                    f"[mem_tracing] Leaked object, created at {loc}, "
                    f"size {size_bytes}: {ref}\n"
                )
        output.write("[mem_tracing] ===== End leaked objects =====\n")
        output.write("[mem_tracing] ===== Freed objects =====\n")
        for ref in self.deallocated:
            size_bytes = self.deallocated[ref].get("size_bytes")
            loc = self.deallocated[ref].get("loc")
            dealloc_loc = self.deallocated[ref].get("dealloc_loc")
            output.write(
                f"[mem_tracing] Freed object from {loc} at {dealloc_loc}, "
                f"size {size_bytes}: {ref}\n"
            )
        output.write("[mem_tracing] ===== End freed objects =====\n")
        output.write(f"[mem_tracing] Peak size bytes {self.peak_mem}\n")
        output.write(f"[mem_tracing] Current size bytes {self.cur_mem}\n")
        return output.getvalue()


def _get_mem_actor():
    return _MemActor.options(
        name="mem_tracing_actor", get_if_exists=True, lifetime="detached"
    ).remote()
