"""Shared harness for the RDT weight-syncing benchmarks.

The three benchmarks in this directory all model the same workload: a trainer
actor that publishes model weights via ``ray.put(..., _tensor_transport="nixl")``
and a generator (inference) actor that pulls them. They differ only in what they
measure, so the model, the actors, the cross-node placement and the result
reporting live here.

Actors are placed on two different nodes by default, since RDMA registration and
transfer costs are only meaningful over the network. Pass ``--single-node`` for
local correctness checks.
"""

import argparse
import csv
import json
import os
import statistics
import time
from typing import Any, Dict, List, Optional, Sequence

import torch

import ray

KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB


def format_bytes(num_bytes: int) -> str:
    """Render a byte count as a short human-readable string."""
    for unit, scale in (("GB", GIB), ("MB", MIB), ("KB", KIB)):
        if num_bytes >= scale:
            value = num_bytes / scale
            return f"{value:.0f}{unit}" if value == int(value) else f"{value:.2f}{unit}"
    return f"{num_bytes}B"


class Model:
    """Stands in for a single-layer model whose weight is the payload to sync.

    The weight is shaped ``(num_views, row_elems)`` so that ``get_views()`` can
    hand back one contiguous row per view, all backed by a single storage. That
    is the layout RDT optimizes for: one memory registration for the base
    storage, with each view sent as an offset into it.

    ``row_elems`` is ``size_bytes // itemsize // num_views`` rounded down, so the
    realized payload can be a few hundred bytes under ``size_bytes`` when the
    view count does not divide it evenly. That error is negligible at the sizes
    these benchmarks use, and ``payload_bytes`` always reports the real number.
    """

    def __init__(
        self,
        size_bytes: int,
        num_views: int,
        device: str = "cuda",
        dtype: torch.dtype = torch.float16,
    ):
        row_elems = max(1, size_bytes // dtype.itemsize // num_views)
        self.weight = torch.zeros((num_views, row_elems), dtype=dtype, device=device)
        self.num_views = num_views

    @property
    def payload_bytes(self) -> int:
        """Total bytes actually covered by the views."""
        return self.weight.numel() * self.weight.element_size()

    def get_views(self) -> List[torch.Tensor]:
        """Return one contiguous view per row of the weight."""
        return [self.weight[i] for i in range(self.num_views)]

    def step(self) -> None:
        """Mutate the weight so each iteration transfers different data."""
        self.weight += 1


def _nixl_transport():
    """Return the process-local NIXL transport manager."""
    from ray.experimental.rdt.util import get_tensor_transport_manager

    return get_tensor_transport_manager("NIXL")


class _RDTActor:
    """Behavior shared by the trainer and generator actors."""

    def setup(
        self,
        size_bytes: int,
        num_views: int,
        pre_register: bool = False,
        pool_bytes: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build the model and optionally pre-register memory or a pool.

        ``pre_register`` and ``pool_bytes`` are mutually exclusive: the pool is
        only eligible for an object when none of its tensors already carry a
        NIXL registration, so doing both would silently disable the pool.
        """
        assert not (
            pre_register and pool_bytes
        ), "pre-registering weights disables the memory pool for those tensors"
        self.model = Model(size_bytes, num_views)
        self.views = self.model.get_views()
        self.pre_registered = pre_register

        if pool_bytes:
            from ray.experimental import register_nixl_memory_pool

            register_nixl_memory_pool(pool_bytes, torch.device("cuda"))
        if pre_register:
            from ray.experimental import register_nixl_memory

            register_nixl_memory(self.model.weight)

        # Force the NIXL agent to exist so its creation cost is not attributed
        # to the first measured transfer. The synchronize is a setup barrier, so
        # the model's allocation and fill cannot spill into a measured region;
        # no measured region contains a synchronize of its own.
        _nixl_transport().get_nixl_agent()
        torch.cuda.synchronize()
        return {
            "node_id": ray.get_runtime_context().get_node_id(),
            "backend": _nixl_transport()._backend,
            "payload_bytes": self.model.payload_bytes,
            "num_views": self.model.num_views,
            "free_gpu_bytes": torch.cuda.mem_get_info()[0],
        }

    def node_id(self) -> str:
        """Return the node this actor is running on."""
        return ray.get_runtime_context().get_node_id()

    def prepare_scratch(self) -> None:
        """Allocate a scratch buffer mirroring the weight layout.

        Timing raw per-view registration against the model weight itself is
        unsafe once the weight has been pre-registered, because the base storage
        registration would overlap the per-view ones. The scratch buffer gives
        the same layout with no existing registration.
        """
        self.scratch = torch.zeros_like(self.model.weight)
        self.scratch_views = [self.scratch[i] for i in range(self.model.num_views)]

    def time_raw_nixl_registration(self, use_scratch: bool = False) -> float:
        """Time registering every view individually, the pre-RDT practice.

        ``register_memory`` on a tensor registers exactly that view's extent, so
        N views cost N registrations. The descriptors are released after the
        timed region so repeated calls stay independent.
        """
        views = self.scratch_views if use_scratch else self.views
        agent = _nixl_transport().get_nixl_agent()
        descs = []
        start = time.perf_counter()
        for view in views:
            descs.append(agent.register_memory(view))
        elapsed = time.perf_counter() - start
        for desc in descs:
            agent.deregister_memory(desc)
        return elapsed

    def time_rdt_registration(self) -> float:
        """Time the single ``register_nixl_memory`` call that covers every view.

        RDT registers the whole underlying storage, so one call on any view
        registers all of them. Calling it per view would only bump refcounts,
        which is exactly the cost this API removes.
        """
        from ray.experimental import deregister_nixl_memory, register_nixl_memory

        start = time.perf_counter()
        register_nixl_memory(self.views[0])
        elapsed = time.perf_counter() - start
        deregister_nixl_memory(self.views[0])
        return elapsed


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class Trainer(_RDTActor):
    """Publishes weights into the RDT store, standing in for an RL trainer."""

    def put_views(self, advance: bool = True) -> "ray.ObjectRef":
        """Publish the current weights and return the resulting ObjectRef.

        The weight update is flushed before the timer starts so its kernel is not
        charged to ``ray.put``, which waits on outstanding device work.
        """
        if advance:
            self.model.step()
            torch.cuda.synchronize()
        start = time.perf_counter()
        ref = ray.put(self.views, _tensor_transport="nixl")
        self._last_put_seconds = time.perf_counter() - start
        return ref

    def last_put_seconds(self) -> float:
        """Seconds spent in the most recent ``ray.put``."""
        return self._last_put_seconds

    def num_xfer_descs(self, ref_hex: str) -> Optional[int]:
        """NIXL transfer descriptor count for a published object.

        One descriptor is one RDMA read on the receiver, so this is the number
        the memory pool is meant to shrink.
        """
        transport = _nixl_transport()
        meta = transport._get_meta(ref_hex)
        if meta is None or meta.nixl_serialized_descs is None:
            return None
        descs = transport.get_nixl_agent().deserialize_descs(meta.nixl_serialized_descs)
        return descs.descCount()

    def num_pool_blocks(self, ref_hex: str) -> Optional[int]:
        """Pool blocks backing an object, or None when the pool was not used."""
        pool = _nixl_transport()._memory_pool
        if pool is None:
            return None
        blocks = pool._allocated_by_obj.get(ref_hex)
        return None if blocks is None else len(blocks)

    def wait_pool_drained(self, timeout: float = 60.0) -> bool:
        """Block until every pool block has been reclaimed.

        Pool blocks are released when the owning ObjectRef goes out of scope,
        which happens asynchronously after the driver drops its reference. The
        benchmarks wait on this so a sized pool is not exhausted mid-sweep.
        """
        pool = _nixl_transport()._memory_pool
        if pool is None:
            return True
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not pool._allocated_by_obj:
                return True
            time.sleep(0.01)
        return False


@ray.remote(num_gpus=1, num_cpus=0, enable_tensor_transport=True)
class Generator(_RDTActor):
    """Pulls weights from the RDT store, standing in for an inference engine."""

    def sync_weights(
        self,
        refs: List["ray.ObjectRef"],
        use_target_buffers: bool,
        measure_peak_memory: bool = False,
        copy_into_weights: bool = True,
    ) -> Dict[str, Any]:
        """Fetch weights and report how long the fetch took.

        The timed region is nothing but ``ray.get``. RDT already synchronizes the
        transfer internally before returning, so no user-land
        ``torch.cuda.synchronize`` belongs inside the measurement, and the
        staging copy is done after the timer stops.

        Args:
            refs: A single-element list holding the ObjectRef. It must be a
                list, otherwise Ray dereferences the argument and performs the
                transfer before this method is entered.
            use_target_buffers: Receive straight into the local weights via
                ``set_target_for_ref`` instead of into fresh staging buffers.
            measure_peak_memory: Also report CUDA allocator peaks.
            copy_into_weights: Whether the staging path copies the received
                tensors into the local weights after the timed region. Ignored
                when receiving into target buffers, which needs no copy.
        """
        from ray.experimental import set_target_for_ref

        (ref,) = refs

        if measure_peak_memory:
            # Outside the timed region: flush pending work so empty_cache can
            # actually release blocks, giving a clean baseline to measure from.
            torch.cuda.synchronize()
            torch.cuda.empty_cache()
            torch.cuda.reset_peak_memory_stats()
        baseline_bytes = torch.cuda.memory_allocated()

        if use_target_buffers:
            set_target_for_ref(ref, self.views)

        start = time.perf_counter()
        received = ray.get(ref)
        elapsed = time.perf_counter() - start

        if not use_target_buffers and copy_into_weights:
            # The staging path needs a second copy into the model weights. It is
            # part of the workload but not part of the transfer time.
            for view, tensor in zip(self.views, received):
                view.copy_(tensor)

        result = {"seconds": elapsed}
        if measure_peak_memory:
            result.update(
                {
                    "baseline_bytes": baseline_bytes,
                    "peak_bytes": torch.cuda.max_memory_allocated(),
                    "peak_over_baseline_bytes": (
                        torch.cuda.max_memory_allocated() - baseline_bytes
                    ),
                    "peak_reserved_bytes": torch.cuda.max_memory_reserved(),
                }
            )
        del received
        return result


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add the arguments every RDT weight-sync benchmark accepts."""
    parser.add_argument(
        "--single-node",
        action="store_true",
        help="Place both actors on one node. For local verification only; "
        "cross-node is the default because that is what RDMA costs reflect.",
    )
    parser.add_argument(
        "--csv-out",
        default=None,
        help="Write results to this CSV path for plotting.",
    )


def _gpu_node_ids() -> List[str]:
    """Node IDs of every alive node that has at least one GPU."""
    return [
        node["NodeID"]
        for node in ray.nodes()
        if node.get("Alive") and node.get("Resources", {}).get("GPU", 0) >= 1
    ]


def make_actors(single_node: bool):
    """Create a trainer and generator, pinned per the placement policy.

    Returns the two actor handles. In the default cross-node mode this asserts
    that two GPU nodes exist and that the actors really did land on different
    ones, so a misconfigured cluster fails loudly instead of quietly measuring
    intra-node transfers.
    """
    gpu_nodes = _gpu_node_ids()
    if single_node:
        assert gpu_nodes, "no alive GPU nodes found"
        target = gpu_nodes[0]
        hints = [{"label_selector": {ray._raylet.RAY_NODE_ID_KEY: target}}] * 2
    else:
        assert len(gpu_nodes) >= 2, (
            f"cross-node run needs at least 2 GPU nodes, found {len(gpu_nodes)}. "
            "Pass --single-node to run locally."
        )
        local = ray.get_runtime_context().get_node_id()
        sender = local if local in gpu_nodes else gpu_nodes[0]
        receiver = next(node for node in gpu_nodes if node != sender)
        hints = [
            {"label_selector": {ray._raylet.RAY_NODE_ID_KEY: sender}},
            {"label_selector": {ray._raylet.RAY_NODE_ID_KEY: receiver}},
        ]

    trainer = Trainer.options(**hints[0]).remote()
    generator = Generator.options(**hints[1]).remote()

    trainer_node, generator_node = ray.get(
        [trainer.node_id.remote(), generator.node_id.remote()]
    )
    if single_node:
        assert trainer_node == generator_node, "expected both actors on one node"
        print(f"Placement: single node {trainer_node[:16]}")
    else:
        assert trainer_node != generator_node, (
            "actors landed on the same node despite cross-node placement: "
            f"{trainer_node}"
        )
        print(
            f"Placement: cross-node, trainer on {trainer_node[:16]}, "
            f"generator on {generator_node[:16]}"
        )
    return trainer, generator


def describe_setup(label: str, info: Dict[str, Any]) -> None:
    """Print what an actor's setup produced."""
    print(
        f"  {label}: backend={info['backend']} "
        f"payload={format_bytes(info['payload_bytes'])} "
        f"views={info['num_views']} "
        f"free_gpu={format_bytes(int(info['free_gpu_bytes']))}"
    )


def summarize(values: Sequence[float]) -> Dict[str, float]:
    """Median, min and max of a sample."""
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def print_table(rows: List[Dict[str, Any]], columns: Sequence[str]) -> None:
    """Print rows as a fixed-width table over the named columns."""
    if not rows:
        print("  (no results)")
        return

    def cell(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:.6f}"
        return "" if value is None else str(value)

    widths = {
        col: max(len(col), *(len(cell(row.get(col))) for row in rows))
        for col in columns
    }
    header = "  ".join(col.rjust(widths[col]) for col in columns)
    print(header)
    print("-" * len(header))
    for row in rows:
        print("  ".join(cell(row.get(col)).rjust(widths[col]) for col in columns))


def write_csv(path: Optional[str], rows: List[Dict[str, Any]]) -> None:
    """Write rows to CSV if a path was given."""
    if not path or not rows:
        return
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote CSV: {path}")


def write_perf_metrics(metrics: List[Dict[str, Any]]) -> None:
    """Emit release-test metrics when TEST_OUTPUT_JSON is set."""
    path = os.environ.get("TEST_OUTPUT_JSON")
    if not path:
        return
    with open(path, "w") as handle:
        json.dump({"perf_metrics": metrics}, handle)
    print(f"Wrote perf metrics: {path}")


def perf_metric(
    name: str, value: float, metric_type: str = "LATENCY"
) -> Dict[str, Any]:
    """Build one release-test perf metric entry."""
    return {
        "perf_metric_name": name,
        "perf_metric_value": value,
        "perf_metric_type": metric_type,
    }


def assert_gpu_headroom(info: Dict[str, Any], needed_bytes: int, label: str) -> None:
    """Fail early with a clear message if a config cannot fit on the GPU."""
    free = int(info["free_gpu_bytes"])
    if free < needed_bytes:
        raise RuntimeError(
            f"{label} needs about {format_bytes(needed_bytes)} of free GPU memory "
            f"but only {format_bytes(free)} is available after setup. "
            "Reduce --sizes or run on a larger GPU."
        )
