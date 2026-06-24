import argparse
import dataclasses
import os
import time

import ray


def install_metadata_timer():
    import ray.data._internal.metadata_exporter as metadata_exporter

    original_create = metadata_exporter.Topology.create_topology_metadata
    original_sanitize = metadata_exporter.sanitize_for_struct

    stats = {"sanitize_calls": 0, "dataclass_calls": 0}

    def timed_sanitize(obj, truncate_length=metadata_exporter.DEFAULT_TRUNCATION_LENGTH):
        stats["sanitize_calls"] += 1
        if dataclasses.is_dataclass(obj):
            stats["dataclass_calls"] += 1
        return original_sanitize(obj, truncate_length)

    def timed_create(dag, op_to_id):
        start = time.perf_counter()
        try:
            return original_create(dag, op_to_id)
        finally:
            elapsed = time.perf_counter() - start
            print(
                "[metadata_timer]",
                f"create_topology_metadata_s={elapsed:.6f}",
                f"sanitize_calls={stats['sanitize_calls']}",
                f"dataclass_calls={stats['dataclass_calls']}",
                flush=True,
            )

    metadata_exporter.sanitize_for_struct = timed_sanitize
    metadata_exporter.Topology.create_topology_metadata = staticmethod(timed_create)


def make_fn(i):
    def fn(batch):
        return batch

    fn.__name__ = f"Stage{i}Task"
    fn.__qualname__ = fn.__name__
    return fn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-stages", type=int, default=30)
    args = parser.parse_args()

    install_metadata_timer()

    print({"ray_version": ray.__version__, "num_stages": args.num_stages}, flush=True)

    ray.init(num_cpus=4, include_dashboard=False)

    ds = ray.data.from_items([{"item": 0}], override_num_blocks=1)
    for i in range(args.num_stages):
        ds = ds.map_batches(make_fn(i), batch_size=1)

    print(ds.take_all())


if __name__ == "__main__":
    main()
