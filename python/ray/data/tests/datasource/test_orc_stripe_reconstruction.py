"""Rebuild an actual lost manifest without retaining a head replica."""

from typing import cast

import pyarrow as pa
import pytest

import ray
from ray import ObjectRefGenerator
from ray.actor import ActorClass, ActorHandle
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.tests.datasource.orc_decode_trace import (
    OrcTrace,
    partitioned,
    prepare_orc,
    write_stripe_pipeline_files,
)
from ray.experimental import get_object_locations
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@pytest.mark.timeout(180)
def test_orc_prelisted_manifest_lineage_reconstruction(
    ray_start_cluster, tmp_path, monkeypatch
):
    monkeypatch.setenv("RAY_DATA_ORC_BIN_PACKING_BYTES", "4096")
    paths = write_stripe_pipeline_files(tmp_path)
    source, _, indexer, packer = prepare_orc(paths)
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=0,
        object_store_memory=128 * 1024**2,
        enable_object_reconstruction=True,
        _system_config={
            "max_direct_call_object_size": 100,
            "task_retry_delay_ms": 100,
            "object_timeout_milliseconds": 200,
            "health_check_period_ms": 100,
            "health_check_failure_threshold": 10,
            "health_check_initial_delay_ms": 0,
        },
    )
    ray.init(address=cluster.address)
    node_options = dict(
        num_cpus=4, object_store_memory=256 * 1024**2, resources={"orc_worker": 1}
    )
    worker = cluster.add_node(**node_options)
    cluster.wait_for_nodes()
    head = ray.get_runtime_context().get_node_id()
    trace = cast(
        ActorHandle[OrcTrace],
        cast(ActorClass[OrcTrace], OrcTrace)
        .options(scheduling_strategy=NodeAffinitySchedulingStrategy(head, soft=False))
        .remote(),
    )
    infos = [FileInfo(str(p), p.stat().st_size) for p in paths]

    @ray.remote(
        num_returns="streaming",
        num_cpus=1,
        resources={"orc_worker": 0.1},
        max_retries=3,
    )
    def listing(indexer, packer):
        ray.get(trace.count.remote("attempt"))
        yield from partitioned(
            indexer.list_files_from_file_infos(
                infos,
                filesystem=source.filesystem,
                shuffle_config=FileShuffleConfig(seed=66999),
                execution_idx=2,
            ),
            packer,
        )

    @ray.remote(resources={"orc_worker": 0.1})
    def consume(refs):
        return [cast(pa.Table, ray.get(ref)).to_pylist() for ref in refs]

    try:
        refs = list(cast(ObjectRefGenerator, listing.remote(indexer, packer)))
        expected = ray.get(consume.remote(refs), timeout=60)
        assert all(
            loc["node_ids"] and head not in loc["node_ids"]
            for loc in get_object_locations(refs).values()
        )
        cluster.remove_node(worker, allow_graceful=False)
        cluster.add_node(**node_options)
        cluster.wait_for_nodes()
        assert [
            cast(pa.Table, ray.get(ref, timeout=60)).to_pylist() for ref in refs
        ] == expected
        assert cast(int, ray.get(trace.count.remote("attempt"))) >= 3
    finally:
        ray.kill(trace)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-q", __file__]))
