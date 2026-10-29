"""Real manifest lineage recovery, isolated from the shared ORC test cluster."""

from typing import cast

import pyarrow as pa
import pytest

import ray
from ray import ObjectRefGenerator
from ray.actor import ActorClass, ActorHandle
from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
    OrcDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.tests.datasource.orc_decode_trace import (
    OrcTrace,
    write_stripe_pipeline_files,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@pytest.mark.timeout(180)
def test_orc_prelisted_manifest_lineage_reconstruction(
    ray_start_cluster, tmp_path, monkeypatch
):
    paths = write_stripe_pipeline_files(tmp_path)
    monkeypatch.setenv("RAY_DATA_ORC_BIN_PACKING_BYTES", "4096")
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", False
    )
    source = OrcDatasourceV2([str(path) for path in paths])
    indexer = source._get_file_indexer()
    sample = sample_files(indexer, source.paths, source.filesystem)
    schema, indexer, partitioner = source.prepare_read(
        sample, indexer, PartitionHints(0, 4096, 4)
    )
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
    worker = cluster.add_node(
        num_cpus=4, object_store_memory=256 * 1024**2, resources={"orc_worker": 1}
    )
    cluster.wait_for_nodes()
    trace = cast(
        "ActorHandle[OrcTrace]",
        cast("ActorClass[OrcTrace]", OrcTrace)
        .options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            )
        )
        .remote(),
    )
    infos = [FileInfo(str(path), path.stat().st_size) for path in paths]

    @ray.remote(
        num_returns="streaming",
        num_cpus=1,
        resources={"orc_worker": 0.1},
        max_retries=3,
    )
    def listing(indexer, partitioner, infos, trace):
        ray.get(trace.begin.remote())
        for manifest in indexer.list_files_from_file_infos(
            infos,
            filesystem=source.filesystem,
            shuffle_config=FileShuffleConfig(seed=66999),
            execution_idx=2,
        ):
            partitioner.add_input(manifest)
            while partitioner.has_partition():
                yield partitioner.next_partition().as_block()
        partitioner.finalize()
        while partitioner.has_partition():
            yield partitioner.next_partition().as_block()

    @ray.remote(resources={"orc_worker": 0.1})
    def consume(refs):
        # Fetch only on the worker that will be lost, keeping the head from
        # retaining a plasma copy of the original manifest objects.
        return [cast(pa.Table, ray.get(ref)).to_pylist() for ref in refs]

    try:
        generator = cast(
            ObjectRefGenerator, listing.remote(indexer, partitioner, infos, trace)
        )
        refs = list(generator)
        expected = ray.get(consume.remote(refs), timeout=60)
        from ray.experimental import get_object_locations

        head = ray.get_runtime_context().get_node_id()
        locations = get_object_locations(refs)
        assert all(
            head not in location["node_ids"] and location["node_ids"]
            for location in locations.values()
        )
        cluster.remove_node(worker, allow_graceful=False)
        cluster.add_node(
            num_cpus=4, object_store_memory=256 * 1024**2, resources={"orc_worker": 1}
        )
        cluster.wait_for_nodes()
        actual = [cast(pa.Table, ray.get(ref, timeout=60)).to_pylist() for ref in refs]
        assert actual == expected
        # The counter covers the original producer and actual reconstruction.
        assert cast(int, ray.get(trace.begin.remote())) >= 3
        rows = []
        for group in actual:
            manifest = FileManifest(pa.Table.from_pylist(group))
            rows.extend(
                row["id"]
                for table in OrcScanner(schema=schema).create_reader().read(manifest)
                for row in table.to_pylist()
            )
        assert sorted(rows) == list(range(320))
    finally:
        ray.kill(trace)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
