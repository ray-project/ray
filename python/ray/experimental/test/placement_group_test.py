import pytest

import ray
from ray.experimental import PlacementStrategy
import ray.tests.conftest
from ray.tests.conftest import ray_start_10_cpus


def test_placement_group(ray_start_10_cpus):
    group = ray.experimental.placement_group("test_group")\
        .set_strategy(PlacementStrategy.SPREAD)\
        .add_bundle({"CPU": 2.0}).add_bundle({"CPU": 1.0}).create()

    from ray.experimental.placement_group import global_pg_table
    assert global_pg_table[group.get_id().hex()]["Bundles"][0]["Id"] == group.get_bundles()[0].get_id()
    assert global_pg_table[group.get_id().hex()]["Bundles"][1]["Id"] == group.get_bundles()[1].get_id()
    assert ray.nodes()[0]["NodeID"] == global_pg_table[group.get_id().hex()]["Bundles"][0]["Units"][0]["Label"]
    assert ray.nodes()[0]["NodeID"] == global_pg_table[group.get_id().hex()]["Bundles"][1]["Units"][0]["Label"]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
