import sys
import os

import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak


def test_bundle_label_selector_repeated_labels(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 2
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=4, labels={"ray.io/accelerator-type": "A100"})
    ray.init(address=cluster.address)

    bundles = [{"CPU": 1}, {"CPU": 1}]
    label_selector = [{"ray.io/accelerator-type": "A100"}] * 2

    placement_group = ray.util.placement_group(
        name="repeated_labels_pg",
        strategy="PACK",
        bundles=bundles,
        bundle_label_selector=label_selector,
    )
    ray.get(placement_group.ready())

    placement_group_assert_no_leak([placement_group])


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
