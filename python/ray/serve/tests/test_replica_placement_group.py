import asyncio
import os
import sys
from typing import Dict, Optional

from fastapi import FastAPI
import requests
import pytest

import ray
from ray.util.placement_group import (
    get_current_placement_group,
    PlacementGroup,
)

from ray import serve
from ray.serve._private.utils import get_all_live_placement_group_names


def test_basic(serve_instance):
    """Test the basic workflow."""
    @serve.deployment(
        num_replicas=2,
        placement_group_bundles=[{"CPU": 1}, {"CPU": 0.1}],
    )
    class D:
        def get_pg(self) -> PlacementGroup:
            return get_current_placement_group()

    h = serve.run(D.bind(), name="pg_test")

    # Verify that each replica has its own placement group with the correct bundles.
    assert len(get_all_live_placement_group_names()) == 2
    unique_pgs = set(ray.get([h.get_pg.remote() for _ in range(20)]))
    assert len(unique_pgs) == 2
    for pg in unique_pgs:
        assert pg.bundle_specs == [{"CPU": 1}, {"CPU": 0.1}]

    # Verify that all placement groups are deleted when the deployment is deleted.
    serve.delete("pg_test")
    assert len(get_all_live_placement_group_names()) == 0


@pytest.mark.skip(reason="Not handled gracefully.")
def test_replica_actor_infeasible(serve_instance):
    """Test the case where the replica actor doesn't fit in the first bundle."""
    @serve.deployment(
        placement_group_bundles=[{"CPU": 0.1}],
    )
    class Infeasible:
        pass

    with pytest.raises(RuntimeError):
        serve.run(Infeasible.bind())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
