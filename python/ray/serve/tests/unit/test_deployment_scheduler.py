import random
import sys
from typing import List

import pytest

import ray
from ray.serve._private.deployment_scheduler import (
    DeploymentSchedulingInfo,
    Resources,
    SpreadDeploymentSchedulingPolicy,
)
from ray.tests.conftest import *  # noqa


def get_random_resources(n: int) -> List[Resources]:
    """Gets n random resources."""

    resources = {
        "CPU": lambda: random.randint(0, 10),
        "GPU": lambda: random.randint(0, 10),
        "memory": lambda: random.randint(0, 10),
        "resources": lambda: {"custom_A": random.randint(0, 10)},
    }

    res = list()
    for _ in range(n):
        resource_dict = dict()
        for resource, callable in resources.items():
            if random.randint(0, 1) == 0:
                resource_dict[resource] = callable()

        res.append(Resources.from_ray_resource_dict(resource_dict))

    return res


class TestResources:
    @pytest.mark.parametrize("resource_type", ["CPU", "GPU", "memory"])
    def test_basic(self, resource_type: str):
        # basic resources
        a = Resources.from_ray_resource_dict({resource_type: 1, "resources": {}})
        b = Resources.from_ray_resource_dict({resource_type: 0})
        assert a.can_fit(b)
        assert not b.can_fit(a)

    def test_neither_bigger(self):
        a = Resources.from_ray_resource_dict({"CPU": 1, "GPU": 0})
        b = Resources.from_ray_resource_dict({"CPU": 0, "GPU": 1})
        assert not a == b
        assert not a.can_fit(b)
        assert not b.can_fit(a)

    combos = [tuple(get_random_resources(20)[i : i + 2]) for i in range(0, 20, 2)]

    @pytest.mark.parametrize("resource_A,resource_B", combos)
    def test_soft_resources_consistent_comparison(self, resource_A, resource_B):
        """Resources should have consistent comparison. Either A==B, A<B, or A>B."""

        assert (
            resource_A == resource_B
            or resource_A > resource_B
            or resource_A < resource_B
        )

    def test_compare_resources(self):
        # Prioritize GPU
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 10, "memory": 10, "resources": {"custom": 10}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 2, "CPU": 0, "memory": 0, "resources": {"custom": 0}}
        )
        assert b > a

        # Then CPU
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 10, "resources": {"custom": 10}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 2, "memory": 0, "resources": {"custom": 0}}
        )
        assert b > a

        # Then memory
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 10}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 2, "resources": {"custom": 0}}
        )
        assert b > a

        # Then custom resources
        a = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 1}}
        )
        b = Resources.from_ray_resource_dict(
            {"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 2}}
        )
        assert b > a

    def test_sort_resources(self):
        """Prioritize GPUs, CPUs, memory, then custom resources when sorting."""

        a = Resources.from_ray_resource_dict(
            {"GPU": 0, "CPU": 4, "memory": 99, "resources": {"A": 10}}
        )
        b = Resources.from_ray_resource_dict({"GPU": 0, "CPU": 2, "memory": 100})
        c = Resources.from_ray_resource_dict({"GPU": 1, "CPU": 1, "memory": 50})
        d = Resources.from_ray_resource_dict({"GPU": 2, "CPU": 0, "memory": 0})
        e = Resources.from_ray_resource_dict(
            {"GPU": 3, "CPU": 8, "memory": 10000, "resources": {"A": 6}}
        )
        f = Resources.from_ray_resource_dict(
            {"GPU": 3, "CPU": 8, "memory": 10000, "resources": {"A": 2}}
        )

        for _ in range(10):
            resources = [a, b, c, d, e, f]
            random.shuffle(resources)
            resources.sort(reverse=True)
            assert resources == [e, f, d, c, a, b]

    def test_custom_resources(self):
        a = Resources.from_ray_resource_dict({"resources": {"alice": 2}})
        b = Resources.from_ray_resource_dict({"resources": {"alice": 3}})
        assert a < b
        assert b.can_fit(a)
        assert a + b == Resources(**{"alice": 5})

        a = Resources.from_ray_resource_dict({"resources": {"bob": 2}})
        b = Resources.from_ray_resource_dict({"CPU": 4})
        assert a + b == Resources(**{"CPU": 4, "bob": 2})

    def test_implicit_resources(self):
        r = Resources()
        # Implicit resources
        assert r.get(f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}random") == 1
        # Everything else
        assert r.get("CPU") == 0
        assert r.get("GPU") == 0
        assert r.get("memory") == 0
        assert r.get("random_custom") == 0

        # Arithmetric with implicit resources
        implicit_resource = f"{ray._raylet.IMPLICIT_RESOURCE_PREFIX}whatever"
        a = Resources()
        b = Resources.from_ray_resource_dict({"resources": {implicit_resource: 0.5}})
        assert a.get(implicit_resource) == 1
        assert b.get(implicit_resource) == 0.5
        assert a.can_fit(b)

        a -= b
        assert a.get(implicit_resource) == 0.5
        assert a.can_fit(b)

        a -= b
        assert a.get(implicit_resource) == 0
        assert not a.can_fit(b)


def test_deployment_scheduling_info():
    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources.from_ray_resource_dict({"CPU": 2, "GPU": 1}),
    )
    assert info.required_resources == Resources.from_ray_resource_dict(
        {"CPU": 2, "GPU": 1}
    )
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources.from_ray_resource_dict({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[
            Resources.from_ray_resource_dict({"CPU": 100}),
            Resources.from_ray_resource_dict({"GPU": 100}),
        ],
        placement_group_strategy="STRICT_PACK",
    )
    assert info.required_resources == Resources.from_ray_resource_dict(
        {"CPU": 100, "GPU": 100}
    )
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources.from_ray_resource_dict({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[
            Resources.from_ray_resource_dict({"CPU": 100}),
            Resources.from_ray_resource_dict({"GPU": 100}),
        ],
        placement_group_strategy="PACK",
    )
    assert info.required_resources == Resources.from_ray_resource_dict(
        {"CPU": 2, "GPU": 1}
    )
    assert info.is_non_strict_pack_pg()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
