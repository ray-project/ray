import random
import sys
from typing import List

import pytest

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

        res.append(Resources(resource_dict))

    return res


class TestResources:
    @pytest.mark.parametrize("resource_type", ["CPU", "GPU", "memory"])
    def test_basic(self, resource_type: str):
        # basic resources
        a = Resources({resource_type: 1, "resources": {}})
        b = Resources({resource_type: 0})
        assert a.can_fit(b)
        assert not b.can_fit(a)

    def test_neither_bigger(self):
        a = Resources({"CPU": 1, "GPU": 0})
        b = Resources({"CPU": 0, "GPU": 1})
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
        a = Resources({"GPU": 1, "CPU": 10, "memory": 10, "resources": {"custom": 10}})
        b = Resources({"GPU": 2, "CPU": 0, "memory": 0, "resources": {"custom": 0}})
        assert b > a

        # Then CPU
        a = Resources({"GPU": 1, "CPU": 1, "memory": 10, "resources": {"custom": 10}})
        b = Resources({"GPU": 1, "CPU": 2, "memory": 0, "resources": {"custom": 0}})
        assert b > a

        # Then memory
        a = Resources({"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 10}})
        b = Resources({"GPU": 1, "CPU": 1, "memory": 2, "resources": {"custom": 0}})
        assert b > a

        # Then custom resources
        a = Resources({"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 1}})
        b = Resources({"GPU": 1, "CPU": 1, "memory": 1, "resources": {"custom": 2}})
        assert b > a

    def test_sort_resources(self):
        """Prioritize GPUs, CPUs, memory, then custom resources when sorting."""

        a = Resources({"GPU": 0, "CPU": 4, "memory": 99, "resources": {"A": 10}})
        b = Resources({"GPU": 0, "CPU": 2, "memory": 100})
        c = Resources({"GPU": 1, "CPU": 1, "memory": 50})
        d = Resources({"GPU": 2, "CPU": 0, "memory": 0})
        e = Resources({"GPU": 3, "CPU": 8, "memory": 10000, "resources": {"A": 6}})
        f = Resources({"GPU": 3, "CPU": 8, "memory": 10000, "resources": {"A": 2}})

        for _ in range(10):
            resources = [a, b, c, d, e, f]
            random.shuffle(resources)
            resources.sort(reverse=True)
            assert resources == [e, f, d, c, a, b]


def test_deployment_scheduling_info():
    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources({"CPU": 2, "GPU": 1}),
    )
    assert info.required_resources == Resources({"CPU": 2, "GPU": 1})
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[Resources({"CPU": 100}), Resources({"GPU": 100})],
        placement_group_strategy="STRICT_PACK",
    )
    assert info.required_resources == Resources({"CPU": 100, "GPU": 100})
    assert not info.is_non_strict_pack_pg()

    info = DeploymentSchedulingInfo(
        scheduling_policy=SpreadDeploymentSchedulingPolicy,
        actor_resources=Resources({"CPU": 2, "GPU": 1}),
        placement_group_bundles=[Resources({"CPU": 100}), Resources({"GPU": 100})],
        placement_group_strategy="PACK",
    )
    assert info.required_resources == Resources({"CPU": 2, "GPU": 1})
    assert info.is_non_strict_pack_pg()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
