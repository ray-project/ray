import sys

import pytest

from ray._raylet import IMPLICIT_RESOURCE_PREFIX
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.domain_probe import (
    DOMAIN_PROBE_NAME_PREFIX,
    probe_actor_options,
)


def test_probe_asks_for_the_replicas_resources_and_selector():
    d_id = DeploymentID(name="Ranker", app_name="search")
    options = probe_actor_options(
        d_id,
        {
            "CPU": 2,
            "GPU": 1,
            "memory": 1024,
            "accelerator": 1,
            f"{IMPLICIT_RESOURCE_PREFIX}search:Ranker": 0.5,
        },
        {"ray.io/availability-zone": "exists(),!in(a)"},
    )

    assert options["name"].startswith(f"{DOMAIN_PROBE_NAME_PREFIX}search#Ranker#")
    assert options["namespace"] == SERVE_NAMESPACE
    assert options["num_cpus"] == 2
    assert options["num_gpus"] == 1
    assert options["memory"] == 1024
    assert options["resources"] == {"accelerator": 1}
    assert options["label_selector"] == {"ray.io/availability-zone": "exists(),!in(a)"}


def test_probe_without_custom_resources():
    options = probe_actor_options(DeploymentID(name="d"), {"CPU": 1}, {})
    assert options["num_gpus"] == 0
    assert "memory" not in options
    assert "resources" not in options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
