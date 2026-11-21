"""Probes that ask the autoscaler for a node in a domain a floor lacks."""

import uuid
from typing import Any, Dict, Optional

import ray
from ray._raylet import IMPLICIT_RESOURCE_PREFIX  # type: ignore[attr-defined]
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.utils import check_obj_ref_ready_nowait

DOMAIN_PROBE_NAME_PREFIX = "SERVE_DOMAIN_PROBE::"


@ray.remote(num_cpus=0, max_restarts=0)
class _DomainProbeActor:
    def node_id(self) -> str:
        return ray.get_runtime_context().get_node_id()


def probe_actor_options(
    deployment_id: DeploymentID,
    resources: Dict[str, float],
    label_selector: Dict[str, str],
) -> Dict[str, Any]:
    """Actor options for a probe with one replica's resources."""
    options: Dict[str, Any] = {
        "name": (
            f"{DOMAIN_PROBE_NAME_PREFIX}{deployment_id.app_name}#"
            f"{deployment_id.name}#{uuid.uuid4().hex[:6]}"
        ),
        "namespace": SERVE_NAMESPACE,
        "num_cpus": resources.get("CPU", 0),
        "num_gpus": resources.get("GPU", 0),
        "label_selector": label_selector,
    }
    if resources.get("memory"):
        options["memory"] = resources["memory"]
    custom = {
        key: value
        for key, value in resources.items()
        if key not in ("CPU", "GPU", "memory")
        and not key.startswith(IMPLICIT_RESOURCE_PREFIX)
        and value
    }
    if custom:
        options["resources"] = custom
    return options


class DomainProbe:
    """One pending probe."""

    def __init__(
        self,
        deployment_id: DeploymentID,
        resources: Dict[str, float],
        label_selector: Dict[str, str],
    ):
        self._actor = _DomainProbeActor.options(  # type: ignore[attr-defined]
            **probe_actor_options(deployment_id, resources, label_selector)
        ).remote()
        self._node_ref = self._actor.node_id.remote()

    def landed_node(self) -> Optional[str]:
        """The probe's node once it is placed, without blocking."""
        if not check_obj_ref_ready_nowait(self._node_ref):
            return None
        try:
            return str(ray.get(self._node_ref))
        except Exception:
            return None

    def kill(self) -> None:
        try:
            ray.kill(self._actor, no_restart=True)
        except Exception:
            pass
