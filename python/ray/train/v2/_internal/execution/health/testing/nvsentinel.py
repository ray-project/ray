"""Wait on NVSentinel's response to an injected fault.

The injection half lives in :mod:`~.dcgm`; this is the other end. After
``dcgmi test --inject`` the event has to travel health monitor -> platform
connector -> datastore -> fault quarantine -> Kubernetes, so a test has to wait
for the node to actually change rather than assert immediately.
"""
import json
import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class NodeSnapshot:
    name: str
    cordoned: bool
    conditions: Dict[str, str]
    taints: List[str]

    @property
    def quarantined(self) -> bool:
        return self.cordoned or bool(self.taints)

    @property
    def firing_conditions(self) -> List[str]:
        """Conditions NVSentinel set, excluding Kubernetes' own."""
        builtin = {
            "Ready",
            "MemoryPressure",
            "DiskPressure",
            "PIDPressure",
            "NetworkUnavailable",
        }
        return sorted(
            name
            for name, status in self.conditions.items()
            if status == "True" and name not in builtin
        )


def read_node(name: str) -> NodeSnapshot:
    """One node's quarantine-relevant state, straight from the Kubernetes API."""
    out = subprocess.run(
        ["kubectl", "get", "node", name, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if out.returncode != 0:
        raise RuntimeError(f"kubectl get node {name} failed: {out.stderr.strip()}")
    node = json.loads(out.stdout)
    return NodeSnapshot(
        name=name,
        cordoned=bool(node["spec"].get("unschedulable")),
        conditions={
            c["type"]: str(c["status"]) for c in node["status"].get("conditions", [])
        },
        taints=[t["key"] for t in node["spec"].get("taints", [])],
    )


def wait_for_quarantine(
    node: str, timeout_s: float = 180.0, poll_s: float = 2.0
) -> NodeSnapshot:
    """Block until NVSentinel cordons or taints ``node``.

    The wait is the measurement: how long the pipeline takes from injected
    counter to a signal Ray Train can read is the number worth recording in a
    release test, alongside how long Ray Train then takes to act on it.
    """
    deadline = time.monotonic() + timeout_s
    last: Optional[NodeSnapshot] = None
    started = time.monotonic()
    while time.monotonic() < deadline:
        last = read_node(node)
        if last.quarantined:
            logger.warning(
                "[fault-injection] NVSentinel quarantined %s after %.1fs "
                "(conditions=%s, taints=%s)",
                node,
                time.monotonic() - started,
                last.firing_conditions,
                last.taints,
            )
            return last
        time.sleep(poll_s)
    raise TimeoutError(
        f"{node} was not quarantined within {timeout_s:.0f}s " f"(last seen: {last})"
    )
