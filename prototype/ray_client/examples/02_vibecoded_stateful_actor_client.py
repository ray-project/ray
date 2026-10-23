"""Example 2: Live stateful actor client over HTTP (`client.actor` & `RemoteRef`).

Demonstrates creating and invoking a stateful Ray actor directly over the
`/api/workspaces/{ws}/actors` REST endpoints without spawning a new driver job
per method call, as well as chaining calls on in-cluster objects via
`return_ref=True` (`{"$ray_ref": "..."}`).
"""

import sys
from typing import Any, Optional

from ray_client import RayClient


class Counter:
    def __init__(self, start_val: int = 0):
        self.val = start_val

    def add(self, delta: int) -> int:
        self.val += delta
        return self.val

    def snapshot(self, tag: str) -> dict:
        return {"tag": tag, "value": self.val}

    def format_snapshot(self, snap: dict) -> str:
        return f"{snap['tag']}={snap['value']}"

    def get(self) -> int:
        return self.val


class LiveCounterClient:
    """Thin client managing a named live actor in a cluster workspace over HTTP."""

    def __init__(
        self,
        address: Optional[str] = None,
        actor_name: str = "shared_counter",
        workspace: str = "counter_demo",
    ):
        self.client = RayClient(address=address, workspace=workspace)
        self.actor_name = actor_name
        self.actor = None

    def start(self, initial: int = 0) -> dict:
        self.actor = self.client.actor(
            Counter,
            initial,
            name=self.actor_name,
            get_if_exists=False,
        )
        return {"actor_name": self.actor.name, "value": self.actor.get()}

    def add(self, delta: int) -> int:
        assert self.actor is not None
        return self.actor.add(delta)

    def snapshot_and_format_in_cluster(self, tag: str) -> str:
        """Keeps intermediate snapshot dict in-cluster via a `$ray_ref` token."""
        assert self.actor is not None
        snap_ref = self.actor.snapshot.ref(tag)
        return self.actor.format_snapshot(snap_ref)

    def stop(self) -> dict:
        assert self.actor is not None
        self.actor.kill()
        self.client.close_workspace()
        return {"stopped": self.actor_name}


def run_demo(address: Optional[str] = None) -> Any:
    counter = LiveCounterClient(address=address)
    counter.start(initial=10)
    v1 = counter.add(5)
    v2 = counter.add(27)
    formatted = counter.snapshot_and_format_in_cluster("final")
    assert formatted == "final=42"
    counter.stop()
    return {"v1": v1, "v2": v2}


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Actor client output:", run_demo(cluster_addr))
