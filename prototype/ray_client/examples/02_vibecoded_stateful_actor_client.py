"""Example 2: Stateful actor session over HTTP (`cluster.actor` & `ActorHandle`).

Deploys a Python class as a detached root actor on `/api/actors`,
invokes methods across multiple steps, reconnects by name, and inspects logs.
"""

import sys
from typing import Any, Optional

from ray_client import RayCluster


class Counter:
    def __init__(self, start_val: int = 0):
        self.val = start_val

    def add(self, delta: int) -> int:
        self.val += delta
        print(f"counter updated to {self.val}")
        return self.val

    def snapshot(self, tag: str) -> dict:
        return {"tag": tag, "value": self.val}

    def get(self) -> int:
        return self.val


class LiveCounterClient:
    def __init__(
        self,
        address: Optional[str] = None,
        actor_name: str = "shared_counter",
    ):
        self.cluster = RayCluster(address=address)
        self.actor_name = actor_name
        self.actor = None

    def start(self, initial: int = 0) -> dict:
        self.actor = self.cluster.actor(
            Counter,
            initial,
            name=self.actor_name,
            get_if_exists=False,
        )
        return {"actor_name": self.actor.name, "value": self.actor.get()}

    def add(self, delta: int) -> int:
        assert self.actor is not None
        return self.actor.add(delta)

    def snapshot(self, tag: str) -> dict:
        reconnected = self.cluster.get_actor(self.actor_name)
        return reconnected.snapshot(tag)

    def logs(self) -> str:
        assert self.actor is not None
        return self.actor.logs()

    def stop(self) -> dict:
        assert self.actor is not None
        self.actor.kill()
        self.cluster.close()
        return {"stopped": self.actor_name}


def run_demo(address: Optional[str] = None) -> Any:
    counter = LiveCounterClient(address=address)
    counter.start(initial=10)
    v1 = counter.add(5)
    v2 = counter.add(27)
    snap = counter.snapshot("final")
    assert snap == {"tag": "final", "value": 42}
    assert "counter updated to 42" in counter.logs()
    counter.stop()
    return {"v1": v1, "v2": v2}


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Actor client output:", run_demo(cluster_addr))
