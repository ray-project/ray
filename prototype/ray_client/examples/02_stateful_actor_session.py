"""Example 2: Stateful actor session over HTTP (`cluster.actor` & `ActorHandle`).

Deploys a Python class as a detached root actor on `/api/actors`,
invokes methods across multiple steps, reconnects by name, inspects
actor status and logs, and runs a `close()` teardown hook on context-manager exit.
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

    def close(self) -> None:
        print(f"closing counter at {self.val}")


def run_demo(address: Optional[str] = None) -> Any:
    with RayCluster(address=address) as cluster:
        with cluster.actor(
            Counter,
            10,
            name="shared_counter",
            get_if_exists="recreate",
        ) as counter:
            assert counter.created is True
            info = counter.info()
            assert info["status"] == "ALIVE"

            v1 = counter.add(5)
            v2 = counter.add(27)

            # Reconnect to the same named actor from another handle/client.
            reconnected = cluster.get_actor("shared_counter")
            assert reconnected.created is False
            snap = reconnected.snapshot(tag="final")
            assert snap == {"tag": "final", "value": 42}
            assert "counter updated to 42" in counter.logs()

        active_names = [a["name"] for a in cluster.list_actors()]
        assert "shared_counter" not in active_names

    return {"v1": v1, "v2": v2}


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Actor client output:", run_demo(cluster_addr))
