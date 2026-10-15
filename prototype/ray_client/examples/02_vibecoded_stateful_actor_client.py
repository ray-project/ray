"""Example 2: Stateful detached actor client built on `RayClient`.

Shows how users can wrap `RayClient` in ~30 lines to create, invoke, and tear
down a named detached actor across separate `run_function_as_driver` calls.
"""

import sys
from typing import Any, Optional

from ray_client import RayClient


class DetachedCounterClient:
    """Thin custom client managing a named detached Ray actor."""

    def __init__(
        self, address: Optional[str] = None, actor_name: str = "shared_counter"
    ):
        self.client = RayClient(address=address)
        self.actor_name = actor_name

    def start(self, initial: int = 0) -> dict:
        def _start_driver(name: str, init_val: int) -> dict:
            import ray

            @ray.remote
            class Counter:
                def __init__(self, start_val: int):
                    self.val = start_val

                def add(self, delta: int) -> int:
                    self.val += delta
                    return self.val

                def get(self) -> int:
                    return self.val

            try:
                existing = ray.get_actor(name)
                ray.kill(existing)
            except ValueError:
                pass
            actor = Counter.options(name=name, lifetime="detached").remote(init_val)
            return {"actor_name": name, "value": ray.get(actor.get.remote())}

        return self.client.run_function_as_driver(
            _start_driver, self.actor_name, initial
        ).result()

    def add(self, delta: int) -> int:
        def _add_driver(name: str, step: int) -> int:
            import ray

            actor = ray.get_actor(name)
            return ray.get(actor.add.remote(step))

        return self.client.run_function_as_driver(
            _add_driver, self.actor_name, delta
        ).result()

    def stop(self) -> dict:
        def _stop_driver(name: str) -> dict:
            import ray

            actor = ray.get_actor(name)
            ray.kill(actor)
            return {"stopped": name}

        return self.client.run_function_as_driver(
            _stop_driver, self.actor_name
        ).result()


def run_demo(address: Optional[str] = None) -> Any:
    counter = DetachedCounterClient(address=address)
    counter.start(initial=10)
    v1 = counter.add(5)
    v2 = counter.add(27)
    counter.stop()
    return {"v1": v1, "v2": v2}


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Actor client output:", run_demo(cluster_addr))
