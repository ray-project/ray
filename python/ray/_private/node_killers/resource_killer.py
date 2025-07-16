import asyncio
import logging
import random
import time
from typing import Callable, Optional

import ray
from ray._common.utils import get_or_create_event_loop
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class ResourceKillerActor:
    """Abstract base class used to implement resource killers for chaos testing.

    Subclasses should implement _find_resource_to_kill, which should find a resource
    to kill. This method should return the args to _kill_resource, which is another
    abstract method that should kill the resource and add it to the `killed` set.
    """

    def __init__(
        self,
        head_node_id,
        kill_interval_s: float = 60,
        kill_delay_s: float = 0,
        max_to_kill: int = 2,
        batch_size_to_kill: int = 1,
        kill_filter_fn: Optional[Callable] = None,
    ):
        self.kill_interval_s = kill_interval_s
        self.kill_delay_s = kill_delay_s
        self.is_running = False
        self.head_node_id = head_node_id
        self.killed = set()
        self.done = get_or_create_event_loop().create_future()
        self.max_to_kill = max_to_kill
        self.batch_size_to_kill = batch_size_to_kill
        self.kill_filter_fn = kill_filter_fn
        self.kill_immediately_after_found = False
        # -- logger. --
        logging.basicConfig(level=logging.INFO)

    def ready(self):
        pass

    async def run(self):
        self.is_running = True

        time.sleep(self.kill_delay_s)

        while self.is_running:
            to_kills = await self._find_resources_to_kill()

            if not self.is_running:
                break

            if self.kill_immediately_after_found:
                sleep_interval = 0
            else:
                sleep_interval = random.random() * self.kill_interval_s
                time.sleep(sleep_interval)

            for to_kill in to_kills:
                self._kill_resource(*to_kill)
            if len(self.killed) >= self.max_to_kill:
                break
            await asyncio.sleep(self.kill_interval_s - sleep_interval)

        self.done.set_result(True)
        await self.stop_run()

    async def _find_resources_to_kill(self):
        raise NotImplementedError

    def _kill_resource(self, *args):
        raise NotImplementedError

    async def stop_run(self):
        was_running = self.is_running
        if was_running:
            self._cleanup()

        self.is_running = False
        return was_running

    async def get_total_killed(self):
        """Get the total number of killed resources"""
        await self.done
        return self.killed

    def _cleanup(self):
        """Cleanup any resources created by the killer.

        Overriding this method is optional.
        """
        pass


def get_and_run_resource_killer(
    resource_killer_cls,
    kill_interval_s,
    namespace=None,
    lifetime=None,
    no_start=False,
    max_to_kill=2,
    batch_size_to_kill=1,
    kill_delay_s=0,
    kill_filter_fn=None,
):
    assert ray.is_initialized(), "The API is only available when Ray is initialized."

    head_node_id = ray.get_runtime_context().get_node_id()
    # Schedule the actor on the current node.
    resource_killer = resource_killer_cls.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False
        ),
        namespace=namespace,
        name="ResourceKiller",
        lifetime=lifetime,
    ).remote(
        head_node_id,
        kill_interval_s=kill_interval_s,
        kill_delay_s=kill_delay_s,
        max_to_kill=max_to_kill,
        batch_size_to_kill=batch_size_to_kill,
        kill_filter_fn=kill_filter_fn,
    )
    print("Waiting for ResourceKiller to be ready...")
    ray.get(resource_killer.ready.remote())
    print("ResourceKiller is ready now.")
    if not no_start:
        resource_killer.run.remote()
    return resource_killer
