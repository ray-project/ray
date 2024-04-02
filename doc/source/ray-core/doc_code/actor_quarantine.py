# flake8: noqa

# fmt: off
# __actor_qurantine_begin__
import os
import ray
from typing import List, Tuple
import asyncio

ray.init()

@ray.remote
class Actor:
    def __init__(self):
        self.counter = 0

    def ping(self):
        return "pong"
    
    def workload(self):
        self.counter += 1
        return self.counter

class ControlPlane:
    def __init__(self, num_replicas=10) -> None:
        self.actors = []
        for i in range(num_replicas):
            self.spawn_actor()
        self.quarantined = {} # (quarantine_id -> (actor, count))
        self.ping_counts = 10
        self.next_quanrantine_id = 0
        self.ping_task = asyncio.get_event_loop().create_task(self.infinite_ping())

    async def infinite_ping(self):
        while True:
            await self.ping_quarantined_actors()
            await asyncio.sleep(10)

    async def ping_quarantined_actors(self):
        """Pings all quarantined actors. Each actor is pinged once. If an actor is dead,
        replaces it with a new one. If the actor is still unavailable, decrement the
        countdown, or replace it if the countdown reaches 0. If the actor is recovered,
        put it back to `self.actors` to receive production workloads.
        """
        pings = [(id, a.ping.remote()) for id, (a, _) in self.quarantined.items()]
        for id, obj_ref in pings:
            try:
                await obj_ref
            except ray.exceptions.RayActorError:
                # the actor is dead, discard.
                del self.quarantined[id]
                self.spawn_actor()
            except ray.exceptions.ActorUnavailableError:
                # still unavailable, retry with count decremented, or discard.
                a, count = self.quarantined[id]
                count -= 1
                if count == 0:
                    del self.quarantined[id]
                    ray.kill(a)
                    self.spawn_actor()
                else:
                    self.quarantined[id] = (a, count)
            else:
                # actor recovered, put it back to service.
                a, count = self.quarantined[id]
                del self.quarantined[id]
                self.actors.append(a)
                

    async def workload(self):
        """
        Selects a replica and runs the workload on it. If the replica is dead, replaces
        it with a new one. If the replica is unavailable, quarantines it.

        If the actor is dead or unavailable, the workload is retried on another actor.
        """
        id = self.next_actor_id()
        a = self.actors[id]

        obj_ref = a.workload.remote()
        try:
            return await obj_ref
        except ray.exceptions.RayActorError:
            # actor is dead, delete it, spawn another one, and retry the request on another actor.
            del self.actors[a]
            self.spawn_actor()
            return await self.workload()
        except ray.exceptions.ActorUnavailableError:
            # actor is unavailable, quarantine it, and retry the request on another actor.
            self.actors.pop(id)
            self.quarantined[self.next_quanrantine_id] = (a, self.ping_counts)
            self.next_quanrantine_id += 1
            return await self.workload()


    def next_actor_id(self):
        # Rounter logic to decide an actor to handle requests
        ...

    def spawn_actor(self):
        # spawns another actor and add it to `self.actors`.
        ...

# User code
control_plane = ControlPlane.remote()
print(ray.get(control_plane.workload.remote()))

# __actor_qurantine_end__
# fmt: on
