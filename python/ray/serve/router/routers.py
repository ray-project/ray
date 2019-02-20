import logging
import time
from collections import defaultdict
from functools import total_ordering
from typing import Callable, Dict, List, Set, Tuple

import ray
from ray.serve.object_id import get_new_oid
from ray.serve.utils.debug import print_debug
from ray.serve.utils.PriorityQueue import PriorityQueue

ACTOR_NOT_REGISTERED_MSG: Callable = lambda name: """
Actor {} is not registered with this router. Please use router.register_actor.remote(...)
to regsier it.
""".format(
    name
)


@total_ordering
class SingleQuery:
    def __init__(self, data, result_oid: ray.ObjectID, deadline_s: float):
        self.data = data
        self.result_oid = result_oid
        self.deadline = deadline_s

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __eq__(self, other):
        return self.deadline == other.deadline


@ray.remote
class DeadlineAwareRouter:
    def __init__(self, router_name):
        # Runtime Data
        self.query_queues: Dict[str, PriorityQueue] = defaultdict(PriorityQueue)
        self.running_queries: Dict[ray.ObjectID, ray.actor.ActorHandle] = dict()
        self.actor_handles: Dict[str, List[ray.actor.ActorHandle]] = defaultdict(list)

        # Actor Metadata
        self.managed_actors: Dict[str, ray.actor.ActorClass] = dict()
        self.actor_init_arguments: Dict[str, Tuple[List, Dict]] = dict()
        self.max_batch_size: Dict[str, int] = dict()

        self.name = router_name

    def start(self):
        """Kick off the router loop"""
        # Note: This is meant for hiding the complexity.
        #       Because the `loop` api can be hard to understand.
        ray.experimental.get_actor(self.name).loop.remote()

    def register_actor(
        self,
        actor_name: str,
        actor_class: ray.actor.ActorClass,
        init_args: List = [],
        init_kwargs: dict = {},
        num_replicas: int = 1,
        max_batch_size: int = -1,  # Unbounded batch size
    ):
        self.managed_actors[actor_name] = actor_class
        self.actor_init_arguments[actor_name] = (init_args, init_kwargs)
        self.max_batch_size[actor_name] = max_batch_size

        ray.experimental.get_actor(self.name).set_replica.remote(
            actor_name, num_replicas
        )

    def set_replica(self, actor_name, new_replica_count):
        assert actor_name in self.managed_actors, ACTOR_NOT_REGISTERED_MSG(actor_name)

        current_replicas = len(self.actor_handles[actor_name])

        # Increase the number of replicas
        if new_replica_count > current_replicas:
            for _ in range(new_replica_count - current_replicas):
                args = self.actor_init_arguments[actor_name][0]
                kwargs = self.actor_init_arguments[actor_name][1]
                new_actor_handle = self.managed_actors[actor_name].remote(
                    *args, **kwargs
                )
                self.actor_handles[actor_name].append(new_actor_handle)

        # Decrease the number of replicas
        if new_replica_count < current_replicas:
            for _ in range(current_replicas - new_replica_count):
                removed_actor = self.actor_handles[actor_name].pop()

                # Note actor destructor will be called after all remaining call finishes
                # Therefore it's safe to call `del` ehre.
                del removed_actor

    def call(self, actor_name, data, deadline_s):
        assert actor_name in self.managed_actors, ACTOR_NOT_REGISTERED_MSG(actor_name)

        result_oid = get_new_oid()
        self.query_queues[actor_name].push(SingleQuery(data, result_oid, deadline_s))

        print_debug(
            "Received request for actor {0}, assigning object id {1}".format(
                actor_name, result_oid
            )
        )

        return [result_oid]

    def loop(self):
        ready_oids, _ = ray.wait(
            object_ids=list(self.running_queries.keys()),
            num_returns=len(self.running_queries),
            timeout=0,
        )

        print_debug("entering loop")

        for ready_oid in ready_oids:
            self.running_queries.pop(ready_oid)
        busy_actors: Set[ray.actor.ActorHandle] = set(self.running_queries.values())
        print_debug("Busy actors", busy_actors)

        for actor_name, queue in self.query_queues.items():
            print_debug("Go overing actor", actor_name, "with length", len(queue))
            # try to drain the queue

            for actor_handle in self.actor_handles[actor_name]:
                if len(queue) == 0:
                    break

                if actor_handle in busy_actors:
                    continue

                # A free actor found. Dispatch queries
                batch = self._get_next_batch(actor_name)
                print_debug("Assining batch", batch, "to", actor_handle)
                assert len(batch)

                actor_handle._dispatch.remote(batch)
                self._mark_running(batch, actor_handle)

        # time.sleep(2)
        ray.experimental.get_actor(self.name).loop.remote()

    def _get_next_batch(self, actor_name: str) -> List[SingleQuery]:
        assert actor_name in self.query_queues, ACTOR_NOT_REGISTERED_MSG(actor_name)

        inputs = []
        batch_size = self.max_batch_size[actor_name]
        if batch_size == -1:
            inp = self.query_queues[actor_name].try_pop()
            while inp:
                print_debug("Adding {0} to batch for actor {1}".format(inp, actor_name))
                inputs.append(inp)
                inp = self.query_queues[actor_name].try_pop()
        else:
            for _ in range(batch_size):
                inp = self.query_queues[actor_name].try_pop()
                if inp:
                    inputs.append(inp)
                else:
                    break

        return inputs

    def _mark_running(
        self, query_batch: List[SingleQuery], actor_handle: ray.actor.ActorHandle
    ):
        for query in query_batch:
            self.running_queries[query.result_oid] = actor_handle
