from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
from functools import total_ordering
from typing import Callable, Dict, List, Set, Tuple

import ray
from ray.experimental.serve.object_id import get_new_oid
from ray.experimental.serve.utils.priority_queue import PriorityQueue

ACTOR_NOT_REGISTERED_MSG: Callable = (
    lambda name: ("Actor {} is not registered with this router. Please use "
                  "'router.register_actor.remote(...)' "
                  "to register it.").format(name))


# Use @total_ordering so we can sort SingleQuery
@total_ordering
class SingleQuery:
    """A data container for a query.

    Attributes:
        data: The request data.
        result_object_id: The result object ID.
        deadline: The deadline in seconds.
    """

    def __init__(self, data, result_object_id: ray.ObjectID,
                 deadline_s: float):
        self.data = data
        self.result_object_id = result_object_id
        self.deadline = deadline_s

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __eq__(self, other):
        return self.deadline == other.deadline


@ray.remote
class DeadlineAwareRouter:
    """DeadlineAwareRouter is a router that is aware of deadlines.

    It takes into consideration the deadline attached to each query. It will
    reorder incoming query based on their deadlines.
    """

    def __init__(self, router_name):
        # Runtime Data
        self.query_queues: Dict[str, PriorityQueue] = defaultdict(
            PriorityQueue)
        self.running_queries: Dict[ray.ObjectID, ray.actor.ActorHandle] = {}
        self.actor_handles: Dict[str, List[ray.actor.ActorHandle]] = (
            defaultdict(list))

        # Actor Metadata
        self.managed_actors: Dict[str, ray.actor.ActorClass] = {}
        self.actor_init_arguments: Dict[str, Tuple[List, Dict]] = {}
        self.max_batch_size: Dict[str, int] = {}

        # Router Metadata
        self.name = router_name

    def start(self):
        """Kick off the router loop"""

        # Note: This is meant for hiding the complexity for a user
        #       facing method.
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
        """Register a new managed actor.
        """
        self.managed_actors[actor_name] = actor_class
        self.actor_init_arguments[actor_name] = (init_args, init_kwargs)
        self.max_batch_size[actor_name] = max_batch_size

        ray.experimental.get_actor(self.name).set_replica.remote(
            actor_name, num_replicas)

    def set_replica(self, actor_name, new_replica_count):
        """Scale a managed actor according to new_replica_count."""
        assert actor_name in self.managed_actors, (
            ACTOR_NOT_REGISTERED_MSG(actor_name))

        current_replicas = len(self.actor_handles[actor_name])

        # Increase the number of replicas
        if new_replica_count > current_replicas:
            for _ in range(new_replica_count - current_replicas):
                args = self.actor_init_arguments[actor_name][0]
                kwargs = self.actor_init_arguments[actor_name][1]
                new_actor_handle = self.managed_actors[actor_name].remote(
                    *args, **kwargs)
                self.actor_handles[actor_name].append(new_actor_handle)

        # Decrease the number of replicas
        if new_replica_count < current_replicas:
            for _ in range(current_replicas - new_replica_count):
                # Note actor destructor will be called after all remaining
                # calls finish. Therefore it's safe to call del here.
                del self.actor_handles[actor_name][-1]

    def call(self, actor_name, data, deadline_s):
        """Enqueue a request to one of the actor managed by this router.

        Returns:
            List[ray.ObjectID] with length 1, the object ID wrapped inside is
                the result object ID when the query is executed.
        """
        assert actor_name in self.managed_actors, (
            ACTOR_NOT_REGISTERED_MSG(actor_name))

        result_object_id = get_new_oid()
        self.query_queues[actor_name].push(
            SingleQuery(data, result_object_id, deadline_s))

        return [result_object_id]

    def loop(self):
        """Main loop for router. It will does the following things:

        1. Check which running actors finished.
        2. Iterate over free actors and request queues, dispatch requests batch
           to free actors.
        3. Tail recursively schedule itself.
        """

        # 1. Check which running actors finished.
        ready_oids, _ = ray.wait(
            object_ids=list(self.running_queries.keys()),
            num_returns=len(self.running_queries),
            timeout=0,
        )

        for ready_oid in ready_oids:
            self.running_queries.pop(ready_oid)
        busy_actors: Set[ray.actor.ActorHandle] = set(
            self.running_queries.values())

        # 2. Iterate over free actors and request queues, dispatch requests
        #    batch to free actors.
        for actor_name, queue in self.query_queues.items():
            # try to drain the queue
            for actor_handle in self.actor_handles[actor_name]:
                if len(queue) == 0:
                    break

                if actor_handle in busy_actors:
                    continue

                # A free actor found. Dispatch queries.
                batch = self._get_next_batch(actor_name)
                assert len(batch)

                batch_result_object_id = actor_handle._dispatch.remote(batch)
                self._mark_running(batch_result_object_id, actor_handle)

        # 3. Tail recursively schedule itself.
        ray.experimental.get_actor(self.name).loop.remote()

    def _get_next_batch(self, actor_name: str) -> List[SingleQuery]:
        """Get next batch of request for the actor whose name is provided."""
        assert actor_name in self.query_queues, (
            ACTOR_NOT_REGISTERED_MSG(actor_name))

        inputs = []
        batch_size = self.max_batch_size[actor_name]
        if batch_size == -1:
            inp = self.query_queues[actor_name].try_pop()
            while inp:
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

    def _mark_running(self, batch_oid: ray.ObjectID,
                      actor_handle: ray.actor.ActorHandle):
        """Mark actor_handle as running identified by batch_oid.

        This means that if batch_oid is fullfilled, then actor_handle must be
        free.
        """
        self.running_queries[batch_oid] = actor_handle
