from collections import defaultdict, deque
import logging
import platform
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

import ray
from ray.actor import ActorClass, ActorHandle
from ray.rllib.utils.deprecation import Deprecated

logger = logging.getLogger(__name__)


class TaskPool:
    """Helper class for tracking the status of many in-flight actor tasks."""

    def __init__(self):
        self._tasks = {}
        self._objects = {}
        self._fetching = deque()

    def add(self, worker, all_obj_refs):
        if isinstance(all_obj_refs, list):
            obj_ref = all_obj_refs[0]
        else:
            obj_ref = all_obj_refs
        self._tasks[obj_ref] = worker
        self._objects[obj_ref] = all_obj_refs

    def completed(self, blocking_wait=False):
        pending = list(self._tasks)
        if pending:
            ready, _ = ray.wait(pending, num_returns=len(pending), timeout=0)
            if not ready and blocking_wait:
                ready, _ = ray.wait(pending, num_returns=1, timeout=10.0)
            for obj_ref in ready:
                yield (self._tasks.pop(obj_ref), self._objects.pop(obj_ref))

    def completed_prefetch(self, blocking_wait=False, max_yield=999):
        """Similar to completed but only returns once the object is local.

        Assumes obj_ref only is one id."""

        for worker, obj_ref in self.completed(blocking_wait=blocking_wait):
            self._fetching.append((worker, obj_ref))

        for _ in range(max_yield):
            if not self._fetching:
                break

            yield self._fetching.popleft()

    def reset_workers(self, workers):
        """Notify that some workers may be removed."""
        for obj_ref, ev in self._tasks.copy().items():
            if ev not in workers:
                del self._tasks[obj_ref]
                del self._objects[obj_ref]

        # We want to keep the same deque reference so that we don't suffer from
        # stale references in generators that are still in flight
        for _ in range(len(self._fetching)):
            ev, obj_ref = self._fetching.popleft()
            if ev in workers:
                # Re-queue items that are still valid
                self._fetching.append((ev, obj_ref))

    @property
    def count(self):
        return len(self._tasks)


def create_colocated_actors(
        actor_specs: Sequence[Tuple[Type, Any, Any, int]],
        node: Optional[str] = "localhost",
        max_attempts: int = 10,
) -> Dict[Type, List[ActorHandle]]:
    """Create co-located actors of any type(s) on any node.

    Args:
        actor_specs: Tuple/list with tuples consisting of: 1) The
            (already @ray.remote) class(es) to construct, 2) c'tor args,
            3) c'tor kwargs, and 4) the number of actors of that class with
            given args/kwargs to construct.
        node: The node to co-locate the actors on. By default ("localhost"),
            place the actors on the node the caller of this function is
            located on. Use None for indicating that any (resource fulfilling)
            node in the cluster may be used.
        max_attempts: The maximum number of co-location attempts to
            perform before throwing an error.

    Returns:
        A dict mapping the created types to the list of n ActorHandles
        created (and co-located) for that type.
    """
    if node == "localhost":
        node = platform.node()

    # Maps each entry in `actor_specs` to lists of already co-located actors.
    ok = [[] for _ in range(len(actor_specs))]

    # Try n times to co-locate all given actor types (`actor_specs`).
    # With each (failed) attempt, increase the number of actors we try to
    # create (on the same node), then kill the ones that have been created in
    # excess.
    for attempt in range(max_attempts):
        # If any attempt to co-locate fails, set this to False and we'll do
        # another attempt.
        all_good = True
        # Process all `actor_specs` in sequence.
        for i, (typ, args, kwargs, count) in enumerate(actor_specs):
            args = args or []  # Allow None.
            kwargs = kwargs or {}  # Allow None.
            # We don't have enough actors yet of this spec co-located on
            # the desired node.
            if len(ok[i]) < count:
                co_located = try_create_colocated(
                    cls=typ,
                    args=args,
                    kwargs=kwargs,
                    count=count * (attempt + 1),
                    node=node)
                # If node did not matter (None), from here on, use the host
                # that the first actor(s) are already co-located on.
                if node is None:
                    node = ray.get(co_located[0].get_host.remote())
                # Add the newly co-located actors to the `ok` list.
                ok[i].extend(co_located)
                # If we still don't have enough -> We'll have to do another
                # attempt.
                if len(ok[i]) < count:
                    all_good = False
            # We created too many actors for this spec -> Kill/truncate
            # the excess ones.
            if len(ok[i]) > count:
                for a in ok[i][count:]:
                    a.__ray_terminate__.remote()
                ok[i] = ok[i][:count]

        # All `actor_specs` have been fulfilled, return lists of
        # co-located actors.
        if all_good:
            return ok

    raise Exception("Unable to create enough colocated actors -> aborting.")


def try_create_colocated(
        cls: Type[ActorClass],
        args: List[Any],
        count: int,
        kwargs: Optional[List[Any]] = None,
        node: Optional[str] = "localhost",
) -> List[ActorHandle]:
    """Tries to co-locate (same node) a set of Actors of the same type.

    Returns a list of successfully co-located actors. All actors that could
    not be co-located (with the others on the given node) will not be in this
    list.

    Creates each actor via it's remote() constructor and then checks, whether
    it has been co-located (on the same node) with the other (already created)
    ones. If not, terminates the just created actor.

    Args:
        cls: The Actor class to use (already @ray.remote "converted").
        args: List of args to pass to the Actor's constructor. One item
            per to-be-created actor (`count`).
        count: Number of actors of the given `cls` to construct.
        kwargs: Optional list of kwargs to pass to the Actor's constructor.
            One item per to-be-created actor (`count`).
        node: The node to co-locate the actors on. By default ("localhost"),
            place the actors on the node the caller of this function is
            located on. If None, will try to co-locate all actors on
            any available node.

    Returns:
        List containing all successfully co-located actor handles.
    """
    if node == "localhost":
        node = platform.node()

    kwargs = kwargs or {}
    actors = [cls.remote(*args, **kwargs) for _ in range(count)]
    co_located, non_co_located = split_colocated(actors, node=node)
    logger.info("Got {} colocated actors of {}".format(len(co_located), count))
    for a in non_co_located:
        a.__ray_terminate__.remote()
    return co_located


def split_colocated(
        actors: List[ActorHandle],
        node: Optional[str] = "localhost",
) -> Tuple[List[ActorHandle], List[ActorHandle]]:
    """Splits up given actors into colocated (on same node) and non colocated.

    The co-location criterion depends on the `node` given:
    If given (or default: platform.node()): Consider all actors that are on
    that node "colocated".
    If None: Consider the largest sub-set of actors that are all located on
    the same node (whatever that node is) as "colocated".

    Args:
        actors: The list of actor handles to split into "colocated" and
            "non colocated".
        node: The node defining "colocation" criterion. If provided, consider
            thos actors "colocated" that sit on this node. If None, use the
            largest subset within `actors` that are sitting on the same
            (any) node.

    Returns:
        Tuple of two lists: 1) Co-located ActorHandles, 2) non co-located
        ActorHandles.
    """
    if node == "localhost":
        node = platform.node()

    # Get nodes of all created actors.
    hosts = ray.get([a.get_host.remote() for a in actors])

    # If `node` not provided, use the largest group of actors that sit on the
    # same node, regardless of what that node is.
    if node is None:
        node_groups = defaultdict(set)
        for host, actor in zip(hosts, actors):
            node_groups[host].add(actor)
        max_ = -1
        largest_group = None
        for host in node_groups:
            if max_ < len(node_groups[host]):
                max_ = len(node_groups[host])
                largest_group = host
        non_co_located = []
        for host in node_groups:
            if host != largest_group:
                non_co_located.extend(list(node_groups[host]))
        return list(node_groups[largest_group]), non_co_located
    # Node provided (or default: localhost): Consider those actors "colocated"
    # that were placed on `node`.
    else:
        # Split into co-located (on `node) and non-co-located (not on `node`).
        co_located = []
        non_co_located = []
        for host, a in zip(hosts, actors):
            # This actor has been placed on the correct node.
            if host == node:
                co_located.append(a)
            # This actor has been placed on a different node.
            else:
                non_co_located.append(a)
        return co_located, non_co_located


@Deprecated(new="create_colocated_actors", error=False)
def create_colocated(cls, arg, count):
    kwargs = {}
    args = arg

    return create_colocated_actors(
        actor_specs=[(cls, args, kwargs, count)],
        node=platform.node(),  # force on localhost
    )[cls]


@Deprecated(error=False)
def drop_colocated(actors: List[ActorHandle]) -> List[ActorHandle]:
    colocated, non_colocated = split_colocated(actors)
    for a in colocated:
        a.__ray_terminate__.remote()
    return non_colocated
