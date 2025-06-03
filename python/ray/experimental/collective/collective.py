from typing import Dict, List, Optional, Union
import threading
import uuid

import ray
from ray.experimental.collective.communicator import Communicator, CommunicatorHandle
from ray.experimental.collective.util import get_address_and_port
from ray.util.collective.types import Backend
from ray.util.collective.util import Info


_local_communicator_manager: "Optional[LocalCommunicatorManager]" = None
_local_communicator_manager_lock = threading.Lock()

_remote_communicator_manager: "Optional[RemoteCommunicatorManager]" = None
_remote_communicator_manager_lock = threading.Lock()


class RemoteCommunicatorManager:
    def __init__(self):
        # Handles to communicators that we created. Key is a user-provided
        # name or UUID.
        self._remote_communicators: Dict[str, CommunicatorHandle] = {}

    @staticmethod
    def get() -> "RemoteCommunicatorManager":
        global _remote_communicator_manager
        with _remote_communicator_manager_lock:
            if _remote_communicator_manager is None:
                _remote_communicator_manager = RemoteCommunicatorManager()
            return _remote_communicator_manager

    def add_remote_communicator(self, comm_handle: CommunicatorHandle):
        self._remote_communicators[comm_handle.name] = comm_handle

    def remove_remote_communicator(self, name: str):
        return self._remote_communicators.pop(name, None)

    def get_collective_groups(
        self, actors: Optional[List[ray.actor.ActorHandle]] = None, backend: Optional[str] = None
    ):
        """
        Get the collective groups that the given actors are a subset of. Filter by
        backend if provided.
        """
        actors = actors or []
        actors = set(actors)

        collectives = []
        # Find all collective groups that the given actors are a subset
        # of, with the matching backend if provided.
        for collective in self._remote_communicators.values():
            if actors.issubset(set(collective.actors)):
                if backend is None or collective.backend == backend:
                    collectives.append(collective)
        return collectives


class LocalCommunicatorManager:
    """
    Class to manage all communicators that we are a member of.
    """

    def __init__(self):
        # Communicators that we are a member of. Key is a user-provided
        # name or UUID.
        self._communicators: Dict[str, Communicator] = {}

    @staticmethod
    def get() -> "LocalCommunicatorManager":
        global _local_communicator_manager
        with _local_communicator_manager_lock:
            if _local_communicator_manager is None:
                _local_communicator_manager = LocalCommunicatorManager()
            return _local_communicator_manager

    def add_communicator(self, name: str, comm: Communicator):
        self._communicators[name] = comm

    def remove_communicator(self, name: str) -> Communicator:
        try:
            popped = self._communicators.pop(name)
        except KeyError:
            popped = None
        return popped


def _do_init_collective_group(
    self,
    world_size: int,
    rank: int,
    backend=Backend.NCCL,
    name: str = "default",
):
    ray.util.collective.init_collective_group(
        world_size, rank, backend, group_name=name
    )

    manager = LocalCommunicatorManager.get()
    comm = Communicator(name, rank, backend)
    manager.add_communicator(name, comm)


def _do_destroy_collective_group(self, name):
    manager = LocalCommunicatorManager.get()
    if manager.remove_communicator(name):
        ray.util.collective.destroy_collective_group(name)


def get_collective_groups(
    actors: List[ray.actor.ActorHandle], backend: Optional[str] = None
) -> List[CommunicatorHandle]:
    """
    Get the collective groups that the given actors are a subset of. Filter by
    backend if provided.

    Args:
        actors: List of actors. Return handles to all collective groups that
            these actors are a subset of.
        backend: An optional backend to filter by. See
            ray.util.collective.types.Backend for valid backends.

    Returns:
        A list of communicator handles that the actors are a subset of.
    """
    manager = RemoteCommunicatorManager.get()
    return manager.get_collective_groups(actors, backend)


def create_collective_group(
    actors: List[ray.actor.ActorHandle],
    backend: str,
    name: Optional[str] = None,
) -> CommunicatorHandle:
    """
    Create a collective group on the given list of actors. If this function
    returns successfully, then the collective group has been initialized on all
    actors, using the given order of actors as the ranks.

    Currently, an actor can only participate in one collective group per
    backend at a time. To reuse an actor, destroy its collective group and
    create a new one.

    Args:
        actors: The actors to participate in the collective group.
        backend: The backend to use. See ray.util.collective.types.Backend for
            valid backends.
        name: A name to use for the collective group. If None is provided, a
            random name will be generated.

    Returns:
        Handle to the communicator.
    """
    manager = RemoteCommunicatorManager.get()

    if name is None:
        name = str(uuid.uuid4())

    # Validate the backend.
    backend = Backend(backend)

    world_size = len(actors)

    for actor in actors:
        if manager.get_collective_groups([actor], backend):
            raise RuntimeError(
                f"Actor {actor} already in group for backend {backend}. Actors can currently only participate in at most one group per backend."
            )

    actor_ids = [actor._ray_actor_id for actor in actors]
    if len(set(actor_ids)) != len(actor_ids):
        raise ValueError(f"All actors must be unique, got: {actors}")

    info_actor = None
    if backend == Backend.TORCH_GLOO:
        # Perform extra setup for torch.distributed.
        # torch.distributed requires a master address and port. Find a suitable
        # port on one of the actors.
        master_metadata = ray.get(
            actors[0].__ray_call__.remote(lambda self: get_address_and_port())
        )

        # Store the metadata on a named actor that all of the other
        # actors can access.
        info_actor_name = "info_" + name
        info_actor = Info.options(name=info_actor_name).remote()
        actor_ids = [a._ray_actor_id for a in actors]
        ray.get(
            info_actor.set_info.remote(
                actor_ids, world_size, -1, backend, -1, master_metadata
            )
        )

    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_collective_group, world_size, rank, backend, name
        )
        for rank, actor in enumerate(actors)
    ]
    ray.get(init_tasks)

    # Make sure the temporary named actor is cleaned up.
    if info_actor is not None:
        ray.kill(info_actor, no_restart=True)

    # Group was successfully created.
    comm = CommunicatorHandle(actors, name, backend)
    manager.add_remote_communicator(comm)
    return comm


def destroy_collective_group(group_or_name: Union[CommunicatorHandle, str]):
    """
    Destroy a collective group. If this functions returns successfully, then
    the actors that were in the collective can be reused to create a new
    collective group.

    Args:
        group_or_name: Either a communicator handle or the name of the group to
            destroy.
    """
    if isinstance(group_or_name, CommunicatorHandle):
        name = group_or_name.name
    elif isinstance(group_or_name, str):
        name = group_or_name
    else:
        raise ValueError("Expected CommunicatorHandle or str (group name).")

    manager = RemoteCommunicatorManager.get()
    group = manager.remove_remote_communicator(name)
    if group is not None:
        destroy_tasks = [
            actor.__ray_call__.remote(_do_destroy_collective_group, name)
            for actor in group.actors
        ]
        ray.get(destroy_tasks)
    else:
        raise ValueError(f"No group with name {name} found.")


def destroy_all_collective_groups():
    manager = RemoteCommunicatorManager.get()
    for collective in manager.get_collective_groups():
        destroy_collective_group(collective.name)
