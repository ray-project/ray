import socket
from dataclasses import dataclass
import logging
from typing import List, TypeVar, Optional, Dict, Type, Tuple

import ray
from ray.actor import ActorHandle

T = TypeVar("T")
ActorMetadata = TypeVar("ActorMetadata")

logger = logging.getLogger(__name__)


@dataclass
class Worker:
    """Class containing an actor and its metadata."""
    actor: ActorHandle
    metadata: ActorMetadata

@dataclass
class ActorConfig:
    pass

class ActorGroup:
    """Group of Ray Actors that can execute arbitrary functions.

    ``WorkerGroup`` launches Ray actors according to the given
    specification. It can then execute arbitrary Python functions in each of
    these actors.

    If not enough resources are available to launch the actors, the Ray
    cluster will automatically scale up if autoscaling is enabled.

    Args:
        actor_cls (Type): The class to use as the remote actors.
        num_actors (int): The number of the provided Ray actors to
            launch. Defaults to 1.
        num_cpus_per_actor (float): The number of CPUs to reserve for each
            actor. Fractional values are allowed. Defaults to 1.
        num_gpus_per_actor (float): The number of GPUs to reserve for each
            actor. Fractional values are allowed. Defaults to 0.
        resources_per_actor (Optional[Dict[str, float]]):
            Dictionary specifying the resources that will be
            requested for each actor in addition to ``num_cpus_per_actor``
            and ``num_gpus_per_actor``.
        remote_cls_args, remote_cls_kwargs: If ``remote_cls`` is provided,
            these args will be used for the actor initialization.


    Example:

    .. code_block:: python

        actor_group = ActorGroup(num_actors=2)
        output = actor_group.execute(lambda: 1)
        assert len(output) == 2
        assert all(o == 1 for o in output)
    """

    def __init__(
            self,
            actor_cls: Type,
            num_actors: int = 1,
            num_cpus_per_actor: float = 1,
            num_gpus_per_actor: float = 0,
            resources_per_actor: Optional[Dict[str, float]] = None,
            actor_cls_args: Optional[Tuple] = None,
            actor_cls_kwargs: Optional[Dict] = None):

        if num_actors <= 0:
            raise ValueError("The provided `num_actors` must be greater "
                             f"than 0. Received num_actors={num_actors} "
                             f"instead.")
        if num_cpus_per_actor < 0 or num_gpus_per_actor < 0:
            raise ValueError("The number of CPUs and GPUs per actor must "
                             "not be negative. Received "
                             f"num_cpus_per_actor={num_cpus_per_actor} and "
                             f"num_gpus_per_actor={num_gpus_per_actor}.")

        self.actors = []

        self.num_actors = num_actors
        self.num_cpus_per_actor = num_cpus_per_actor
        self.num_gpus_per_actor = num_gpus_per_actor
        self.additional_resources_per_actor = resources_per_actor

        self._actor_cls_args = actor_cls_args or []
        self._actor_cls_kwargs = actor_cls_kwargs or {}

        #TODO: make into dataclass.
        self._remote_cls = ray.remote(
            num_cpus=self.num_cpus_per_actor,
            num_gpus=self.num_gpus_per_actor,
            resources=self.additional_resources_per_actor)(actor_cls)
        self.start()

    def start(self):
        """Starts all the actors in this actor group."""
        if self.actors and len(self.actors) > 0:
            raise RuntimeError("The actors have already been started. "
                               "Please call `shutdown` first if you want to "
                               "restart them.")

        logger.debug(f"Starting {self.num_actors} actors.")
        self.add_actors(self.num_actors)
        logger.debug(f"{len(self.actors)} actors have successfully started.")

    def shutdown(self, patience_s: float = 5):
        """Shutdown all the actors in this actor group.

        Args:
            patience_s (float): Attempt a graceful shutdown
                of the actors for this many seconds. Fallback to force kill
                if graceful shutdown is not complete after this time. If
                this is less than or equal to 0, immediately force kill all
                actors.
        """
        logger.debug(f"Shutting down {len(self.actors)} actors.")
        if patience_s <= 0:
            for actor in self.actors:
                ray.kill(actor.actor)
        else:
            done_refs = [
                w.actor.__ray_terminate__.remote() for w in self.actors
            ]
            # Wait for actors to die gracefully.
            done, not_done = ray.wait(done_refs, timeout=patience_s)
            if not_done:
                logger.debug("Graceful termination failed. Falling back to "
                             "force kill.")
                # If all actors are not able to die gracefully, then kill them.
                for actor in self.actors:
                    ray.kill(actor.actor)

        logger.debug("Shutdown successful.")
        self.actors = []

    def remove_actors(self, actor_indexes: List[int]):
        """Removes the actors with the specified indexes.

        Args:
            actor_indexes (List[int]): The indexes of the actors to remove.
        """
        new_actors = []
        for i in range(len(self.actors)):
            if i not in actor_indexes:
                new_actors.append(self.actors[i])
        self.actors = new_actors

    def add_actors(self, num_actors: int):
        """Adds ``num_actors`` to this ActorGroup.

        Args:
            num_actors (int): The number of actors to add.
        """
        new_actors = []
        new_actor_metadata = []
        for _ in range(num_actors):
            actor = self._remote_cls.remote(*self._actor_cls_args,
                                            **self._actor_cls_kwargs)
            new_actors.append(actor)
            new_actor_metadata.append(
                actor._BaseactorMixin__execute.remote(construct_metadata))

        # Get metadata from all actors.
        metadata = ray.get(new_actor_metadata)

        for i in range(len(new_actors)):
            self.actors.append(
                Actor(actor=new_actors[i], metadata=metadata[i]))

    def __len__(self):
        return len(self.actors)

    def __getitem__(self, item):
        return self.actors[item]
