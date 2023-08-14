import weakref
from dataclasses import dataclass
import logging
from typing import List, TypeVar, Optional, Dict, Type, Tuple

import ray
from ray.actor import ActorHandle
from ray.util.annotations import Deprecated
from ray._private.utils import get_ray_doc_version

T = TypeVar("T")
ActorMetadata = TypeVar("ActorMetadata")

logger = logging.getLogger(__name__)


@dataclass
class ActorWrapper:
    """Class containing an actor and its metadata."""

    actor: ActorHandle
    metadata: ActorMetadata


@dataclass
class ActorConfig:
    num_cpus: float
    num_gpus: float
    resources: Optional[Dict[str, float]]
    init_args: Tuple
    init_kwargs: Dict


class ActorGroupMethod:
    def __init__(self, actor_group: "ActorGroup", method_name: str):
        self.actor_group = weakref.ref(actor_group)
        self._method_name = method_name

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "ActorGroup methods cannot be called directly. "
            "Instead "
            f"of running 'object.{self._method_name}()', try "
            f"'object.{self._method_name}.remote()'."
        )

    def remote(self, *args, **kwargs):
        return [
            getattr(a.actor, self._method_name).remote(*args, **kwargs)
            for a in self.actor_group().actors
        ]


@Deprecated(
    message="For stateless/task processing, use ray.util.multiprocessing, see details "
    f"in https://docs.ray.io/en/{get_ray_doc_version()}/ray-more-libs/multiprocessing.html. "  # noqa: E501
    "For stateful/actor processing such as batch prediction, use "
    "Datasets.map_batches(compute=ActorPoolStrategy, ...), see details in "
    f"https://docs.ray.io/en/{get_ray_doc_version()}/data/api/dataset.html#ray.data.Dataset.map_batches.",  # noqa: E501
    warning=True,
)
class ActorGroup:
    """Group of Ray Actors that can execute arbitrary functions.

    ``ActorGroup`` launches Ray actors according to the given
    specification. It can then execute arbitrary Python functions in each of
    these actors.

    If not enough resources are available to launch the actors, the Ray
    cluster will automatically scale up if autoscaling is enabled.

    Args:
        actor_cls: The class to use as the remote actors.
        num_actors: The number of the provided Ray actors to
            launch. Defaults to 1.
        num_cpus_per_actor: The number of CPUs to reserve for each
            actor. Fractional values are allowed. Defaults to 1.
        num_gpus_per_actor: The number of GPUs to reserve for each
            actor. Fractional values are allowed. Defaults to 0.
        resources_per_actor (Optional[Dict[str, float]]):
            Dictionary specifying the resources that will be
            requested for each actor in addition to ``num_cpus_per_actor``
            and ``num_gpus_per_actor``.
        init_args, init_kwargs: If ``actor_cls`` is provided,
            these args will be used for the actor initialization.

    """

    def __init__(
        self,
        actor_cls: Type,
        num_actors: int = 1,
        num_cpus_per_actor: float = 1,
        num_gpus_per_actor: float = 0,
        resources_per_actor: Optional[Dict[str, float]] = None,
        init_args: Optional[Tuple] = None,
        init_kwargs: Optional[Dict] = None,
    ):
        from ray._private.usage.usage_lib import record_library_usage

        record_library_usage("util.ActorGroup")

        if num_actors <= 0:
            raise ValueError(
                "The provided `num_actors` must be greater "
                f"than 0. Received num_actors={num_actors} "
                f"instead."
            )
        if num_cpus_per_actor < 0 or num_gpus_per_actor < 0:
            raise ValueError(
                "The number of CPUs and GPUs per actor must "
                "not be negative. Received "
                f"num_cpus_per_actor={num_cpus_per_actor} and "
                f"num_gpus_per_actor={num_gpus_per_actor}."
            )

        self.actors = []

        self.num_actors = num_actors

        self.actor_config = ActorConfig(
            num_cpus=num_cpus_per_actor,
            num_gpus=num_gpus_per_actor,
            resources=resources_per_actor,
            init_args=init_args or (),
            init_kwargs=init_kwargs or {},
        )

        self._remote_cls = ray.remote(
            num_cpus=self.actor_config.num_cpus,
            num_gpus=self.actor_config.num_gpus,
            resources=self.actor_config.resources,
        )(actor_cls)

        self.start()

    def __getattr__(self, item):
        if len(self.actors) == 0:
            raise RuntimeError(
                "This ActorGroup has been shutdown. Please start it again."
            )
        # Same implementation as actor.py
        return ActorGroupMethod(self, item)

    def __len__(self):
        return len(self.actors)

    def __getitem__(self, item):
        return self.actors[item]

    def start(self):
        """Starts all the actors in this actor group."""
        if self.actors and len(self.actors) > 0:
            raise RuntimeError(
                "The actors have already been started. "
                "Please call `shutdown` first if you want to "
                "restart them."
            )

        logger.debug(f"Starting {self.num_actors} actors.")
        self.add_actors(self.num_actors)
        logger.debug(f"{len(self.actors)} actors have successfully started.")

    def shutdown(self, patience_s: float = 5):
        """Shutdown all the actors in this actor group.

        Args:
            patience_s: Attempt a graceful shutdown
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
            done_refs = [w.actor.__ray_terminate__.remote() for w in self.actors]
            # Wait for actors to die gracefully.
            done, not_done = ray.wait(done_refs, timeout=patience_s)
            if not_done:
                logger.debug("Graceful termination failed. Falling back to force kill.")
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
            num_actors: The number of actors to add.
        """
        new_actors = []
        new_actor_metadata = []
        for _ in range(num_actors):
            actor = self._remote_cls.remote(
                *self.actor_config.init_args, **self.actor_config.init_kwargs
            )
            new_actors.append(actor)
            if hasattr(actor, "get_actor_metadata"):
                new_actor_metadata.append(actor.get_actor_metadata.remote())

        # Get metadata from all actors.
        metadata = ray.get(new_actor_metadata)

        if len(metadata) == 0:
            metadata = [None] * len(new_actors)

        for i in range(len(new_actors)):
            self.actors.append(ActorWrapper(actor=new_actors[i], metadata=metadata[i]))

    @property
    def actor_metadata(self):
        return [a.metadata for a in self.actors]
