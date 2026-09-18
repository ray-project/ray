import threading

import ray
from ray.actor import ActorHandle

# All Ray Data singletons share one namespace so they can't collide with
# unrelated actors that happen to pick the same name.
SINGLETON_ACTOR_NAMESPACE = "_ray_data_singleton_actors"

# Ray Core doesn't support creating the same named actor from multiple threads
# simultaneously (https://github.com/ray-project/ray/issues/41324), so every
# creation goes through this process-wide lock.
_creation_lock = threading.RLock()


def get_or_create_singleton_actor(
    actor_cls: type, *, max_concurrency: int = 1
) -> ActorHandle:
    """Return the cluster's singleton actor for ``actor_cls``, creating it if needed.

    Ray Data keeps a handful of cluster-wide singletons (stats, usage collection,
    autoscaling, node trackers). They all want the same lifecycle: detached,
    restartable, and pinned to the caller's node so they fate-share with the
    driver and avoid cross-node chatter.

    The actor is named after ``actor_cls``, so singleton classes must have
    distinct names.

    Args:
        actor_cls: The actor's class. It must be undecorated; this function owns
            the ``ray.remote`` options so every singleton gets the same lifecycle.
        max_concurrency: Max concurrent calls the actor serves. The default of 1
            means the actor's state needs no internal locking.

    Returns:
        A handle to the existing or newly-created actor.
    """
    # For Ray Client the driver isn't a cluster worker, so this resolves to the
    # head node -- which is still the node we want to fate-share with.
    label_selector = {
        # pyrefly: ignore[missing-attribute]  # constant lives in the Cython ext
        ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
    }

    with _creation_lock:
        return (
            ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)(actor_cls)
            .options(
                name=actor_cls.__name__,
                namespace=SINGLETON_ACTOR_NAMESPACE,
                get_if_exists=True,
                lifetime="detached",
                label_selector=label_selector,
                max_concurrency=max_concurrency,
            )
            .remote()
        )
