import logging

import ray

logger = logging.getLogger(__name__)


def _get_actor(name):
    worker = ray.worker.global_worker
    handle = worker.core_worker.get_named_actor_handle(name)
    return handle


def get_actor(name: str) -> ray.actor.ActorHandle:
    """Get a named actor which was previously created.

    If the actor doesn't exist, an exception will be raised.

    Args:
        name: The name of the named actor.

    Returns:
        The ActorHandle object corresponding to the name.
    """
    logger.warning("ray.util.get_actor has been moved to ray.get_actor and "
                   "will be removed in the future.")
    return _get_actor(name)
