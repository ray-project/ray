import logging

import ray

logger = logging.getLogger(__name__)


def _get_actor(name):
    worker = ray.worker.global_worker
    handle = worker.core_worker.get_named_actor_handle(name)
    return handle
