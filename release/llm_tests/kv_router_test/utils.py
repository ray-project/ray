"""Shared helpers for the KV-router GPU release tests."""

import ray
from ray.serve._private.constants import (
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
)
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm.request_router import KVAwareRouter


class _TestKVAwareRouter(RoundRobinRouter, KVAwareRouter):
    """A ``KVAwareRouter`` subclass that borrows ``RoundRobinRouter``'s selection.

    KVAwareRouter's own routing (scoring by KV-cache overlap) is not implemented
    yet, so this inherits RoundRobinRouter's ``choose_replicas`` (via MRO) while
    remaining a KVAwareRouter subclass.

    TODO (jeffreywang): Remove RoundRobinRouter base class once KVAwareRouter's selection
    is implemented.
    """


def discover_deployment_actor(app_name, deployment_name, actor_name):
    """Resolve a deployment-scoped actor by its registered name.

    The test driver isn't a replica, so ``get_deployment_actor`` is
    unavailable; match the name's stable prefix/suffix instead (the middle
    embeds an opaque code_version).
    """
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    suffix = f"::{actor_name}"
    for entry in ray.util.list_named_actors(all_namespaces=True):
        name = entry.get("name") or ""
        if (
            entry.get("namespace") == SERVE_NAMESPACE
            and name.startswith(prefix)
            and name.endswith(suffix)
        ):
            return ray.get_actor(name, namespace=SERVE_NAMESPACE)
    return None
