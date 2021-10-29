import ray

from ray.util.annotations import DeveloperAPI

# The context singleton on this process.
_default_context = None


@DeveloperAPI
class DatasetContext:
    """Singleton for shared Dataset resources and configurations.

    This object is automatically propagated to workers and can be retrieved
    from the driver and remote workers via DatasetContext.get_instance().
    """

    def __init__(self, block_owner: ray.actor.ActorHandle,
                 target_max_block_size: int):
        """Private constructor (use get_instance() instead)."""
        self.block_owner = block_owner
        self.target_max_block_size = target_max_block_size

    @staticmethod
    def get_instance():
        global _default_context
        if _default_context is None:
            _default_context = DatasetContext(
                _get_or_create_block_owner_actor(), 500 * 1024 * 1024)
        return _default_context

    @staticmethod
    def _set_current(context: "DatasetContext") -> None:
        global _default_context
        _default_context = context


@ray.remote(num_cpus=0, placement_group=None)
class _DesignatedBlockOwner:
    def ping(self):
        return "ok"


# We manually assign the owner so ray.put() blocks do not fate share with the
# worker that created them.
# TODO(ekl) remove this once we implement automatic ownership transfer.
def _get_or_create_block_owner_actor() -> ray.actor.ActorHandle:
    name = "datasets_global_block_owner"
    namespace = "datasets_global_namespace"
    try:
        actor = ray.get_actor(name=name, namespace=namespace)
    except ValueError:
        actor = _DesignatedBlockOwner.options(
            name=name, namespace=namespace, lifetime="detached").remote()
    ray.get(actor.ping.remote())
    return actor
