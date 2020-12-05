"""
Constants.

Contains constants used to setup collective groups.
"""
NAMED_ACTOR_STORE_SUFFIX = "_unique_id_actor"


def get_nccl_store_name(group_name):
    """Generate the unique name for the NCCLUniqueID store (named actor)."""
    if not group_name:
        raise ValueError("group_name is None.")
    return group_name + NAMED_ACTOR_STORE_SUFFIX
