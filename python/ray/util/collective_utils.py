"""Collections of collective util functions"""


def collective_to_envs(collective, envs):
    """A helper method that get information from collective and add to envs.
    
    Args:
        collective(dict): collective information
        envs(dict): os environment dict

    Returns:
        envs(dict): modified os environment dict
    """

    if envs is not None:
        assert all([
            "collective_group_name", "collective_rank",
            "collective_world_size", "collective_backend"
        ]) not in envs

    else:
        envs = {}
    envs["collective_group_name"] = str(collective["group_name"])
    envs["collective_rank"] = str(collective["rank"])
    envs["collective_world_size"] = str(collective["world_size"])
    envs["collective_backend"] = str(collective["backend"])

    return envs
