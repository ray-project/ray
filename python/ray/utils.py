import ray._private.utils as private_utils

deprecated = private_utils.deprecated(
    "If you need to use this function, open a feature request issue on " "GitHub.",
    removal_release="1.4",
    warn_once=True,
)

get_system_memory = deprecated(private_utils.get_system_memory)
