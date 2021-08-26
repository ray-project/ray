import warnings

import ray


# TODO(ekl) deprecate this after Ray 1.4
def get_node_ip_address(address="8.8.8.8:53"):
    warnings.warn(
        DeprecationWarning(
            "ray.services.get_node_ip_address has been moved to "
            "ray.util.get_node_ip_address. 'ray.services' will be "
            "removed after Ray 1.4."),
        stacklevel=2)
    return ray.util.get_node_ip_address(address)
