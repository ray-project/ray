import re

def is_ray_node_resource(resource_key):
    """Check if the current resource is the host ip."""
    ishost_regex = re.compile(r"^node:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    return ishost_regex.match(resource_key)
