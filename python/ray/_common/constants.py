# Prefix for the node id resource that is automatically added to each node.
# For example, a node may have id `node:172.23.42.1`.
NODE_ID_PREFIX = "node:"
# The system resource that head node has.
HEAD_NODE_RESOURCE_NAME = NODE_ID_PREFIX + "__internal_head__"

RAY_WARN_BLOCKING_GET_INSIDE_ASYNC_ENV_VAR = "RAY_WARN_BLOCKING_GET_INSIDE_ASYNC"
