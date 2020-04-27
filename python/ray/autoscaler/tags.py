"""The Ray autoscaler uses tags/labels to associate metadata with instances."""

# Tag for the name of the node
TAG_RAY_NODE_NAME = "ray-node-name"

# Tag for the type of node (e.g. Head, Worker)
TAG_RAY_NODE_TYPE = "ray-node-type"
NODE_TYPE_HEAD = "head"
NODE_TYPE_WORKER = "worker"

# Tag that reports the current state of the node (e.g. Updating, Up-to-date)
TAG_RAY_NODE_STATUS = "ray-node-status"
STATUS_UNINITIALIZED = "uninitialized"
STATUS_WAITING_FOR_SSH = "waiting-for-ssh"
STATUS_SYNCING_FILES = "syncing-files"
STATUS_SETTING_UP = "setting-up"
STATUS_UPDATE_FAILED = "update-failed"
STATUS_UP_TO_DATE = "up-to-date"

# Tag uniquely identifying all nodes of a cluster
TAG_RAY_CLUSTER_NAME = "ray-cluster-name"

# Hash of the node launch config, used to identify out-of-date nodes
TAG_RAY_LAUNCH_CONFIG = "ray-launch-config"

# Hash of the node runtime config, used to determine if updates are needed
TAG_RAY_RUNTIME_CONFIG = "ray-runtime-config"
