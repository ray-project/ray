"""The Ray autoscaler uses tags/labels to associate metadata with instances."""

# Tag for the name of the node
TAG_RAY_NODE_NAME = "ray-node-name"

# Tag for the kind of node (e.g. Head, Worker). For legacy reasons, the tag
# value says 'type' instead of 'kind'.
TAG_RAY_NODE_KIND = "ray-node-type"
NODE_KIND_HEAD = "head"
NODE_KIND_WORKER = "worker"
NODE_KIND_UNMANAGED = "unmanaged"

# Tag for user defined node types (e.g., m4xl_spot). This is used for multi
# node type clusters.
TAG_RAY_USER_NODE_TYPE = "ray-user-node-type"
# Tag for autofilled node types for legacy cluster yamls without multi
# node type defined in the cluster configs.
NODE_TYPE_LEGACY_HEAD = "ray-legacy-head-node-type"
NODE_TYPE_LEGACY_WORKER = "ray-legacy-worker-node-type"

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
# Hash of the contents of the directories specified by the file_mounts config
# if the node is a worker, this also hashes content of the directories
# specified by the cluster_synced_files config
TAG_RAY_FILE_MOUNTS_CONTENTS = "ray-file-mounts-contents"
