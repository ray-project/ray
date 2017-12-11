from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

"""The Ray autoscaler uses tags to associate metadata with instances."""

# Tag for the name of the node
TAG_NAME = "Name"

# Tag for the type of node (e.g. Head, Worker)
TAG_RAY_NODE_TYPE = "ray:NodeType"

# Tag uniquely identifying all nodes in the cluster
TAG_RAY_WORKER_GROUP = "ray:WorkerGroup"

# Tag that reports the current state of the node (e.g. Updating, Up-to-date)
TAG_RAY_WORKER_STATUS = "ray:WorkerStatus"

# Hash of the node launch config, used to identify out-of-date nodes
TAG_RAY_LAUNCH_CONFIG = "ray:LaunchConfig"

# Hash of the node runtime config, used to determine if updates are needed
TAG_RAY_RUNTIME_CONFIG = "ray:RuntimeConfig"
