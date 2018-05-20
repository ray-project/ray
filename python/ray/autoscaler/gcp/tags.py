"""The Ray autoscaler uses tags/labels to associate metadata with instances.

Notice that the name 'tag' here is somewhat misnomer, since gcp uses the name
'label' for key-value pairs, and 'tag' for value-only tags.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

TAG_KEYS = {
    # Tag uniquely identifying all nodes of a cluster
    'node-name': 'ray_node-name',
    # Tag for the type of node (e.g. Head, Worker)
    'node-type': 'ray_node-type',
    # Tag that reports the current state of the node (e.g. Updating, Up-to-date)
    'node-status': 'ray_node-status',
    # Tag identifying nodes of a cluster
    'cluster-name': 'ray_cluster-name',
    # Hash of the node launch config, used to identify out-of-date nodes
    'launch-config': 'ray_launch-config',
    # Hash of the node runtime config, used to determine if updates are needed
    'runtime-config': 'ray_runtime-config',
}


TAG_VALUES = {
    'head': 'head',
    'waiting-for-ssh': 'waiting-for-ssh',
    'update-failed': 'update-failed',
    'setting-up': 'setting-up',
    'syncing-files': 'syncing-files',
    'up-to-date': 'up-to-date',
    'worker': 'worker',
    'uninitialized': 'uninitialized',
}
