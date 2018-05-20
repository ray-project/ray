"""The Ray autoscaler uses tags/labels to associate metadata with instances."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

TAG_KEYS = {
    # Tag uniquely identifying all nodes of a cluster
    'node-name': 'ray:NodeName',
    # Tag for the type of node (e.g. Head, Worker)
    'node-type': 'ray:NodeType',
    # Tag that reports the current state of the node (e.g. Updating, Up-to-date)
    'node-status': 'ray:NodeStatus',
    # Tag identifying nodes of a cluster
    'cluster-name': 'ray:ClusterName',
    # Hash of the node launch config, used to identify out-of-date nodes
    'launch-config': 'ray:LaunchConfig',
    # Hash of the node runtime config, used to determine if updates are needed
    'runtime-config': 'ray:RuntimeConfig',
}


TAG_VALUES = {
    'head': 'Head',
    'waiting-for-ssh': 'WaitingForSSH',
    'update-failed': 'UpdateFailed',
    'setting-up': 'SettingUp',
    'syncing-files': 'SyncingFiles',
    'up-to-date': 'Up-To-Date',
    'worker': 'Worker',
    'uninitialized': 'Uninitialized',
}
