"""Defines schema for autoscaler YAML config files."""

import warnings

REQUIRED, OPTIONAL = True, False

# For (a, b), if a is a dictionary object, then
# no extra fields can be introduced.

CLUSTER_CONFIG_SCHEMA = {
    # An unique identifier for the head node and workers of this cluster.
    "cluster_name": (str, REQUIRED),

    # The minimum number of workers nodes to launch in addition to the head
    # node. This number should be >= 0.
    "min_workers": (int, OPTIONAL),

    # The maximum number of workers nodes to launch in addition to the head
    # node. This takes precedence over min_workers.
    "max_workers": (int, REQUIRED),

    # The number of workers to launch initially, in addition to the head node.
    "initial_workers": (int, OPTIONAL),

    # The mode of the autoscaler e.g. default, aggressive
    "autoscaling_mode": (str, OPTIONAL),

    # The autoscaler will scale up the cluster to this target fraction of
    # resources usage. For example, if a cluster of 8 nodes is 100% busy
    # and target_utilization was 0.8, it would resize the cluster to 10.
    "target_utilization_fraction": (float, OPTIONAL),

    # If a node is idle for this many minutes, it will be removed.
    "idle_timeout_minutes": (int, OPTIONAL),

    # Cloud-provider specific configuration.
    "provider": (
        {
            "type": (str, REQUIRED),  # e.g. aws
            "region": (str, OPTIONAL),  # e.g. us-east-1
            "availability_zone": (str, OPTIONAL),  # e.g. us-east-1a
            "module": (str,
                       OPTIONAL),  # module, if using external node provider
            "project_id": (None, OPTIONAL),  # gcp project id, if using gcp
            "head_ip": (str, OPTIONAL),  # local cluster head node
            "worker_ips": (list, OPTIONAL),  # local cluster worker nodes
            "use_internal_ips": (bool, OPTIONAL),  # don't require public ips
            "namespace": (str, OPTIONAL),  # k8s namespace, if using k8s

            # k8s autoscaler permissions, if using k8s
            "autoscaler_service_account": (dict, OPTIONAL),
            "autoscaler_role": (dict, OPTIONAL),
            "autoscaler_role_binding": (dict, OPTIONAL),
            "extra_config": (dict, OPTIONAL),  # provider-specific config

            # Whether to try to reuse previously stopped nodes instead of
            # launching nodes. This will also cause the autoscaler to stop
            # nodes instead of terminating them. Only implemented for AWS.
            "cache_stopped_nodes": (bool, OPTIONAL),
        },
        REQUIRED),

    # How Ray will authenticate with newly launched nodes.
    "auth": (
        {
            "ssh_user": (str, OPTIONAL),  # e.g. ubuntu
            "ssh_private_key": (str, OPTIONAL),
        },
        OPTIONAL),

    # Docker configuration. If this is specified, all setup and start commands
    # will be executed in the container.
    "docker": (
        {
            "image": (str, OPTIONAL),  # e.g. tensorflow/tensorflow:1.5.0-py3
            "container_name": (str, OPTIONAL),  # e.g., ray_docker
            "pull_before_run": (bool, OPTIONAL),  # run `docker pull` first
            # shared options for starting head/worker docker
            "run_options": (list, OPTIONAL),

            # image for head node, takes precedence over "image" if specified
            "head_image": (str, OPTIONAL),
            # head specific run options, appended to run_options
            "head_run_options": (list, OPTIONAL),
            # analogous to head_image
            "worker_image": (str, OPTIONAL),
            # analogous to head_run_options
            "worker_run_options": (list, OPTIONAL),
        },
        OPTIONAL),

    # Provider-specific config for the head node, e.g. instance type.
    "head_node": (dict, OPTIONAL),

    # Provider-specific config for worker nodes. e.g. instance type.
    "worker_nodes": (dict, OPTIONAL),

    # Map of remote paths to local paths, e.g. {"/tmp/data": "/my/local/data"}
    "file_mounts": (dict, OPTIONAL),

    # Whether to avoid restarting the cluster during updates. This field is
    # controlled by the ray --no-restart flag and cannot be set by the user.
    "no_restart": (None, OPTIONAL),
}


def make_command_schema(suffix):
    """Create a schema for commands, ending with `suffix`.

    Args:
        suffix: (str) end of keys in the scehma.

    Returns:
         A dict with keys `suffix`, `"head_" + suffix`, `"worker" + suffix`,
         each with schema `(list, OPTIONAL)`. Semantically, the `suffix` key
         is for commands common to all nodes, and are run first; the other
         keys are for commands specific to head and worker nodes.
    """
    return {
        suffix: (list, OPTIONAL),
        "head_" + suffix: (list, OPTIONAL),
        "worker_" + suffix: (list, OPTIONAL),
    }


# Commands that will be run when the cluster is first created.
NODE_CREATION_COMMANDS = "node_creation_commands"
# Commands that will be run whenever Ray is (re)started.
RAY_START_COMMAND = "start_ray_commands"

for suffix in [
        NODE_CREATION_COMMANDS,
        RAY_START_COMMAND,
        # TODO: remove deprecated "setup_commands" in 0.8.x release
        "setup_commands"
]:
    CLUSTER_CONFIG_SCHEMA.update(make_command_schema(suffix))


def get_commands(config, key, is_head=False):
    """Get commands for a head or worker node under key `key`.

    This appends `"common"` commands, that should run on head and worker,
    to the head/worker specific config.
    """
    kind_specific = "head" if is_head else "worker"
    kind_specific_key = kind_specific + "_" + key
    # TODO: remove backward compatibility code in 0.8.x release
    if key == "node_creation_commands":
        no_creation = key not in config and kind_specific_key not in config
        if no_creation:
            res = get_commands(config, "setup_commands", is_head=is_head)
            if res:
                msg = ("Using deprecated config parameter 'setup_commands'; "
                       "update your config to use 'node_creation_commands'.")
                warnings.warn(msg, DeprecationWarning)
            return res
    return config.get(kind_specific + "_" + key, []) + config.get(key, [])
