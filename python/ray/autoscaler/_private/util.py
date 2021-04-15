import collections
import copy
from datetime import datetime
import logging
import hashlib
import json
import jsonschema
import os
import threading
from typing import Any, Dict, List

import ray
import ray.ray_constants
import ray._private.services as services
from ray.autoscaler._private import constants
from ray.autoscaler._private.providers import _get_default_config
from ray.autoscaler._private.docker import validate_docker_config
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler.tags import NODE_TYPE_LEGACY_WORKER, NODE_TYPE_LEGACY_HEAD

REQUIRED, OPTIONAL = True, False
RAY_SCHEMA_PATH = os.path.join(
    os.path.dirname(ray.autoscaler.__file__), "ray-schema.json")

# Internal kv keys for storing debug status.
DEBUG_AUTOSCALING_ERROR = "__autoscaling_error"
DEBUG_AUTOSCALING_STATUS = "__autoscaling_status"
DEBUG_AUTOSCALING_STATUS_LEGACY = "__autoscaling_status_legacy"

HEAD_TYPE_MAX_WORKERS_WARN_TEMPLATE = "Setting `max_workers` for node type"\
    " `{node_type}` to the global `max_workers` value of {max_workers}. To"\
    " avoid spawning worker nodes of type `{node_type}`, explicitly set" \
    " `max_workers: 0` for `{node_type}`.\n"\
    "Note that `max_workers: 0` was the default value prior to Ray 1.3.0."\
    " Your current version is Ray {version}.\n"\
    "See the docs for more information:\n"\
    "https://docs.ray.io/en/master/cluster/config.html"\
    "#cluster-configuration-node-max-workers\n"\
    "https://docs.ray.io/en/master/cluster/config.html#full-configuration"

logger = logging.getLogger(__name__)


class ConcurrentCounter:
    def __init__(self):
        self._lock = threading.RLock()
        self._counter = collections.defaultdict(int)

    def inc(self, key, count):
        with self._lock:
            self._counter[key] += count
            return self.value

    def dec(self, key, count):
        with self._lock:
            self._counter[key] -= count
            assert self._counter[key] >= 0, "counter cannot go negative"
            return self.value

    def breakdown(self):
        with self._lock:
            return dict(self._counter)

    @property
    def value(self):
        with self._lock:
            return sum(self._counter.values())


def validate_config(config: Dict[str, Any]) -> None:
    """Required Dicts indicate that no extra fields can be introduced."""
    if not isinstance(config, dict):
        raise ValueError("Config {} is not a dictionary".format(config))

    with open(RAY_SCHEMA_PATH) as f:
        schema = json.load(f)
    try:
        jsonschema.validate(config, schema)
    except jsonschema.ValidationError as e:
        raise e from None

    # Detect out of date defaults. This happens when the autoscaler that filled
    # out the default values is older than the version of the autoscaler that
    # is running on the cluster.
    if "cluster_synced_files" not in config:
        raise RuntimeError(
            "Missing 'cluster_synced_files' field in the cluster "
            "configuration. This is likely due to the Ray version running "
            "in the cluster {ray_version} is greater than the Ray version "
            "running on your laptop. Please try updating Ray on your local "
            "machine and make sure the versions match.".format(
                ray_version=ray.__version__))

    if "available_node_types" in config:
        if "head_node_type" not in config:
            raise ValueError(
                "You must specify `head_node_type` if `available_node_types "
                "is set.")
        if config["head_node_type"] not in config["available_node_types"]:
            raise ValueError(
                "`head_node_type` must be one of `available_node_types`.")

        sum_min_workers = sum(
            config["available_node_types"][node_type].get("min_workers", 0)
            for node_type in config["available_node_types"])
        if sum_min_workers > config["max_workers"]:
            raise ValueError(
                "The specified global `max_workers` is smaller than the "
                "sum of `min_workers` of all the available node types.")


def prepare_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    The returned config has the following properties:
    - Uses the multi-node-type autoscaler configuration.
    - Merged with the appropriate defaults.yaml
    - Has a valid Docker configuration if provided.
    - Has max_worker set for each node type.
    """
    with_defaults = fillout_defaults(config)
    merge_setup_commands(with_defaults)
    validate_docker_config(with_defaults)
    fill_node_type_max_workers(with_defaults)
    return with_defaults


def fillout_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _get_default_config(config["provider"])
    defaults.update(config)

    # Just for clarity:
    merged_config = copy.deepcopy(defaults)

    # Fill auth field to avoid key errors.
    # This field is accessed when calling NodeUpdater but is not relevant to
    # certain node providers and is thus left out of some cluster launching
    # configs.
    merged_config["auth"] = merged_config.get("auth", {})

    # A legacy config is one which doesn't have available_node_types,
    # but has at least one of head_node or worker_nodes.
    is_legacy_config = (("available_node_types" not in config) and
                        ("head_node" in config or "worker_nodes" in config))
    # Do merging logic for legacy configs.
    if is_legacy_config:
        merged_config = merge_legacy_yaml_with_defaults(merged_config)
    # Take care of this here, in case a config does not specify any of head,
    # workers, node types, but does specify min workers:
    merged_config.pop("min_workers", None)

    return merged_config


def merge_legacy_yaml_with_defaults(
        merged_config: Dict[str, Any]) -> Dict[str, Any]:
    """Rewrite legacy config's available node types after it has been merged
    with defaults yaml.
    """
    cli_logger.warning(
        "Converting legacy cluster config to a multi node type cluster "
        "config. Multi-node-type cluster configs are the recommended "
        "format for configuring Ray clusters. "
        "See the docs for more information:\n"
        "https://docs.ray.io/en/master/cluster/config.html#full-configuration")

    # Get default head and worker types.
    default_head_type = merged_config["head_node_type"]
    # Default configs are assumed to have two node types -- one for the head
    # and one for the workers.
    assert len(merged_config["available_node_types"].keys()) == 2
    default_worker_type = (merged_config["available_node_types"].keys() -
                           {default_head_type}).pop()

    if merged_config["head_node"]:
        # User specified a head node in legacy config.
        # Convert it into data for the head's node type.
        head_node_info = {
            "node_config": merged_config["head_node"],
            "resources": merged_config["head_node"].get("resources") or {},
            "min_workers": 0,
            "max_workers": 0,
        }
    else:
        # Use default data for the head's node type.
        head_node_info = merged_config["available_node_types"][
            default_head_type]
    if merged_config["worker_nodes"]:
        # User specified a worker node in legacy config.
        # Convert it into data for the workers' node type.
        worker_node_info = {
            "node_config": merged_config["worker_nodes"],
            "resources": merged_config["worker_nodes"].get("resources") or {},
            "min_workers": merged_config.get("min_workers", 0),
            "max_workers": merged_config["max_workers"],
        }
    else:
        # Use default data for the workers' node type.
        worker_node_info = merged_config["available_node_types"][
            default_worker_type]

    # Rewrite available_node_types.
    merged_config["available_node_types"] = {
        NODE_TYPE_LEGACY_HEAD: head_node_info,
        NODE_TYPE_LEGACY_WORKER: worker_node_info
    }
    merged_config["head_node_type"] = NODE_TYPE_LEGACY_HEAD

    # Resources field in head/worker fields cause node launch to fail.
    merged_config["head_node"].pop("resources", None)
    merged_config["worker_nodes"].pop("resources", None)

    return merged_config


def merge_setup_commands(config):
    config["head_setup_commands"] = (
        config["setup_commands"] + config["head_setup_commands"])
    config["worker_setup_commands"] = (
        config["setup_commands"] + config["worker_setup_commands"])
    return config


def fill_node_type_max_workers(config):
    """Sets default per-node max workers to global max_workers.
    This equivalent to setting the default per-node max workers to infinity,
    with the only upper constraint coming from the global max_workers.
    """
    assert "max_workers" in config, "Global max workers should be set."
    node_types = config["available_node_types"]
    for node_type_name in node_types:
        node_type_data = node_types[node_type_name]

        # Log a warning if head node type's max_workers is absent.
        if (node_type_name == config["head_node_type"]
                and "max_workers" not in node_type_data):
            cli_logger.warning(
                HEAD_TYPE_MAX_WORKERS_WARN_TEMPLATE.format(
                    node_type=node_type_name,
                    max_workers=config["max_workers"],
                    version=ray.__version__))

        # The key part of this function:
        node_type_data.setdefault("max_workers", config["max_workers"])


def with_head_node_ip(cmds, head_ip=None):
    if head_ip is None:
        head_ip = services.get_node_ip_address()
    out = []
    for cmd in cmds:
        out.append("export RAY_HEAD_IP={}; {}".format(head_ip, cmd))
    return out


def hash_launch_conf(node_conf, auth):
    hasher = hashlib.sha1()
    # For hashing, we replace the path to the key with the
    # key itself. This is to make sure the hashes are the
    # same even if keys live at different locations on different
    # machines.
    full_auth = auth.copy()
    for key_type in ["ssh_private_key", "ssh_public_key"]:
        if key_type in auth:
            with open(os.path.expanduser(auth[key_type])) as key:
                full_auth[key_type] = key.read()
    hasher.update(
        json.dumps([node_conf, full_auth], sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


# Cache the file hashes to avoid rescanning it each time. Also, this avoids
# inadvertently restarting workers if the file mount content is mutated on the
# head node.
_hash_cache = {}


def hash_runtime_conf(file_mounts,
                      cluster_synced_files,
                      extra_objs,
                      generate_file_mounts_contents_hash=False):
    """Returns two hashes, a runtime hash and file_mounts_content hash.

    The runtime hash is used to determine if the configuration or file_mounts
    contents have changed. It is used at launch time (ray up) to determine if
    a restart is needed.

    The file_mounts_content hash is used to determine if the file_mounts or
    cluster_synced_files contents have changed. It is used at monitor time to
    determine if additional file syncing is needed.
    """
    runtime_hasher = hashlib.sha1()
    contents_hasher = hashlib.sha1()

    def add_content_hashes(path, allow_non_existing_paths: bool = False):
        def add_hash_of_file(fpath):
            with open(fpath, "rb") as f:
                for chunk in iter(lambda: f.read(2**20), b""):
                    contents_hasher.update(chunk)

        path = os.path.expanduser(path)
        if allow_non_existing_paths and not os.path.exists(path):
            return
        if os.path.isdir(path):
            dirs = []
            for dirpath, _, filenames in os.walk(path):
                dirs.append((dirpath, sorted(filenames)))
            for dirpath, filenames in sorted(dirs):
                contents_hasher.update(dirpath.encode("utf-8"))
                for name in filenames:
                    contents_hasher.update(name.encode("utf-8"))
                    fpath = os.path.join(dirpath, name)
                    add_hash_of_file(fpath)
        else:
            add_hash_of_file(path)

    conf_str = (json.dumps(file_mounts, sort_keys=True).encode("utf-8") +
                json.dumps(extra_objs, sort_keys=True).encode("utf-8"))

    # Only generate a contents hash if generate_contents_hash is true or
    # if we need to generate the runtime_hash
    if conf_str not in _hash_cache or generate_file_mounts_contents_hash:
        for local_path in sorted(file_mounts.values()):
            add_content_hashes(local_path)
        head_node_contents_hash = contents_hasher.hexdigest()

        # Generate a new runtime_hash if its not cached
        # The runtime hash does not depend on the cluster_synced_files hash
        # because we do not want to restart nodes only if cluster_synced_files
        # contents have changed.
        if conf_str not in _hash_cache:
            runtime_hasher.update(conf_str)
            runtime_hasher.update(head_node_contents_hash.encode("utf-8"))
            _hash_cache[conf_str] = runtime_hasher.hexdigest()

        # Add cluster_synced_files to the file_mounts_content hash
        if cluster_synced_files is not None:
            for local_path in sorted(cluster_synced_files):
                # For cluster_synced_files, we let the path be non-existant
                # because its possible that the source directory gets set up
                # anytime over the life of the head node.
                add_content_hashes(local_path, allow_non_existing_paths=True)

        file_mounts_contents_hash = contents_hasher.hexdigest()

    else:
        file_mounts_contents_hash = None

    return (_hash_cache[conf_str], file_mounts_contents_hash)


def add_resources(dict1: Dict[str, float],
                  dict2: Dict[str, float]) -> Dict[str, float]:
    """Add the values in two dictionaries.

    Returns:
        dict: A new dictionary (inputs remain unmodified).
    """
    new_dict = dict1.copy()
    for k, v in dict2.items():
        new_dict[k] = v + new_dict.get(k, 0)
    return new_dict


def freq_of_dicts(dicts: List[Dict],
                  serializer=lambda d: frozenset(d.items()),
                  deserializer=dict):
    """Count a list of dictionaries (or unhashable types).

    This is somewhat annoying because mutable data structures aren't hashable,
    and set/dict keys must be hashable.

    Args:
        dicts (List[D]): A list of dictionaries to be counted.
        serializer (D -> S): A custom serailization function. The output type S
            must be hashable. The default serializer converts a dictionary into
            a frozenset of KV pairs.
        deserializer (S -> U): A custom deserialization function. See the
            serializer for information about type S. For dictionaries U := D.

    Returns:
        List[Tuple[U, int]]: Returns a list of tuples. Each entry in the list
            is a tuple containing a unique entry from `dicts` and its
            corresponding frequency count.
    """
    freqs = collections.Counter(map(lambda d: serializer(d), dicts))
    as_list = []
    for as_set, count in freqs.items():
        as_list.append((deserializer(as_set), count))
    return as_list


def add_prefix(info_string, prefix):
    """Prefixes each line of info_string, except the first, by prefix."""
    lines = info_string.split("\n")
    prefixed_lines = [lines[0]]
    for line in lines[1:]:
        prefixed_line = ":".join([prefix, line])
        prefixed_lines.append(prefixed_line)
    prefixed_info_string = "\n".join(prefixed_lines)
    return prefixed_info_string


def format_pg(pg):
    strategy = pg["strategy"]
    bundles = pg["bundles"]
    shape_strs = []
    for bundle, count in bundles:
        shape_strs.append(f"{bundle} * {count}")
    bundles_str = ", ".join(shape_strs)
    return f"{bundles_str} ({strategy})"


def get_usage_report(lm_summary) -> str:
    usage_lines = []
    for resource, (used, total) in sorted(lm_summary.usage.items()):
        if "node:" in resource:
            continue  # Skip the auto-added per-node "node:<ip>" resource.
        line = f" {used}/{total} {resource}"
        if resource in ["memory", "object_store_memory"]:
            to_GiB = 1 / 2**30
            used *= to_GiB
            total *= to_GiB
            line = f" {used:.2f}/{total:.3f} GiB {resource}"
        usage_lines.append(line)
    usage_report = "\n".join(usage_lines)
    return usage_report


def get_demand_report(lm_summary):
    demand_lines = []
    for bundle, count in lm_summary.resource_demand:
        line = f" {bundle}: {count}+ pending tasks/actors"
        demand_lines.append(line)
    for entry in lm_summary.pg_demand:
        pg, count = entry
        pg_str = format_pg(pg)
        line = f" {pg_str}: {count}+ pending placement groups"
        demand_lines.append(line)
    for bundle, count in lm_summary.request_demand:
        line = f" {bundle}: {count}+ from request_resources()"
        demand_lines.append(line)
    if len(demand_lines) > 0:
        demand_report = "\n".join(demand_lines)
    else:
        demand_report = " (no resource demands)"
    return demand_report


def format_info_string(lm_summary, autoscaler_summary, time=None):
    if time is None:
        time = datetime.now()
    header = "=" * 8 + f" Autoscaler status: {time} " + "=" * 8
    separator = "-" * len(header)
    available_node_report_lines = []
    for node_type, count in autoscaler_summary.active_nodes.items():
        line = f" {count} {node_type}"
        available_node_report_lines.append(line)
    available_node_report = "\n".join(available_node_report_lines)

    pending_lines = []
    for node_type, count in autoscaler_summary.pending_launches.items():
        line = f" {node_type}, {count} launching"
        pending_lines.append(line)
    for ip, node_type, status in autoscaler_summary.pending_nodes:
        line = f" {ip}: {node_type}, {status.lower()}"
        pending_lines.append(line)
    if pending_lines:
        pending_report = "\n".join(pending_lines)
    else:
        pending_report = " (no pending nodes)"

    failure_lines = []
    for ip, node_type in autoscaler_summary.failed_nodes:
        line = f" {ip}: {node_type}"
        failure_lines.append(line)
    failure_lines = failure_lines[:
                                  -constants.AUTOSCALER_MAX_FAILURES_DISPLAYED:
                                  -1]
    failure_report = "Recent failures:\n"
    if failure_lines:
        failure_report += "\n".join(failure_lines)
    else:
        failure_report += " (no failures)"

    usage_report = get_usage_report(lm_summary)
    demand_report = get_demand_report(lm_summary)

    formatted_output = f"""{header}
Node status
{separator}
Healthy:
{available_node_report}
Pending:
{pending_report}
{failure_report}

Resources
{separator}

Usage:
{usage_report}

Demands:
{demand_report}"""
    return formatted_output


def format_info_string_no_node_types(lm_summary, time=None):
    if time is None:
        time = datetime.now()
    header = "=" * 8 + f" Cluster status: {time} " + "=" * 8
    separator = "-" * len(header)

    node_lines = []
    for node_type, count in lm_summary.node_types:
        line = f" {count} node(s) with resources: {node_type}"
        node_lines.append(line)
    node_report = "\n".join(node_lines)

    usage_report = get_usage_report(lm_summary)
    demand_report = get_demand_report(lm_summary)

    formatted_output = f"""{header}
Node status
{separator}
{node_report}

Resources
{separator}
Usage:
{usage_report}

Demands:
{demand_report}"""
    return formatted_output
