import collections
import logging
import hashlib
import json
import jsonschema
import os
import threading
from typing import Any, Dict

import ray
import ray._private.services as services
from ray.autoscaler._private.providers import _get_default_config
from ray.autoscaler._private.docker import validate_docker_config
from ray.autoscaler.tags import NODE_TYPE_LEGACY_WORKER, NODE_TYPE_LEGACY_HEAD

REQUIRED, OPTIONAL = True, False
RAY_SCHEMA_PATH = os.path.join(
    os.path.dirname(ray.autoscaler.__file__), "ray-schema.json")

# Internal kv keys for storing debug status.
DEBUG_AUTOSCALING_ERROR = "__autoscaling_error"
DEBUG_AUTOSCALING_STATUS = "__autoscaling_status"

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
        if "worker_default_node_type" not in config:
            raise ValueError("You must specify `worker_default_node_type` if "
                             "`available_node_types is set.")
        if (config["worker_default_node_type"] not in config[
                "available_node_types"]):
            raise ValueError("`worker_default_node_type` must be one of "
                             "`available_node_types`.")


def prepare_config(config):
    with_defaults = fillout_defaults(config)
    merge_setup_commands(with_defaults)
    validate_docker_config(with_defaults)
    return with_defaults


def rewrite_legacy_yaml_to_available_node_types(
        config: Dict[str, Any]) -> Dict[str, Any]:

    if "available_node_types" not in config:
        # TODO(ameer/ekl/alex): we can also rewrite here many other fields
        # that include initialization/setup/start commands and ImageId.
        logger.debug("Converting legacy cluster config to multi node types.")
        config["available_node_types"] = {
            NODE_TYPE_LEGACY_HEAD: {
                "node_config": config["head_node"],
                "resources": config["head_node"].get("resources") or {},
                "min_workers": 0,
                "max_workers": 0,
            },
            NODE_TYPE_LEGACY_WORKER: {
                "node_config": config["worker_nodes"],
                "resources": config["worker_nodes"].get("resources") or {},
                "min_workers": config.get("min_workers", 0),
                "max_workers": config.get("max_workers", 0),
            },
        }
        config["head_node_type"] = NODE_TYPE_LEGACY_HEAD
        config["worker_default_node_type"] = NODE_TYPE_LEGACY_WORKER

    return config


def fillout_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _get_default_config(config["provider"])
    defaults.update(config)
    defaults["auth"] = defaults.get("auth", {})
    defaults = rewrite_legacy_yaml_to_available_node_types(defaults)
    return defaults


def merge_setup_commands(config):
    config["head_setup_commands"] = (
        config["setup_commands"] + config["head_setup_commands"])
    config["worker_setup_commands"] = (
        config["setup_commands"] + config["worker_setup_commands"])
    return config


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
