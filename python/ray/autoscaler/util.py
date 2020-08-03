import collections
import hashlib
import json
import jsonschema
import os
import threading
from typing import Any, Dict

import ray
import ray.services as services
from ray.autoscaler.node_provider import get_default_config
from ray.autoscaler.docker import dockerize_if_needed

REQUIRED, OPTIONAL = True, False
RAY_SCHEMA_PATH = os.path.join(
    os.path.dirname(ray.autoscaler.__file__), "ray-schema.json")

# Internal kv keys for storing debug status.
DEBUG_AUTOSCALING_ERROR = "__autoscaling_error"
DEBUG_AUTOSCALING_STATUS = "__autoscaling_status"


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
        raise jsonschema.ValidationError(message=e.message) from None


def prepare_config(config):
    with_defaults = fillout_defaults(config)
    merge_setup_commands(with_defaults)
    dockerize_if_needed(with_defaults)
    return with_defaults


def fillout_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    defaults = get_default_config(config["provider"])
    defaults.update(config)
    defaults["auth"] = defaults.get("auth", {})
    return defaults


def merge_setup_commands(config):
    config["head_setup_commands"] = (
        config["setup_commands"] + config["head_setup_commands"])
    config["worker_setup_commands"] = (
        config["setup_commands"] + config["worker_setup_commands"])
    return config


def with_head_node_ip(cmds):
    head_ip = services.get_node_ip_address()
    out = []
    for cmd in cmds:
        out.append("export RAY_HEAD_IP={}; {}".format(head_ip, cmd))
    return out


def hash_launch_conf(node_conf, auth):
    hasher = hashlib.sha1()
    hasher.update(
        json.dumps([node_conf, auth], sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


# Cache the file hashes to avoid rescanning it each time. Also, this avoids
# inadvertently restarting workers if the file mount content is mutated on the
# head node.
_hash_cache = {}


def hash_runtime_conf(file_mounts,
                      extra_objs,
                      generate_file_mounts_contents_hash=False):
    """Returns two hashes, a runtime hash and file_mounts_content hash.

    The runtime hash is used to determine if the configuration or file_mounts
    contents have changed. It is used at launch time (ray up) to determine if
    a restart is needed.

    The file_mounts_content hash is used to determine if the file_mounts
    contents have changed. It is used at monitor time to determine if
    additional file syncing is needed.
    """
    runtime_hasher = hashlib.sha1()
    contents_hasher = hashlib.sha1()

    def add_content_hashes(path):
        def add_hash_of_file(fpath):
            with open(fpath, "rb") as f:
                for chunk in iter(lambda: f.read(2**20), b""):
                    contents_hasher.update(chunk)

        path = os.path.expanduser(path)
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
        contents_hash = contents_hasher.hexdigest()

        # Generate a new runtime_hash if its not cached
        if conf_str not in _hash_cache:
            runtime_hasher.update(conf_str)
            runtime_hasher.update(contents_hash.encode("utf-8"))
            _hash_cache[conf_str] = runtime_hasher.hexdigest()

    else:
        contents_hash = None

    return (_hash_cache[conf_str], contents_hash)
