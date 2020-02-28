import copy
import logging
import threading
import time
from collections import defaultdict
from threading import Thread

import numpy as np
import ray
import psutil

logger = logging.getLogger(__name__)

try:
    import GPUtil
except ImportError:
    GPUtil = None

_pinned_objects = []
PINNED_OBJECT_PREFIX = "ray.tune.PinnedObject:"
START_OF_TIME = time.time()


class UtilMonitor(Thread):
    """Class for system usage utilization monitoring.

    It keeps track of CPU, RAM, GPU, VRAM usage (each gpu separately) by
    pinging for information every x seconds in a separate thread.

    Requires psutil and GPUtil to be installed. Can be enabled with
    tune.run(config={"log_sys_usage": True}).
    """

    def __init__(self, start=True, delay=0.7):
        self.stopped = True
        if GPUtil is None and start:
            logger.warning("Install gputil for GPU system monitoring.")

        if psutil is None and start:
            logger.warning("Install psutil to monitor system performance.")

        if GPUtil is None and psutil is None:
            return

        super(UtilMonitor, self).__init__()
        self.delay = delay  # Time between calls to GPUtil
        self.values = defaultdict(list)
        self.lock = threading.Lock()
        self.daemon = True
        if start:
            self.start()

    def _read_utilization(self):
        with self.lock:
            if psutil is not None:
                self.values["cpu_util_percent"].append(
                    float(psutil.cpu_percent(interval=None)))
                self.values["ram_util_percent"].append(
                    float(getattr(psutil.virtual_memory(), "percent")))
            if GPUtil is not None:
                gpu_list = []
                try:
                    gpu_list = GPUtil.getGPUs()
                except Exception:
                    logger.debug("GPUtil failed to retrieve GPUs.")
                for gpu in gpu_list:
                    self.values["gpu_util_percent" + str(gpu.id)].append(
                        float(gpu.load))
                    self.values["vram_util_percent" + str(gpu.id)].append(
                        float(gpu.memoryUtil))

    def get_data(self):
        if self.stopped:
            return {}

        with self.lock:
            ret_values = copy.deepcopy(self.values)
            for key, val in self.values.items():
                del val[:]
        return {
            "perf": {
                k: np.mean(v)
                for k, v in ret_values.items() if len(v) > 0
            }
        }

    def run(self):
        self.stopped = False
        while not self.stopped:
            self._read_utilization()
            time.sleep(self.delay)

    def stop(self):
        self.stopped = True


def pin_in_object_store(obj):
    """Deprecated, use ray.put(value, weakref=False) instead."""

    obj_id = ray.put(obj, weakref=False)
    _pinned_objects.append(obj_id)
    return obj_id


def get_pinned_object(pinned_id):
    """Deprecated."""

    return ray.get(pinned_id)


class warn_if_slow:
    """Prints a warning if a given operation is slower than 100ms.

    Example:
        >>> with warn_if_slow("some_operation"):
        ...    ray.get(something)
    """

    DEFAULT_THRESHOLD = 0.5

    def __init__(self, name, threshold=None):
        self.name = name
        self.threshold = threshold or self.DEFAULT_THRESHOLD
        self.too_slow = False

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        now = time.time()
        if now - self.start > self.threshold and now - START_OF_TIME > 60.0:
            self.too_slow = True
            logger.warning(
                "The `%s` operation took %s seconds to complete, "
                "which may be a performance bottleneck.", self.name,
                now - self.start)


def merge_dicts(d1, d2):
    """
    Args:
        d1 (dict): Dict 1.
        d2 (dict): Dict 2.

    Returns:
         dict: A new dict that is d1 and d2 deep merged.
    """
    merged = copy.deepcopy(d1)
    deep_update(merged, d2, True, [])
    return merged


def deep_update(original,
                new_dict,
                new_keys_allowed=False,
                whitelist=None,
                override_all_if_type_changes=None):
    """Updates original dict with values from new_dict recursively.

    If new key is introduced in new_dict, then if new_keys_allowed is not
    True, an error will be thrown. Further, for sub-dicts, if the key is
    in the whitelist, then new subkeys can be introduced.

    Args:
        original (dict): Dictionary with default values.
        new_dict (dict): Dictionary with values to be updated
        new_keys_allowed (bool): Whether new keys are allowed.
        whitelist (Optional[List[str]]): List of keys that correspond to dict
            values where new subkeys can be introduced. This is only at the top
            level.
        override_all_if_type_changes(Optional[List[str]]): List of top level
            keys with value=dict, for which we always simply override the
            entire value (dict), iff the "type" key in that value dict changes.
    """
    whitelist = whitelist or []
    override_all_if_type_changes = override_all_if_type_changes or []

    for k, value in new_dict.items():
        if k not in original and not new_keys_allowed:
            raise Exception("Unknown config parameter `{}` ".format(k))

        # Both orginal value and new one are dicts.
        if isinstance(original.get(k), dict) and isinstance(value, dict):
            # Check old type vs old one. If different, override entire value.
            if k in override_all_if_type_changes and \
                    "type" in value and "type" in original[k] and \
                    value["type"] != original[k]["type"]:
                original[k] = value
            # Whitelisted key -> ok to add new subkeys.
            elif k in whitelist:
                deep_update(original[k], value, True)
            # Non-whitelisted key.
            else:
                deep_update(original[k], value, new_keys_allowed)
        # Original value not a dict OR new value not a dict:
        # Override entire value.
        else:
            original[k] = value
    return original


def flatten_dict(dt, delimiter="/"):
    dt = copy.deepcopy(dt)
    while any(isinstance(v, dict) for v in dt.values()):
        remove = []
        add = {}
        for key, value in dt.items():
            if isinstance(value, dict):
                for subkey, v in value.items():
                    add[delimiter.join([key, subkey])] = v
                remove.append(key)
        dt.update(add)
        for k in remove:
            del dt[k]
    return dt


def _to_pinnable(obj):
    """Converts obj to a form that can be pinned in object store memory.

    Currently only numpy arrays are pinned in memory, if you have a strong
    reference to the array value.
    """

    return (obj, np.zeros(1))


def _from_pinnable(obj):
    """Retrieve from _to_pinnable format."""

    return obj[0]


def validate_save_restore(trainable_cls,
                          config=None,
                          num_gpus=0,
                          use_object_store=False):
    """Helper method to check if your Trainable class will resume correctly.

    Args:
        trainable_cls: Trainable class for evaluation.
        config (dict): Config to pass to Trainable when testing.
        num_gpus (int): GPU resources to allocate when testing.
        use_object_store (bool): Whether to save and restore to Ray's object
            store. Recommended to set this to True if planning to use
            algorithms that pause training (i.e., PBT, HyperBand).
    """
    assert ray.is_initialized(), "Need Ray to be initialized."
    remote_cls = ray.remote(num_gpus=num_gpus)(trainable_cls)
    trainable_1 = remote_cls.remote(config=config)
    trainable_2 = remote_cls.remote(config=config)

    from ray.tune.result import TRAINING_ITERATION

    for _ in range(3):
        res = ray.get(trainable_1.train.remote())

    assert res.get(TRAINING_ITERATION), (
        "Validation will not pass because it requires `training_iteration` "
        "to be returned.")

    if use_object_store:
        restore_check = trainable_2.restore_from_object.remote(
            trainable_1.save_to_object.remote())
        ray.get(restore_check)
    else:
        restore_check = ray.get(
            trainable_2.restore.remote(trainable_1.save.remote()))

    res = ray.get(trainable_2.train.remote())
    assert res[TRAINING_ITERATION] == 4

    res = ray.get(trainable_2.train.remote())
    assert res[TRAINING_ITERATION] == 5
    return True


if __name__ == "__main__":
    ray.init()
    X = pin_in_object_store("hello")
    print(X)
    result = get_pinned_object(X)
    print(result)
