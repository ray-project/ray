from typing import Dict, List, Union
import copy
import json
import glob
import logging
import numbers
import os
import inspect
import threading
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from datetime import datetime
from threading import Thread
from typing import Optional

import numpy as np
import ray
import psutil

logger = logging.getLogger(__name__)


def _import_gputil():
    try:
        import GPUtil
    except ImportError:
        GPUtil = None
    return GPUtil


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
        GPUtil = _import_gputil()
        self.GPUtil = GPUtil
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
            if self.GPUtil is not None:
                gpu_list = []
                try:
                    gpu_list = self.GPUtil.getGPUs()
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
    """Deprecated, use ray.put(value) instead."""

    obj_ref = ray.put(obj)
    _pinned_objects.append(obj_ref)
    return obj_ref


def get_pinned_object(pinned_id):
    """Deprecated."""

    return ray.get(pinned_id)


class warn_if_slow:
    """Prints a warning if a given operation is slower than 500ms.

    Example:
        >>> with warn_if_slow("some_operation"):
        ...    ray.get(something)
    """

    DEFAULT_THRESHOLD = float(os.environ.get("TUNE_WARN_THRESHOLD_S", 0.5))
    DEFAULT_MESSAGE = "The `{name}` operation took {duration:.3f} s, " \
                      "which may be a performance bottleneck."

    def __init__(self,
                 name: str,
                 threshold: Optional[float] = None,
                 message: Optional[str] = None,
                 disable: bool = False):
        self.name = name
        self.threshold = threshold or self.DEFAULT_THRESHOLD
        self.message = message or self.DEFAULT_MESSAGE
        self.too_slow = False
        self.disable = disable

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        now = time.time()
        if self.disable:
            return
        if now - self.start > self.threshold and now - START_OF_TIME > 60.0:
            self.too_slow = True
            duration = now - self.start
            logger.warning(
                self.message.format(name=self.name, duration=duration))


class Tee(object):
    def __init__(self, stream1, stream2):
        self.stream1 = stream1
        self.stream2 = stream2

    def write(self, *args, **kwargs):
        self.stream1.write(*args, **kwargs)
        self.stream2.write(*args, **kwargs)

    def flush(self, *args, **kwargs):
        self.stream1.flush(*args, **kwargs)
        self.stream2.flush(*args, **kwargs)


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


def is_nan_or_inf(value):
    return np.isnan(value) or np.isinf(value)


def env_integer(key, default):
    # TODO(rliaw): move into ray.constants
    if key in os.environ:
        value = os.environ[key]
        if value.isdigit():
            return int(os.environ[key])
        raise ValueError(f"Found {key} in environment, but value must "
                         f"be an integer. Got: {value}.")
    return default


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
                allow_new_subkey_list=None,
                override_all_if_type_changes=None):
    """Updates original dict with values from new_dict recursively.

    If new key is introduced in new_dict, then if new_keys_allowed is not
    True, an error will be thrown. Further, for sub-dicts, if the key is
    in the allow_new_subkey_list, then new subkeys can be introduced.

    Args:
        original (dict): Dictionary with default values.
        new_dict (dict): Dictionary with values to be updated
        new_keys_allowed (bool): Whether new keys are allowed.
        allow_new_subkey_list (Optional[List[str]]): List of keys that
            correspond to dict values where new subkeys can be introduced.
            This is only at the top level.
        override_all_if_type_changes(Optional[List[str]]): List of top level
            keys with value=dict, for which we always simply override the
            entire value (dict), iff the "type" key in that value dict changes.
    """
    allow_new_subkey_list = allow_new_subkey_list or []
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
            # Allowed key -> ok to add new subkeys.
            elif k in allow_new_subkey_list:
                deep_update(original[k], value, True)
            # Non-allowed key.
            else:
                deep_update(original[k], value, new_keys_allowed)
        # Original value not a dict OR new value not a dict:
        # Override entire value.
        else:
            original[k] = value
    return original


def flatten_dict(dt, delimiter="/", prevent_delimiter=False):
    dt = copy.deepcopy(dt)
    if prevent_delimiter and any(delimiter in key for key in dt):
        # Raise if delimiter is any of the keys
        raise ValueError(
            "Found delimiter `{}` in key when trying to flatten array."
            "Please avoid using the delimiter in your specification.")
    while any(isinstance(v, dict) for v in dt.values()):
        remove = []
        add = {}
        for key, value in dt.items():
            if isinstance(value, dict):
                for subkey, v in value.items():
                    if prevent_delimiter and delimiter in subkey:
                        # Raise  if delimiter is in any of the subkeys
                        raise ValueError(
                            "Found delimiter `{}` in key when trying to "
                            "flatten array. Please avoid using the delimiter "
                            "in your specification.")
                    add[delimiter.join([key, str(subkey)])] = v
                remove.append(key)
        dt.update(add)
        for k in remove:
            del dt[k]
    return dt


def unflatten_dict(dt, delimiter="/"):
    """Unflatten dict. Does not support unflattening lists."""
    dict_type = type(dt)
    out = dict_type()
    for key, val in dt.items():
        path = key.split(delimiter)
        item = out
        for k in path[:-1]:
            item = item.setdefault(k, dict_type())
        item[path[-1]] = val
    return out


def unflatten_list_dict(dt, delimiter="/"):
    """Unflatten nested dict and list.

    This function now has some limitations:
    (1) The keys of dt must be str.
    (2) If unflattened dt (the result) contains list, the index order must be
        ascending when accessing dt. Otherwise, this function will throw
        AssertionError.
    (3) The unflattened dt (the result) shouldn't contain dict with number
        keys.

    Be careful to use this function. If you want to improve this function,
    please also improve the unit test. See #14487 for more details.

    Args:
        dt (dict): Flattened dictionary that is originally nested by multiple
            list and dict.
        delimiter (str): Delimiter of keys.

    Example:
        >>> dt = {"aaa/0/bb": 12, "aaa/1/cc": 56, "aaa/1/dd": 92}
        >>> unflatten_list_dict(dt)
        {'aaa': [{'bb': 12}, {'cc': 56, 'dd': 92}]}
    """
    out_type = list if list(dt)[0].split(delimiter, 1)[0].isdigit() \
        else type(dt)
    out = out_type()
    for key, val in dt.items():
        path = key.split(delimiter)

        item = out
        for i, k in enumerate(path[:-1]):
            next_type = list if path[i + 1].isdigit() else dict
            if isinstance(item, dict):
                item = item.setdefault(k, next_type())
            elif isinstance(item, list):
                if int(k) >= len(item):
                    item.append(next_type())
                    assert int(k) == len(item) - 1
                item = item[int(k)]

        if isinstance(item, dict):
            item[path[-1]] = val
        elif isinstance(item, list):
            item.append(val)
            assert int(path[-1]) == len(item) - 1
    return out


def unflattened_lookup(flat_key, lookup, delimiter="/", **kwargs):
    """
    Unflatten `flat_key` and iteratively look up in `lookup`. E.g.
    `flat_key="a/0/b"` will try to return `lookup["a"][0]["b"]`.
    """
    if flat_key in lookup:
        return lookup[flat_key]
    keys = deque(flat_key.split(delimiter))
    base = lookup
    while keys:
        key = keys.popleft()
        try:
            if isinstance(base, Mapping):
                base = base[key]
            elif isinstance(base, Sequence):
                base = base[int(key)]
            else:
                raise KeyError()
        except KeyError as e:
            if "default" in kwargs:
                return kwargs["default"]
            raise e
    return base


def _to_pinnable(obj):
    """Converts obj to a form that can be pinned in object store memory.

    Currently only numpy arrays are pinned in memory, if you have a strong
    reference to the array value.
    """

    return (obj, np.zeros(1))


def _from_pinnable(obj):
    """Retrieve from _to_pinnable format."""

    return obj[0]


def diagnose_serialization(trainable):
    """Utility for detecting why your trainable function isn't serializing.

    Args:
        trainable (func): The trainable object passed to
            tune.run(trainable). Currently only supports
            Function API.

    Returns:
        bool | set of unserializable objects.

    Example:

    .. code-block:: python

        import threading
        # this is not serializable
        e = threading.Event()

        def test():
            print(e)

        diagnose_serialization(test)
        # should help identify that 'e' should be moved into
        # the `test` scope.

        # correct implementation
        def test():
            e = threading.Event()
            print(e)

        assert diagnose_serialization(test) is True

    """
    from ray.tune.registry import register_trainable, check_serializability

    def check_variables(objects, failure_set, printer):
        for var_name, variable in objects.items():
            msg = None
            try:
                check_serializability(var_name, variable)
                status = "PASSED"
            except Exception as e:
                status = "FAILED"
                msg = f"{e.__class__.__name__}: {str(e)}"
                failure_set.add(var_name)
            printer(f"{str(variable)}[name='{var_name}'']... {status}")
            if msg:
                printer(msg)

    print(f"Trying to serialize {trainable}...")
    try:
        register_trainable("__test:" + str(trainable), trainable, warn=False)
        print("Serialization succeeded!")
        return True
    except Exception as e:
        print(f"Serialization failed: {e}")

    print("Inspecting the scope of the trainable by running "
          f"`inspect.getclosurevars({str(trainable)})`...")
    closure = inspect.getclosurevars(trainable)
    failure_set = set()
    if closure.globals:
        print(f"Detected {len(closure.globals)} global variables. "
              "Checking serializability...")
        check_variables(closure.globals, failure_set,
                        lambda s: print("   " + s))

    if closure.nonlocals:
        print(f"Detected {len(closure.nonlocals)} nonlocal variables. "
              "Checking serializability...")
        check_variables(closure.nonlocals, failure_set,
                        lambda s: print("   " + s))

    if not failure_set:
        print("Nothing was found to have failed the diagnostic test, though "
              "serialization did not succeed. Feel free to raise an "
              "issue on github.")
        return failure_set
    else:
        print(f"Variable(s) {failure_set} was found to be non-serializable. "
              "Consider either removing the instantiation/imports "
              "of these objects or moving them into the scope of "
              "the trainable. ")
        return failure_set


def atomic_save(state: Dict, checkpoint_dir: str, file_name: str,
                tmp_file_name: str):
    """Atomically saves the state object to the checkpoint directory.

    This is automatically used by tune.run during a Tune job.

    Args:
        state (dict): Object state to be serialized.
        checkpoint_dir (str): Directory location for the checkpoint.
        file_name (str): Final name of file.
        tmp_file_name (str): Temporary name of file.
    """
    import ray.cloudpickle as cloudpickle
    tmp_search_ckpt_path = os.path.join(checkpoint_dir, tmp_file_name)
    with open(tmp_search_ckpt_path, "wb") as f:
        cloudpickle.dump(state, f)

    os.replace(tmp_search_ckpt_path, os.path.join(checkpoint_dir, file_name))


def load_newest_checkpoint(dirpath: str, ckpt_pattern: str) -> dict:
    """Returns the most recently modified checkpoint.

    Assumes files are saved with an ordered name, most likely by
    :obj:atomic_save.

    Args:
        dirpath (str): Directory in which to look for the checkpoint file.
        ckpt_pattern (str): File name pattern to match to find checkpoint
            files.

    Returns:
        (dict) Deserialized state dict.
    """
    import ray.cloudpickle as cloudpickle
    full_paths = glob.glob(os.path.join(dirpath, ckpt_pattern))
    if not full_paths:
        return
    most_recent_checkpoint = max(full_paths)
    with open(most_recent_checkpoint, "rb") as f:
        checkpoint_state = cloudpickle.load(f)
    return checkpoint_state


def wait_for_gpu(gpu_id=None,
                 target_util=0.01,
                 retry=20,
                 delay_s=5,
                 gpu_memory_limit=None):
    """Checks if a given GPU has freed memory.

    Requires ``gputil`` to be installed: ``pip install gputil``.

    Args:
        gpu_id (Optional[Union[int, str]]): GPU id or uuid to check.
            Must be found within GPUtil.getGPUs(). If none, resorts to
            the first item returned from `ray.get_gpu_ids()`.
        target_util (float): The utilization threshold to reach to unblock.
            Set this to 0 to block until the GPU is completely free.
        retry (int): Number of times to check GPU limit. Sleeps `delay_s`
            seconds between checks.
        delay_s (int): Seconds to wait before check.
        gpu_memory_limit (float): Deprecated.

    Returns:
        bool: True if free.

    Raises:
        RuntimeError: If GPUtil is not found, if no GPUs are detected
            or if the check fails.

    Example:

    .. code-block:: python

        def tune_func(config):
            tune.util.wait_for_gpu()
            train()

        tune.run(tune_func, resources_per_trial={"GPU": 1}, num_samples=10)
    """
    GPUtil = _import_gputil()
    if gpu_memory_limit:
        raise ValueError("'gpu_memory_limit' is deprecated. "
                         "Use 'target_util' instead.")
    if GPUtil is None:
        raise RuntimeError(
            "GPUtil must be installed if calling `wait_for_gpu`.")

    if gpu_id is None:
        gpu_id_list = ray.get_gpu_ids()
        if not gpu_id_list:
            raise RuntimeError("No GPU ids found from `ray.get_gpu_ids()`. "
                               "Did you set Tune resources correctly?")
        gpu_id = gpu_id_list[0]

    gpu_attr = "id"
    if isinstance(gpu_id, str):
        if gpu_id.isdigit():
            # GPU ID returned from `ray.get_gpu_ids()` is a str representation
            # of the int GPU ID
            gpu_id = int(gpu_id)
        else:
            # Could not coerce gpu_id to int, so assume UUID
            # and compare against `uuid` attribute e.g.,
            # 'GPU-04546190-b68d-65ac-101b-035f8faed77d'
            gpu_attr = "uuid"
    elif not isinstance(gpu_id, int):
        raise ValueError(f"gpu_id ({type(gpu_id)}) must be type str/int.")

    def gpu_id_fn(g):
        # Returns either `g.id` or `g.uuid` depending on
        # the format of the input `gpu_id`
        return getattr(g, gpu_attr)

    gpu_ids = {gpu_id_fn(g) for g in GPUtil.getGPUs()}
    if gpu_id not in gpu_ids:
        raise ValueError(
            f"{gpu_id} not found in set of available GPUs: {gpu_ids}. "
            "`wait_for_gpu` takes either GPU ordinal ID (e.g., '0') or "
            "UUID (e.g., 'GPU-04546190-b68d-65ac-101b-035f8faed77d').")

    for i in range(int(retry)):
        gpu_object = next(
            g for g in GPUtil.getGPUs() if gpu_id_fn(g) == gpu_id)
        if gpu_object.memoryUtil > target_util:
            logger.info(f"Waiting for GPU util to reach {target_util}. "
                        f"Util: {gpu_object.memoryUtil:0.3f}")
            time.sleep(delay_s)
        else:
            return True
    raise RuntimeError("GPU memory was not freed.")


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


def detect_checkpoint_function(train_func, abort=False, partial=False):
    """Use checkpointing if any arg has "checkpoint_dir" and args = 2"""
    func_sig = inspect.signature(train_func)
    validated = True
    try:
        # check if signature is func(config, checkpoint_dir=None)
        if partial:
            func_sig.bind_partial({}, checkpoint_dir="tmp/path")
        else:
            func_sig.bind({}, checkpoint_dir="tmp/path")
    except Exception as e:
        logger.debug(str(e))
        validated = False
    if abort and not validated:
        func_args = inspect.getfullargspec(train_func).args
        raise ValueError(
            "Provided training function must have 2 args "
            "in the signature, and the latter arg must "
            "contain `checkpoint_dir`. For example: "
            "`func(config, checkpoint_dir=None)`. Got {}".format(func_args))
    return validated


def detect_reporter(func):
    """Use reporter if any arg has "reporter" and args = 2"""
    func_sig = inspect.signature(func)
    use_reporter = True
    try:
        func_sig.bind({}, reporter=None)
    except Exception as e:
        logger.debug(str(e))
        use_reporter = False
    return use_reporter


def detect_config_single(func):
    """Check if func({}) works."""
    func_sig = inspect.signature(func)
    use_config_single = True
    try:
        func_sig.bind({})
    except Exception as e:
        logger.debug(str(e))
        use_config_single = False
    return use_config_single


def create_logdir(dirname: str, local_dir: str):
    """Create an empty logdir with name `dirname` in `local_dir`.

    If `local_dir`/`dirname` already exists, a unique string is appended
    to the dirname.

    Args:
        dirname (str): Dirname to create in `local_dir`
        local_dir (str): Root directory for the log dir

    Returns:
        full path to the newly created logdir.
    """
    local_dir = os.path.expanduser(local_dir)
    logdir = os.path.join(local_dir, dirname)
    if os.path.exists(logdir):
        old_dirname = dirname
        dirname += "_" + uuid.uuid4().hex[:4]
        logger.info(f"Creating a new dirname {dirname} because "
                    f"trial dirname '{old_dirname}' already exists.")
        logdir = os.path.join(local_dir, dirname)
    os.makedirs(logdir, exist_ok=True)
    return logdir


def validate_warmstart(parameter_names: List[str],
                       points_to_evaluate: List[Union[List, Dict]],
                       evaluated_rewards: List):
    """Generic validation of a Searcher's warm start functionality.
    Raises exceptions in case of type and length mismatches betwee
    parameters.
    """
    if points_to_evaluate:
        if not isinstance(points_to_evaluate, list):
            raise TypeError(
                "points_to_evaluate expected to be a list, got {}.".format(
                    type(points_to_evaluate)))
        for point in points_to_evaluate:
            if not isinstance(point, (dict, list)):
                raise TypeError(
                    f"points_to_evaluate expected to include list or dict, "
                    f"got {point}.")

            if not len(point) == len(parameter_names):
                raise ValueError("Dim of point {}".format(point) +
                                 " and parameter_names {}".format(
                                     parameter_names) + " do not match.")

    if points_to_evaluate and evaluated_rewards:
        if not isinstance(evaluated_rewards, list):
            raise TypeError(
                "evaluated_rewards expected to be a list, got {}.".format(
                    type(evaluated_rewards)))
        if not len(evaluated_rewards) == len(points_to_evaluate):
            raise ValueError(
                "Dim of evaluated_rewards {}".format(evaluated_rewards) +
                " and points_to_evaluate {}".format(points_to_evaluate) +
                " do not match.")


class SafeFallbackEncoder(json.JSONEncoder):
    def __init__(self, nan_str="null", **kwargs):
        super(SafeFallbackEncoder, self).__init__(**kwargs)
        self.nan_str = nan_str

    def default(self, value):
        try:
            if np.isnan(value):
                return self.nan_str

            if (type(value).__module__ == np.__name__
                    and isinstance(value, np.ndarray)):
                return value.tolist()

            if issubclass(type(value), numbers.Integral):
                return int(value)
            if issubclass(type(value), numbers.Number):
                return float(value)

            return super(SafeFallbackEncoder, self).default(value)

        except Exception:
            return str(value)  # give up, just stringify it (ok for logs)


if __name__ == "__main__":
    ray.init()
    X = pin_in_object_store("hello")
    print(X)
    result = get_pinned_object(X)
    print(result)
