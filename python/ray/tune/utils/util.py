from typing import Dict, List, Union, Callable, Type
import copy
import glob
import logging
import os
import inspect
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime
from threading import Thread
from typing import Optional

import numpy as np
import ray
import psutil

from ray.util.ml_utils.json import SafeFallbackEncoder  # noqa
from ray.util.ml_utils.dict import (  # noqa: F401
    merge_dicts,
    deep_update,
    flatten_dict,
    unflatten_dict,
    unflatten_list_dict,
    unflattened_lookup,
)

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
                    float(psutil.cpu_percent(interval=None))
                )
                self.values["ram_util_percent"].append(
                    float(getattr(psutil.virtual_memory(), "percent"))
                )
            if self.GPUtil is not None:
                gpu_list = []
                try:
                    gpu_list = self.GPUtil.getGPUs()
                except Exception:
                    logger.debug("GPUtil failed to retrieve GPUs.")
                for gpu in gpu_list:
                    self.values["gpu_util_percent" + str(gpu.id)].append(
                        float(gpu.load)
                    )
                    self.values["vram_util_percent" + str(gpu.id)].append(
                        float(gpu.memoryUtil)
                    )

    def get_data(self):
        if self.stopped:
            return {}

        with self.lock:
            ret_values = copy.deepcopy(self.values)
            for key, val in self.values.items():
                del val[:]
        return {"perf": {k: np.mean(v) for k, v in ret_values.items() if len(v) > 0}}

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
    DEFAULT_MESSAGE = (
        "The `{name}` operation took {duration:.3f} s, "
        "which may be a performance bottleneck."
    )

    def __init__(
        self,
        name: str,
        threshold: Optional[float] = None,
        message: Optional[str] = None,
        disable: bool = False,
    ):
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
            logger.warning(self.message.format(name=self.name, duration=duration))


class Tee(object):
    def __init__(self, stream1, stream2):
        self.stream1 = stream1
        self.stream2 = stream2

    def seek(self, *args, **kwargs):
        self.stream1.seek(*args, **kwargs)
        self.stream2.seek(*args, **kwargs)

    def write(self, *args, **kwargs):
        self.stream1.write(*args, **kwargs)
        self.stream2.write(*args, **kwargs)

    def flush(self, *args, **kwargs):
        self.stream1.flush(*args, **kwargs)
        self.stream2.flush(*args, **kwargs)

    @property
    def encoding(self):
        if hasattr(self.stream1, "encoding"):
            return self.stream1.encoding
        return self.stream2.encoding

    @property
    def error(self):
        if hasattr(self.stream1, "error"):
            return self.stream1.error
        return self.stream2.error

    @property
    def newlines(self):
        if hasattr(self.stream1, "newlines"):
            return self.stream1.newlines
        return self.stream2.newlines

    def detach(self):
        raise NotImplementedError

    def read(self, *args, **kwargs):
        raise NotImplementedError

    def readline(self, *args, **kwargs):
        raise NotImplementedError

    def tell(self, *args, **kwargs):
        raise NotImplementedError


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


def is_nan_or_inf(value):
    return np.isnan(value) or np.isinf(value)


def _to_pinnable(obj):
    """Converts obj to a form that can be pinned in object store memory.

    Currently only numpy arrays are pinned in memory, if you have a strong
    reference to the array value.
    """

    return (obj, np.zeros(1))


def _from_pinnable(obj):
    """Retrieve from _to_pinnable format."""

    return obj[0]


def diagnose_serialization(trainable: Callable):
    """Utility for detecting why your trainable function isn't serializing.

    Args:
        trainable: The trainable object passed to
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

    print(
        "Inspecting the scope of the trainable by running "
        f"`inspect.getclosurevars({str(trainable)})`..."
    )
    closure = inspect.getclosurevars(trainable)
    failure_set = set()
    if closure.globals:
        print(
            f"Detected {len(closure.globals)} global variables. "
            "Checking serializability..."
        )
        check_variables(closure.globals, failure_set, lambda s: print("   " + s))

    if closure.nonlocals:
        print(
            f"Detected {len(closure.nonlocals)} nonlocal variables. "
            "Checking serializability..."
        )
        check_variables(closure.nonlocals, failure_set, lambda s: print("   " + s))

    if not failure_set:
        print(
            "Nothing was found to have failed the diagnostic test, though "
            "serialization did not succeed. Feel free to raise an "
            "issue on github."
        )
        return failure_set
    else:
        print(
            f"Variable(s) {failure_set} was found to be non-serializable. "
            "Consider either removing the instantiation/imports "
            "of these objects or moving them into the scope of "
            "the trainable. "
        )
        return failure_set


def atomic_save(state: Dict, checkpoint_dir: str, file_name: str, tmp_file_name: str):
    """Atomically saves the state object to the checkpoint directory.

    This is automatically used by tune.run during a Tune job.

    Args:
        state: Object state to be serialized.
        checkpoint_dir: Directory location for the checkpoint.
        file_name: Final name of file.
        tmp_file_name: Temporary name of file.
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
        dirpath: Directory in which to look for the checkpoint file.
        ckpt_pattern: File name pattern to match to find checkpoint
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


def wait_for_gpu(
    gpu_id: Optional[Union[int, str]] = None,
    target_util: float = 0.01,
    retry: int = 20,
    delay_s: int = 5,
    gpu_memory_limit: Optional[float] = None,
):
    """Checks if a given GPU has freed memory.

    Requires ``gputil`` to be installed: ``pip install gputil``.

    Args:
        gpu_id: GPU id or uuid to check.
            Must be found within GPUtil.getGPUs(). If none, resorts to
            the first item returned from `ray.get_gpu_ids()`.
        target_util: The utilization threshold to reach to unblock.
            Set this to 0 to block until the GPU is completely free.
        retry: Number of times to check GPU limit. Sleeps `delay_s`
            seconds between checks.
        delay_s: Seconds to wait before check.
        gpu_memory_limit: Deprecated.

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
        raise ValueError("'gpu_memory_limit' is deprecated. Use 'target_util' instead.")
    if GPUtil is None:
        raise RuntimeError("GPUtil must be installed if calling `wait_for_gpu`.")

    if gpu_id is None:
        gpu_id_list = ray.get_gpu_ids()
        if not gpu_id_list:
            raise RuntimeError(
                "No GPU ids found from `ray.get_gpu_ids()`. "
                "Did you set Tune resources correctly?"
            )
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
            "UUID (e.g., 'GPU-04546190-b68d-65ac-101b-035f8faed77d')."
        )

    for i in range(int(retry)):
        gpu_object = next(g for g in GPUtil.getGPUs() if gpu_id_fn(g) == gpu_id)
        if gpu_object.memoryUtil > target_util:
            logger.info(
                f"Waiting for GPU util to reach {target_util}. "
                f"Util: {gpu_object.memoryUtil:0.3f}"
            )
            time.sleep(delay_s)
        else:
            return True
    raise RuntimeError("GPU memory was not freed.")


def validate_save_restore(
    trainable_cls: Type,
    config: Optional[Dict] = None,
    num_gpus: int = 0,
    use_object_store: bool = False,
):
    """Helper method to check if your Trainable class will resume correctly.

    Args:
        trainable_cls: Trainable class for evaluation.
        config: Config to pass to Trainable when testing.
        num_gpus: GPU resources to allocate when testing.
        use_object_store: Whether to save and restore to Ray's object
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
        "to be returned."
    )

    if use_object_store:
        restore_check = trainable_2.restore_from_object.remote(
            trainable_1.save_to_object.remote()
        )
        ray.get(restore_check)
    else:
        restore_check = ray.get(trainable_2.restore.remote(trainable_1.save.remote()))

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
            "`func(config, checkpoint_dir=None)`. Got {}".format(func_args)
        )
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
        dirname: Dirname to create in `local_dir`
        local_dir: Root directory for the log dir

    Returns:
        full path to the newly created logdir.
    """
    local_dir = os.path.expanduser(local_dir)
    logdir = os.path.join(local_dir, dirname)
    if os.path.exists(logdir):
        old_dirname = dirname
        dirname += "_" + uuid.uuid4().hex[:4]
        logger.info(
            f"Creating a new dirname {dirname} because "
            f"trial dirname '{old_dirname}' already exists."
        )
        logdir = os.path.join(local_dir, dirname)
    os.makedirs(logdir, exist_ok=True)
    return logdir


def validate_warmstart(
    parameter_names: List[str],
    points_to_evaluate: List[Union[List, Dict]],
    evaluated_rewards: List,
    validate_point_name_lengths: bool = True,
):
    """Generic validation of a Searcher's warm start functionality.
    Raises exceptions in case of type and length mismatches between
    parameters.

    If ``validate_point_name_lengths`` is False, the equality of lengths
    between ``points_to_evaluate`` and ``parameter_names`` will not be
    validated.
    """
    if points_to_evaluate:
        if not isinstance(points_to_evaluate, list):
            raise TypeError(
                "points_to_evaluate expected to be a list, got {}.".format(
                    type(points_to_evaluate)
                )
            )
        for point in points_to_evaluate:
            if not isinstance(point, (dict, list)):
                raise TypeError(
                    f"points_to_evaluate expected to include list or dict, "
                    f"got {point}."
                )

            if validate_point_name_lengths and (not len(point) == len(parameter_names)):
                raise ValueError(
                    "Dim of point {}".format(point)
                    + " and parameter_names {}".format(parameter_names)
                    + " do not match."
                )

    if points_to_evaluate and evaluated_rewards:
        if not isinstance(evaluated_rewards, list):
            raise TypeError(
                "evaluated_rewards expected to be a list, got {}.".format(
                    type(evaluated_rewards)
                )
            )
        if not len(evaluated_rewards) == len(points_to_evaluate):
            raise ValueError(
                "Dim of evaluated_rewards {}".format(evaluated_rewards)
                + " and points_to_evaluate {}".format(points_to_evaluate)
                + " do not match."
            )


if __name__ == "__main__":
    ray.init()
    X = pin_in_object_store("hello")
    print(X)
    result = get_pinned_object(X)
    print(result)
