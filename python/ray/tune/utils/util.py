import copy
import glob
import inspect
import logging
import os
import threading
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime
from numbers import Number
from threading import Thread
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

import numpy as np
import psutil
import ray
from ray.air.checkpoint import Checkpoint
from ray.air._internal.remote_storage import delete_at_uri, _is_local_windows_path
from ray.air.util.node import _get_node_id_from_node_ip, _force_on_node
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.air._internal.json import SafeFallbackEncoder  # noqa
from ray.air._internal.util import (  # noqa: F401
    is_nan,
    is_nan_or_inf,
)
from ray._private.dict import (  # noqa: F401
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


@DeveloperAPI
class UtilMonitor(Thread):
    """Class for system usage utilization monitoring.

    It keeps track of CPU, RAM, GPU, VRAM usage (each gpu separately) by
    pinging for information every x seconds in a separate thread.

    Requires psutil and GPUtil to be installed. Can be enabled with
    Tuner(param_space={"log_sys_usage": True}).
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


@DeveloperAPI
def retry_fn(
    fn: Callable[[], Any],
    exception_type: Union[Type[Exception], Sequence[Type[Exception]]] = Exception,
    num_retries: int = 3,
    sleep_time: int = 1,
    timeout: Optional[Number] = None,
) -> bool:
    errored = threading.Event()

    def _try_fn():
        try:
            fn()
        except exception_type as e:
            logger.warning(e)
            errored.set()

    for i in range(num_retries):
        errored.clear()

        proc = threading.Thread(target=_try_fn)
        proc.daemon = True
        proc.start()
        proc.join(timeout=timeout)

        if proc.is_alive():
            logger.debug(
                f"Process timed out (try {i+1}/{num_retries}): "
                f"{getattr(fn, '__name__', None)}"
            )
        elif not errored.is_set():
            return True

        # Timed out, sleep and try again
        time.sleep(sleep_time)

    # Timed out, so return False
    return False


def _split_remote_local_path(
    path: str, default_local_path: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Return a local and remote location from a path.

    Our storage configuration allows to specify paths on
    remote storage or local disk. This utility function detects the
    location of a path and returns a tuple of (local path, remote path)
    for further processing.

    For instance, if ``path="s3://some/location"``, then
    ``local_path=default_local_path`` and ``remote_path="s3://some/location``.

    If ``path="/local/dir"``, then ``local_path="/local/dir"`` and
    ``remote_path=None``.

    """
    parsed = urllib.parse.urlparse(path)
    if parsed.scheme and not _is_local_windows_path(path):
        # If a scheme is set, this means it's not a local path.
        # Note that we also treat `file://` as a URI.
        remote_path = path
        local_path = default_local_path
    else:
        remote_path = None
        local_path = path

    return local_path, remote_path


def _resolve_storage_path(
    path: str,
    legacy_local_dir: Optional[str],
    legacy_upload_dir: Optional[str],
    error_location: str = "air.RunConfig",
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve a path (using ``_split_remote_local_path``) with backwards compatibility.

    As we changed the input API to specify persistent storage locations, we still
    have the old ways to define local and remote storage paths. Until these are
    fully deprecated, this utility helps resolving all options currently available
    to users to configure storage locations.
    """

    local_path, remote_path = _split_remote_local_path(
        path=path, default_local_path=None
    )

    if legacy_local_dir:
        if local_path:
            raise ValueError(
                "Only one of `storage_path` and `local_dir` can be passed to "
                f"`{error_location}`. Since `local_dir` is deprecated, "
                "only pass `storage_path` instead."
            )
        local_path = legacy_local_dir

    if legacy_upload_dir:
        if remote_path:
            raise ValueError(
                "Only one of a remote `storage_path` and `SyncConfig.upload_dir` "
                f"can be passed to `{error_location}`. "
                "Since `SyncConfig.upload_dir` is deprecated, "
                "only pass `storage_path` instead."
            )
        remote_path = legacy_upload_dir

    return local_path, remote_path


@ray.remote
def _serialize_checkpoint(checkpoint_path) -> bytes:
    checkpoint = Checkpoint.from_directory(checkpoint_path)
    return checkpoint.to_bytes()


def _get_checkpoint_from_remote_node(
    checkpoint_path: str, node_ip: str, timeout: float = 300.0
) -> Optional[Checkpoint]:
    node_id = _get_node_id_from_node_ip(node_ip)

    if node_id is None:
        logger.warning(
            f"Could not fetch checkpoint with path {checkpoint_path} from "
            f"node with IP {node_ip} because the node is not available "
            f"anymore."
        )
        return None

    fut = _serialize_checkpoint.options(num_cpus=0, **_force_on_node(node_id)).remote(
        checkpoint_path
    )
    try:
        checkpoint_data = ray.get(fut, timeout=timeout)
    except Exception as e:
        logger.warning(
            f"Could not fetch checkpoint with path {checkpoint_path} from "
            f"node with IP {node_ip} because serialization failed: {e}"
        )
        return None
    return Checkpoint.from_bytes(checkpoint_data)


def _delete_external_checkpoint(checkpoint_uri: str):
    delete_at_uri(checkpoint_uri)


@DeveloperAPI
class warn_if_slow:
    """Prints a warning if a given operation is slower than 500ms.

    Example:
        >>> from ray.tune.utils.util import warn_if_slow
        >>> something = ... # doctest: +SKIP
        >>> with warn_if_slow("some_operation"): # doctest: +SKIP
        ...    ray.get(something) # doctest: +SKIP
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


@DeveloperAPI
class Tee(object):
    def __init__(self, stream1, stream2):
        self.stream1 = stream1
        self.stream2 = stream2

        # If True, we are currently handling a warning.
        # We use this flag to avoid infinite recursion.
        self._handling_warning = False

    def _warn(self, op, s, args, kwargs):
        # If we are already handling a warning, this is because
        # `logger.warning` below triggered the same object again
        # (e.g. because stderr is redirected to this object).
        # In that case, exit early to avoid recursion.
        if self._handling_warning:
            return

        msg = f"ValueError when calling '{op}' on stream ({s}). "
        msg += f"args: {args} kwargs: {kwargs}"

        self._handling_warning = True
        logger.warning(msg)
        self._handling_warning = False

    def seek(self, *args, **kwargs):
        for s in [self.stream1, self.stream2]:
            try:
                s.seek(*args, **kwargs)
            except ValueError:
                self._warn("seek", s, args, kwargs)

    def write(self, *args, **kwargs):
        for s in [self.stream1, self.stream2]:
            try:
                s.write(*args, **kwargs)
            except ValueError:
                self._warn("write", s, args, kwargs)

    def flush(self, *args, **kwargs):
        for s in [self.stream1, self.stream2]:
            try:
                s.flush(*args, **kwargs)
            except ValueError:
                self._warn("flush", s, args, kwargs)

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


@DeveloperAPI
def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


def _to_pinnable(obj):
    """Converts obj to a form that can be pinned in object store memory.

    Currently only numpy arrays are pinned in memory, if you have a strong
    reference to the array value.
    """

    return (obj, np.zeros(1))


def _from_pinnable(obj):
    """Retrieve from _to_pinnable format."""

    return obj[0]


@DeveloperAPI
def diagnose_serialization(trainable: Callable):
    """Utility for detecting why your trainable function isn't serializing.

    Args:
        trainable: The trainable object passed to
            tune.Tuner(trainable). Currently only supports
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
    from ray.tune.registry import register_trainable, _check_serializability

    def check_variables(objects, failure_set, printer):
        for var_name, variable in objects.items():
            msg = None
            try:
                _check_serializability(var_name, variable)
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


def _atomic_save(state: Dict, checkpoint_dir: str, file_name: str, tmp_file_name: str):
    """Atomically saves the state object to the checkpoint directory.

    This is automatically used by Tuner().fit during a Tune job.

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


def _load_newest_checkpoint(dirpath: str, ckpt_pattern: str) -> Optional[Dict]:
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


@PublicAPI(stability="beta")
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

        tuner = tune.Tuner(
            tune.with_resources(
                tune_func,
                resources={"gpu": 1}
            ),
            tune_config=tune.TuneConfig(num_samples=10)
        )
        tuner.fit()

    """
    GPUtil = _import_gputil()

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


@DeveloperAPI
def validate_save_restore(
    trainable_cls: Type,
    config: Optional[Dict] = None,
    num_gpus: int = 0,
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

    from ray.air.constants import TRAINING_ITERATION

    for _ in range(3):
        res = ray.get(trainable_1.train.remote())

    assert res.get(TRAINING_ITERATION), (
        "Validation will not pass because it requires `training_iteration` "
        "to be returned."
    )

    ray.get(trainable_2.restore.remote(trainable_1.save.remote()))

    res = ray.get(trainable_2.train.remote())
    assert res[TRAINING_ITERATION] == 4

    res = ray.get(trainable_2.train.remote())
    assert res[TRAINING_ITERATION] == 5
    return True


def _detect_checkpoint_function(train_func, abort=False, partial=False):
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
            "Provided training function must have 1 `config` argument "
            "`func(config)`. Got {}".format(func_args)
        )
    return validated


def _detect_reporter(func):
    """Use reporter if any arg has "reporter" and args = 2"""
    func_sig = inspect.signature(func)
    use_reporter = True
    try:
        func_sig.bind({}, reporter=None)
    except Exception as e:
        logger.debug(str(e))
        use_reporter = False
    return use_reporter


def _detect_config_single(func):
    """Check if func({}) works."""
    func_sig = inspect.signature(func)
    use_config_single = True
    try:
        func_sig.bind({})
    except Exception as e:
        logger.debug(str(e))
        use_config_single = False
    return use_config_single


@PublicAPI()
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
