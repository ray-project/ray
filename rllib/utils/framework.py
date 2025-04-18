import logging
import numpy as np
import os
import sys
from typing import Any, Optional, TYPE_CHECKING

import tree  # pip install dm_tree

import ray
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import (
    TensorShape,
    TensorStructType,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger(__name__)


@PublicAPI
def convert_to_tensor(
    data: TensorStructType,
    framework: str,
    device: Optional[str] = None,
):
    """Converts any nested numpy struct into framework-specific tensors.

    Args:
        data: The input data (numpy) to convert to framework-specific tensors.
        framework: The framework to convert to. Only "torch" and "tf2" allowed.
        device: An optional device name (for torch only).

    Returns:
        The converted tensor struct matching the input data.
    """
    if framework == "torch":
        from ray.rllib.utils.torch_utils import convert_to_torch_tensor

        return convert_to_torch_tensor(data, device=device)
    elif framework == "tf2":
        _, tf, _ = try_import_tf()

        return tree.map_structure(lambda s: tf.convert_to_tensor(s), data)
    raise NotImplementedError(
        f"framework={framework} not supported in `convert_to_tensor()`!"
    )


@PublicAPI
def get_device(config: "AlgorithmConfig", num_gpus_requested: int = 1):
    """Returns a single device (CPU or some GPU) depending on a config.

    Args:
        config: An AlgorithmConfig to extract information from about the device to use.
        num_gpus_requested: The number of GPUs actually requested. This may be the value
            of `config.num_gpus_per_env_runner` when for example calling this function
            from an EnvRunner.

    Returns:
        A single device (or name) given `config` and `num_gpus_requested`.
    """
    if config.framework_str == "torch":
        torch, _ = try_import_torch()

        # TODO (Kourosh): How do we handle model parallelism?
        # TODO (Kourosh): Instead of using _TorchAccelerator, we should use the public
        #  API in ray.train but allow for session to be None without any errors raised.
        if num_gpus_requested > 0:
            from ray.air._internal.torch_utils import get_devices

            # `get_devices()` returns a list that contains the 0th device if
            # it is called from outside a Ray Train session. It's necessary to give
            # the user the option to run on the gpu of their choice, so we enable that
            # option here through `config.local_gpu_idx`.
            devices = get_devices()
            # Note, if we have a single learner and we do not run on Ray Tune, the local
            # learner is not an Ray actor and Ray does not manage devices for it.
            if (
                len(devices) == 1
                and ray._private.worker._mode() == ray._private.worker.WORKER_MODE
            ):
                return devices[0]
            else:
                assert config.local_gpu_idx < torch.cuda.device_count(), (
                    f"local_gpu_idx {config.local_gpu_idx} is not a valid GPU ID "
                    "or is not available."
                )
                # This is an index into the available CUDA devices. For example, if
                # `os.environ["CUDA_VISIBLE_DEVICES"] = "1"` then
                # `torch.cuda.device_count() = 1` and torch.device(0) maps to that GPU
                # with ID=1 on the node.
                return torch.device(config.local_gpu_idx)
        else:
            return torch.device("cpu")
    else:
        raise NotImplementedError(
            f"`framework_str` {config.framework_str} not supported!"
        )


@PublicAPI
def try_import_jax(error: bool = False):
    """Tries importing JAX and FLAX and returns both modules (or Nones).

    Args:
        error: Whether to raise an error if JAX/FLAX cannot be imported.

    Returns:
        Tuple containing the jax- and the flax modules.

    Raises:
        ImportError: If error=True and JAX is not installed.
    """
    if "RLLIB_TEST_NO_JAX_IMPORT" in os.environ:
        logger.warning("Not importing JAX for test purposes.")
        return None, None

    try:
        import jax
        import flax
    except ImportError:
        if error:
            raise ImportError(
                "Could not import JAX! RLlib requires you to "
                "install at least one deep-learning framework: "
                "`pip install [torch|tensorflow|jax]`."
            )
        return None, None

    return jax, flax


@PublicAPI
def try_import_tf(error: bool = False):
    """Tries importing tf and returns the module (or None).

    Args:
        error: Whether to raise an error if tf cannot be imported.

    Returns:
        Tuple containing
        1) tf1.x module (either from tf2.x.compat.v1 OR as tf1.x).
        2) tf module (resulting from `import tensorflow`). Either tf1.x or
        2.x. 3) The actually installed tf version as int: 1 or 2.

    Raises:
        ImportError: If error=True and tf is not installed.
    """
    tf_stub = _TFStub()
    # Make sure, these are reset after each test case
    # that uses them: del os.environ["RLLIB_TEST_NO_TF_IMPORT"]
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow for test purposes")
        return None, tf_stub, None

    if "TF_CPP_MIN_LOG_LEVEL" not in os.environ:
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

    # Try to reuse already imported tf module. This will avoid going through
    # the initial import steps below and thereby switching off v2_behavior
    # (switching off v2 behavior twice breaks all-framework tests for eager).
    was_imported = False
    if "tensorflow" in sys.modules:
        tf_module = sys.modules["tensorflow"]
        was_imported = True

    else:
        try:
            import tensorflow as tf_module
        except ImportError:
            if error:
                raise ImportError(
                    "Could not import TensorFlow! RLlib requires you to "
                    "install at least one deep-learning framework: "
                    "`pip install [torch|tensorflow|jax]`."
                )
            return None, tf_stub, None

    # Try "reducing" tf to tf.compat.v1.
    try:
        tf1_module = tf_module.compat.v1
        tf1_module.logging.set_verbosity(tf1_module.logging.ERROR)
        if not was_imported:
            tf1_module.disable_v2_behavior()
            tf1_module.enable_resource_variables()
        tf1_module.logging.set_verbosity(tf1_module.logging.WARN)
    # No compat.v1 -> return tf as is.
    except AttributeError:
        tf1_module = tf_module

    if not hasattr(tf_module, "__version__"):
        version = 1  # sphinx doc gen
    else:
        version = 2 if "2." in tf_module.__version__[:2] else 1

    return tf1_module, tf_module, version


# Fake module for tf.
class _TFStub:
    def __init__(self) -> None:
        self.keras = _KerasStub()

    def __bool__(self):
        # if tf should return False
        return False


# Fake module for tf.keras.
class _KerasStub:
    def __init__(self) -> None:
        self.Model = _FakeTfClassStub


# Fake classes under keras (e.g for tf.keras.Model)
class _FakeTfClassStub:
    def __init__(self, *a, **kw):
        raise ImportError("Could not import `tensorflow`. Try pip install tensorflow.")


@DeveloperAPI
def tf_function(tf_module):
    """Conditional decorator for @tf.function.

    Use @tf_function(tf) instead to avoid errors if tf is not installed."""

    # The actual decorator to use (pass in `tf` (which could be None)).
    def decorator(func):
        # If tf not installed -> return function as is (won't be used anyways).
        if tf_module is None or tf_module.executing_eagerly():
            return func
        # If tf installed, return @tf.function-decorated function.
        return tf_module.function(func)

    return decorator


@PublicAPI
def try_import_tfp(error: bool = False):
    """Tries importing tfp and returns the module (or None).

    Args:
        error: Whether to raise an error if tfp cannot be imported.

    Returns:
        The tfp module.

    Raises:
        ImportError: If error=True and tfp is not installed.
    """
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow Probability for test purposes.")
        return None

    try:
        import tensorflow_probability as tfp

        return tfp
    except ImportError as e:
        if error:
            raise e
        return None


# Fake module for torch.nn.
class _NNStub:
    def __init__(self, *a, **kw):
        # Fake nn.functional module within torch.nn.
        self.functional = None
        self.Module = _FakeTorchClassStub
        self.parallel = _ParallelStub()


# Fake class for e.g. torch.nn.Module to allow it to be inherited from.
class _FakeTorchClassStub:
    def __init__(self, *a, **kw):
        raise ImportError("Could not import `torch`. Try pip install torch.")


class _ParallelStub:
    def __init__(self, *a, **kw):
        self.DataParallel = _FakeTorchClassStub
        self.DistributedDataParallel = _FakeTorchClassStub


@PublicAPI
def try_import_torch(error: bool = False):
    """Tries importing torch and returns the module (or None).

    Args:
        error: Whether to raise an error if torch cannot be imported.

    Returns:
        Tuple consisting of the torch- AND torch.nn modules.

    Raises:
        ImportError: If error=True and PyTorch is not installed.
    """
    if "RLLIB_TEST_NO_TORCH_IMPORT" in os.environ:
        logger.warning("Not importing PyTorch for test purposes.")
        return _torch_stubs()

    try:
        import torch
        import torch.nn as nn

        return torch, nn
    except ImportError:
        if error:
            raise ImportError(
                "Could not import PyTorch! RLlib requires you to "
                "install at least one deep-learning framework: "
                "`pip install [torch|tensorflow|jax]`."
            )
        return _torch_stubs()


def _torch_stubs():
    nn = _NNStub()
    return None, nn


@DeveloperAPI
def get_variable(
    value: Any,
    framework: str = "tf",
    trainable: bool = False,
    tf_name: str = "unnamed-variable",
    torch_tensor: bool = False,
    device: Optional[str] = None,
    shape: Optional[TensorShape] = None,
    dtype: Optional[TensorType] = None,
) -> Any:
    """Creates a tf variable, a torch tensor, or a python primitive.

    Args:
        value: The initial value to use. In the non-tf case, this will
            be returned as is. In the tf case, this could be a tf-Initializer
            object.
        framework: One of "tf", "torch", or None.
        trainable: Whether the generated variable should be
            trainable (tf)/require_grad (torch) or not (default: False).
        tf_name: For framework="tf": An optional name for the
            tf.Variable.
        torch_tensor: For framework="torch": Whether to actually create
            a torch.tensor, or just a python value (default).
        device: An optional torch device to use for
            the created torch tensor.
        shape: An optional shape to use iff `value`
            does not have any (e.g. if it's an initializer w/o explicit value).
        dtype: An optional dtype to use iff `value` does
            not have any (e.g. if it's an initializer w/o explicit value).
            This should always be a numpy dtype (e.g. np.float32, np.int64).

    Returns:
        A framework-specific variable (tf.Variable, torch.tensor, or
        python primitive).
    """
    if framework in ["tf2", "tf"]:
        import tensorflow as tf

        dtype = dtype or getattr(
            value,
            "dtype",
            tf.float32
            if isinstance(value, float)
            else tf.int32
            if isinstance(value, int)
            else None,
        )
        return tf.compat.v1.get_variable(
            tf_name,
            initializer=value,
            dtype=dtype,
            trainable=trainable,
            **({} if shape is None else {"shape": shape}),
        )
    elif framework == "torch" and torch_tensor is True:
        torch, _ = try_import_torch()
        if not isinstance(value, np.ndarray):
            value = np.array(value)
        var_ = torch.from_numpy(value)
        if dtype in [torch.float32, np.float32]:
            var_ = var_.float()
        elif dtype in [torch.int32, np.int32]:
            var_ = var_.int()
        elif dtype in [torch.float64, np.float64]:
            var_ = var_.double()

        if device:
            var_ = var_.to(device)
        var_.requires_grad = trainable
        return var_
    # torch or None: Return python primitive.
    return value


@Deprecated(
    old="rllib/utils/framework.py::get_activation_fn",
    new="rllib/models/utils.py::get_activation_fn",
    error=True,
)
def get_activation_fn(name: Optional[str] = None, framework: str = "tf"):
    pass
