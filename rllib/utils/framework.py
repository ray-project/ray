import logging
import os
import sys
from typing import Any, Optional

from ray.rllib.utils.types import TensorStructType, TensorShape, TensorType

logger = logging.getLogger(__name__)

# Represents a generic tensor type.
TensorType = TensorType

# Either a plain tensor, or a dict or tuple of tensors (or StructTensors).
TensorStructType = TensorStructType


def try_import_tf(error=False):
    """Tries importing tf and returns the module (or None).

    Args:
        error (bool): Whether to raise an error if tf cannot be imported.

    Returns:
        Tuple:
            - tf1.x module (either from tf2.x.compat.v1 OR as tf1.x).
            - tf module (resulting from `import tensorflow`).
                Either tf1.x or 2.x.
            - The actually installed tf version as int: 1 or 2.

    Raises:
        ImportError: If error=True and tf is not installed.
    """
    # Make sure, these are reset after each test case
    # that uses them: del os.environ["RLLIB_TEST_NO_TF_IMPORT"]
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow for test purposes")
        return None, None, None

    if "TF_CPP_MIN_LOG_LEVEL" not in os.environ:
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

    # TODO: (sven) Allow env var to force compat.v1 behavior even if tf2.x
    #  installed.

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
        except ImportError as e:
            if error:
                raise e
            return None, None, None

    # Try "reducing" tf to tf.compat.v1.
    try:
        tf1_module = tf_module.compat.v1
        if not was_imported:
            tf1_module.disable_v2_behavior()
    # No compat.v1 -> return tf as is.
    except AttributeError:
        tf1_module = tf_module

    if not hasattr(tf_module, "__version__"):
        version = 1  # sphinx doc gen
    else:
        version = 2 if "2." in tf_module.__version__[:2] else 1

    return tf1_module, tf_module, version


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


def try_import_tfp(error=False):
    """Tries importing tfp and returns the module (or None).

    Args:
        error (bool): Whether to raise an error if tfp cannot be imported.

    Returns:
        The tfp module.

    Raises:
        ImportError: If error=True and tfp is not installed.
    """
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow Probability for test "
                       "purposes.")
        return None

    try:
        import tensorflow_probability as tfp
        return tfp
    except ImportError as e:
        if error:
            raise e
        return None


# Fake module for torch.nn.
class NNStub:
    def __init__(self, *a, **kw):
        # Fake nn.functional module within torch.nn.
        self.functional = None
        self.Module = ModuleStub


# Fake class for torch.nn.Module to allow it to be inherited from.
class ModuleStub:
    def __init__(self, *a, **kw):
        raise ImportError("Could not import `torch`.")


def try_import_torch(error=False):
    """Tries importing torch and returns the module (or None).

    Args:
        error (bool): Whether to raise an error if torch cannot be imported.

    Returns:
        tuple: torch AND torch.nn modules.

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
    except ImportError as e:
        if error:
            raise e
        return _torch_stubs()


def _torch_stubs():
    nn = NNStub()
    return None, nn


def get_variable(value,
                 framework: str = "tf",
                 trainable: bool = False,
                 tf_name: str = "unnamed-variable",
                 torch_tensor: bool = False,
                 device: Optional[str] = None,
                 shape: Optional[TensorShape] = None,
                 dtype: Optional[Any] = None):
    """
    Args:
        value (any): The initial value to use. In the non-tf case, this will
            be returned as is. In the tf case, this could be a tf-Initializer
            object.
        framework (str): One of "tf", "torch", or None.
        trainable (bool): Whether the generated variable should be
            trainable (tf)/require_grad (torch) or not (default: False).
        tf_name (str): For framework="tf": An optional name for the
            tf.Variable.
        torch_tensor (bool): For framework="torch": Whether to actually create
            a torch.tensor, or just a python value (default).
        device (Optional[torch.Device]): An optional torch device to use for
            the created torch tensor.
        shape (Optional[TensorShape]): An optional shape to use iff `value`
            does not have any (e.g. if it's an initializer w/o explicit value).
        dtype (Optional[TensorType]): An optional dtype to use iff `value` does
            not have any (e.g. if it's an initializer w/o explicit value).

    Returns:
        any: A framework-specific variable (tf.Variable, torch.tensor, or
            python primitive).
    """
    if framework in ["tf", "tfe"]:
        import tensorflow as tf
        dtype = dtype or getattr(
            value, "dtype", tf.float32
            if isinstance(value, float) else tf.int32
            if isinstance(value, int) else None)
        return tf.compat.v1.get_variable(
            tf_name,
            initializer=value,
            dtype=dtype,
            trainable=trainable,
            **({} if shape is None else {
                "shape": shape
            }))
    elif framework == "torch" and torch_tensor is True:
        torch, _ = try_import_torch()
        var_ = torch.from_numpy(value)
        if dtype == torch.float32:
            var_ = var_.float()
        elif dtype == torch.int32:
            var_ = var_.int()
        if device:
            var_ = var_.to(device)
        var_.requires_grad = trainable
        return var_
    # torch or None: Return python primitive.
    return value


def get_activation_fn(name, framework="tf"):
    """Returns a framework specific activation function, given a name string.

    Args:
        name (str): One of "relu" (default), "tanh", or "linear".
        framework (str): One of "tf" or "torch".

    Returns:
        A framework-specific activtion function. e.g. tf.nn.tanh or
            torch.nn.ReLU. None if name in ["linear", None].

    Raises:
        ValueError: If name is an unknown activation function.
    """
    if framework == "torch":
        if name in ["linear", None]:
            return None
        _, nn = try_import_torch()
        if name == "relu":
            return nn.ReLU
        elif name == "tanh":
            return nn.Tanh
    else:
        if name in ["linear", None]:
            return None
        tf1, tf, tfv = try_import_tf()
        fn = getattr(tf.nn, name, None)
        if fn is not None:
            return fn

    raise ValueError("Unknown activation ({}) for framework={}!".format(
        name, framework))
