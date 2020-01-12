import logging
import os

logger = logging.getLogger(__name__)


def try_import_tf():
    """
    Returns:
        The tf module (either from tf2.0.compat.v1 OR as tf1.x.
    """
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow for test purposes")
        return None

    try:
        if "TF_CPP_MIN_LOG_LEVEL" not in os.environ:
            os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
        import tensorflow.compat.v1 as tf
        tf.logging.set_verbosity(tf.logging.ERROR)
        tf.disable_v2_behavior()
        return tf
    except ImportError:
        try:
            import tensorflow as tf
            return tf
        except ImportError:
            return None


def try_import_tfp():
    """
    Returns:
        The tfp module.
    """
    if "RLLIB_TEST_NO_TF_IMPORT" in os.environ:
        logger.warning("Not importing TensorFlow Probability for test "
                       "purposes.")
        return None

    try:
        import tensorflow_probability as tfp
        return tfp
    except ImportError:
        return None


def try_import_torch():
    """
    Returns:
        tuple: torch AND torch.nn modules.
    """
    if "RLLIB_TEST_NO_TORCH_IMPORT" in os.environ:
        logger.warning("Not importing Torch for test purposes.")
        return None, None

    try:
        import torch
        import torch.nn as nn
        return torch, nn
    except ImportError:
        return None, None
