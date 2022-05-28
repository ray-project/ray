from typing import Optional

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_jax, try_import_tf, try_import_torch


@DeveloperAPI
def get_activation_fn(name: Optional[str] = None, framework: str = "tf"):
    """Returns a framework specific activation function, given a name string.

    Args:
        name (Optional[str]): One of "relu" (default), "tanh", "elu",
            "swish", or "linear" (same as None).
        framework (str): One of "jax", "tf|tfe|tf2" or "torch".

    Returns:
        A framework-specific activtion function. e.g. tf.nn.tanh or
            torch.nn.ReLU. None if name in ["linear", None].

    Raises:
        ValueError: If name is an unknown activation function.
    """
    # Already a callable, return as-is.
    if callable(name):
        return name

    # Infer the correct activation function from the string specifier.
    if framework == "torch":
        if name in ["linear", None]:
            return None
        if name == "swish":
            from ray.rllib.utils.torch_utils import Swish

            return Swish
        _, nn = try_import_torch()
        if name == "relu":
            return nn.ReLU
        elif name == "tanh":
            return nn.Tanh
        elif name == "elu":
            return nn.ELU
    elif framework == "jax":
        if name in ["linear", None]:
            return None
        jax, _ = try_import_jax()
        if name == "swish":
            return jax.nn.swish
        if name == "relu":
            return jax.nn.relu
        elif name == "tanh":
            return jax.nn.hard_tanh
        elif name == "elu":
            return jax.nn.elu
    else:
        assert framework in ["tf", "tfe", "tf2"], "Unsupported framework `{}`!".format(
            framework
        )
        if name in ["linear", None]:
            return None
        tf1, tf, tfv = try_import_tf()
        fn = getattr(tf.nn, name, None)
        if fn is not None:
            return fn

    raise ValueError(
        "Unknown activation ({}) for framework={}!".format(name, framework)
    )


@DeveloperAPI
def get_filter_config(shape):
    """Returns a default Conv2D filter config (list) for a given image shape.

    Args:
        shape (Tuple[int]): The input (image) shape, e.g. (84,84,3).

    Returns:
        List[list]: The Conv2D filter configuration usable as `conv_filters`
            inside a model config dict.
    """
    # VizdoomGym (large 480x640).
    filters_480x640 = [
        [16, [24, 32], [14, 18]],
        [32, [6, 6], 4],
        [256, [9, 9], 1],
    ]
    # VizdoomGym (small 240x320).
    filters_240x320 = [
        [16, [12, 16], [7, 9]],
        [32, [6, 6], 4],
        [256, [9, 9], 1],
    ]
    # 96x96x3 (e.g. CarRacing-v0).
    filters_96x96 = [
        [16, [8, 8], 4],
        [32, [4, 4], 2],
        [256, [11, 11], 2],
    ]
    # Atari.
    filters_84x84 = [
        [16, [8, 8], 4],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
    ]
    # Small (1/2) Atari.
    filters_42x42 = [
        [16, [4, 4], 2],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
    ]
    # Test image (10x10).
    filters_10x10 = [
        [16, [5, 5], 2],
        [32, [5, 5], 2],
    ]

    shape = list(shape)
    if len(shape) in [2, 3] and (shape[:2] == [480, 640] or shape[1:] == [480, 640]):
        return filters_480x640
    elif len(shape) in [2, 3] and (shape[:2] == [240, 320] or shape[1:] == [240, 320]):
        return filters_240x320
    elif len(shape) in [2, 3] and (shape[:2] == [96, 96] or shape[1:] == [96, 96]):
        return filters_96x96
    elif len(shape) in [2, 3] and (shape[:2] == [84, 84] or shape[1:] == [84, 84]):
        return filters_84x84
    elif len(shape) in [2, 3] and (shape[:2] == [42, 42] or shape[1:] == [42, 42]):
        return filters_42x42
    elif len(shape) in [2, 3] and (shape[:2] == [10, 10] or shape[1:] == [10, 10]):
        return filters_10x10
    else:
        raise ValueError(
            "No default configuration for obs shape {}".format(shape)
            + ", you must specify `conv_filters` manually as a model option. "
            "Default configurations are only available for inputs of shape "
            "[42, 42, K] and [84, 84, K]. You may alternatively want "
            "to use a custom model or preprocessor."
        )


@DeveloperAPI
def get_initializer(name, framework="tf"):
    """Returns a framework specific initializer, given a name string.

    Args:
        name (str): One of "xavier_uniform" (default), "xavier_normal".
        framework (str): One of "jax", "tf|tfe|tf2" or "torch".

    Returns:
        A framework-specific initializer function, e.g.
            tf.keras.initializers.GlorotUniform or
            torch.nn.init.xavier_uniform_.

    Raises:
        ValueError: If name is an unknown initializer.
    """
    # Already a callable, return as-is.
    if callable(name):
        return name

    if framework == "jax":
        _, flax = try_import_jax()
        assert flax is not None, "`flax` not installed. Try `pip install jax flax`."
        import flax.linen as nn

        if name in [None, "default", "xavier_uniform"]:
            return nn.initializers.xavier_uniform()
        elif name == "xavier_normal":
            return nn.initializers.xavier_normal()
    if framework == "torch":
        _, nn = try_import_torch()
        assert nn is not None, "`torch` not installed. Try `pip install torch`."
        if name in [None, "default", "xavier_uniform"]:
            return nn.init.xavier_uniform_
        elif name == "xavier_normal":
            return nn.init.xavier_normal_
    else:
        assert framework in ["tf", "tfe", "tf2"], "Unsupported framework `{}`!".format(
            framework
        )
        tf1, tf, tfv = try_import_tf()
        assert (
            tf is not None
        ), "`tensorflow` not installed. Try `pip install tensorflow`."
        if name in [None, "default", "xavier_uniform"]:
            return tf.keras.initializers.GlorotUniform
        elif name == "xavier_normal":
            return tf.keras.initializers.GlorotNormal

    raise ValueError(
        "Unknown activation ({}) for framework={}!".format(name, framework)
    )
