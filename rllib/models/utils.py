from ray.rllib.utils.framework import try_import_tf, try_import_torch


def get_filter_config(shape):
    """Returns a default Conv2D filter config (list) for a given image shape.

    Args:
        shape (Tuple[int]): The input (image) shape, e.g. (84,84,3).

    Returns:
        List[list]: The Conv2D filter configuration usable as `conv_filters`
            inside a model config dict.
    """
    shape = list(shape)
    filters_84x84 = [
        [16, [8, 8], 4],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
    ]
    filters_42x42 = [
        [16, [4, 4], 2],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
    ]
    if len(shape) == 3 and shape[:2] == [84, 84]:
        return filters_84x84
    elif len(shape) == 3 and shape[:2] == [42, 42]:
        return filters_42x42
    else:
        raise ValueError(
            "No default configuration for obs shape {}".format(shape) +
            ", you must specify `conv_filters` manually as a model option. "
            "Default configurations are only available for inputs of shape "
            "[42, 42, K] and [84, 84, K]. You may alternatively want "
            "to use a custom model or preprocessor.")


def get_initializer(name, framework="tf"):
    """Returns a framework specific initializer, given a name string.

    Args:
        name (str): One of "xavier_uniform" (default), "xavier_normal".
        framework (str): One of "tf" or "torch".

    Returns:
        A framework-specific initializer function, e.g.
            tf.keras.initializers.GlorotUniform or
            torch.nn.init.xavier_uniform_.

    Raises:
        ValueError: If name is an unknown initializer.
    """
    if framework == "torch":
        _, nn = try_import_torch()
        if name in [None, "default", "xavier_uniform"]:
            return nn.init.xavier_uniform_
        elif name == "xavier_normal":
            return nn.init.xavier_normal_
    else:
        tf1, tf, tfv = try_import_tf()
        if name in [None, "default", "xavier_uniform"]:
            return tf.keras.initializers.GlorotUniform
        elif name == "xavier_normal":
            return tf.keras.initializers.GlorotNormal

    raise ValueError("Unknown activation ({}) for framework={}!".format(
        name, framework))
