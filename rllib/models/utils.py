from typing import Callable, Optional, Union

from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_jax, try_import_tf, try_import_torch


@DeveloperAPI
def input_to_output_specs(
    input_specs: SpecDict,
    num_input_feature_dims: int,
    output_key: str,
    output_feature_spec: TensorSpec,
) -> SpecDict:
    """Convert an input spec to an output spec, based on a module.

    Drops the feature dimension(s) from an input_specs, replacing them with
    output_feature_spec dimension(s).

    Examples:
        input_to_output_specs(
            input_specs=SpecDict({
                "bork": "batch, time, feature0",
                "dork": "batch, time, feature1"
                }, feature0=2, feature1=3
            ),
            num_input_feature_dims=1,
            output_key="outer_product",
            output_feature_spec=TensorSpec("row, col", row=2, col=3)
        )

        will return:
        SpecDict({"outer_product": "batch, time, row, col", row=2, col=3})

        input_to_output_specs(
            input_specs=SpecDict({
                "bork": "batch, time, h, w, c",
                }, h=32, w=32, c=3,
            ),
            num_input_feature_dims=3,
            output_key="latent_image_representation",
            output_feature_spec=TensorSpec("feature", feature=128)
        )

        will return:
        SpecDict({"latent_image_representation": "batch, time, feature"}, feature=128)


    Args:
        input_specs: SpecDict describing input to a specified module
        num_input_dims: How many feature dimensions the module will process. E.g.
            a linear layer will only process the last dimension (1), while a CNN
            might process the last two dimensions (2)
        output_key: The key in the output spec we will write the resulting shape to
        output_feature_spec: A spec denoting the feature dimensions output by a
            specified module

    Returns:
        A SpecDict based on the input_specs, with the trailing dimensions replaced
            by the output_feature_spec

    """
    assert num_input_feature_dims >= 1, "Must specify at least one feature dim"
    num_dims = [len(v.shape) != len for v in input_specs.values()]
    assert all(
        nd == num_dims[0] for nd in num_dims
    ), "All specs in input_specs must all have the same number of dimensions"

    # All keys in input should have the same numbers of dims
    # so it doesn't matter which key we use
    key = list(input_specs.keys())[0]
    batch_spec = input_specs[key].rdrop(num_input_feature_dims)
    full_spec = batch_spec.append(output_feature_spec)
    return SpecDict({output_key: full_spec})


@DeveloperAPI
def get_activation_fn(
    name: Optional[Union[Callable, str]] = None,
    framework: str = "tf",
):
    """Returns a framework specific activation function, given a name string.

    Args:
        name: One of "relu" (default), "tanh", "elu",
            "swish" (or "silu", which is the same), or "linear" (same as None).
        framework: One of "jax", "tf|tf2" or "torch".

    Returns:
        A framework-specific activtion function. e.g. tf.nn.tanh or
            torch.nn.ReLU. None if name in ["linear", None].

    Raises:
        ValueError: If name is an unknown activation function.
    """
    # Already a callable, return as-is.
    if callable(name):
        return name

    name_lower = name.lower() if isinstance(name, str) else name

    # Infer the correct activation function from the string specifier.
    if framework == "torch":
        if name_lower in ["linear", None]:
            return None

        _, nn = try_import_torch()
        # First try getting the correct activation function from nn directly.
        # Note that torch activation functions are not all lower case.
        fn = getattr(nn, name, None)
        if fn is not None:
            return fn

        if name_lower in ["swish", "silu"]:
            return nn.SiLU
        elif name_lower == "relu":
            return nn.ReLU
        elif name_lower == "tanh":
            return nn.Tanh
        elif name_lower == "elu":
            return nn.ELU
    elif framework == "jax":
        if name_lower in ["linear", None]:
            return None
        jax, _ = try_import_jax()
        if name_lower in ["swish", "silu"]:
            return jax.nn.swish
        if name_lower == "relu":
            return jax.nn.relu
        elif name_lower == "tanh":
            return jax.nn.hard_tanh
        elif name_lower == "elu":
            return jax.nn.elu
    else:
        assert framework in ["tf", "tf2"], "Unsupported framework `{}`!".format(
            framework
        )
        if name_lower in ["linear", None]:
            return None

        tf1, tf, tfv = try_import_tf()
        # Try getting the correct activation function from tf.nn directly.
        # Note that tf activation functions are all lower case, so this should always
        # work.
        fn = getattr(tf.nn, name_lower, None)

        if fn is not None:
            return fn

    raise ValueError(
        "Unknown activation ({}) for framework={}!".format(name, framework)
    )


@DeveloperAPI
def get_initializer_fn(name: Optional[Union[str, Callable]], framework: str = "torch"):
    """Returns the framework-specific initializer class or function.

    This function relies fully on the specified initializer classes and
    functions in the frameworks `torch` and `tf2` (see for `torch`
    https://pytorch.org/docs/stable/nn.init.html and for `tf2` see
    https://www.tensorflow.org/api_docs/python/tf/keras/initializers).

    Note, for framework `torch` the in-place initializers are needed, i.e. names
    should end with an underscore `_`, e.g. `glorot_uniform_`.

    Args:
        name: Name of the initializer class or function in one of the two
            supported frameworks, i.e. `torch` or `tf2`.
        framework: The framework string, either `torch  or `tf2`.

    Returns:
        A framework-specific function or class defining an initializer to be used
        for network initialization,

    Raises:
        `ValueError` if the `name` is neither class or function in the specified
        `framework`. Raises also a `ValueError`, if `name` does not define an
        in-place initializer for framework `torch`.
    """
    # Already a callable or `None` return as is. If `None` we use the default
    # initializer defined in the framework-specific layers themselves.
    if callable(name) or name is None:
        return name

    if framework == "torch":
        name_lower = name.lower() if isinstance(name, str) else name

        _, nn = try_import_torch()

        # Check, if the name includes an underscore. We must use the
        # in-place initialization from Torch.
        if not name_lower.endswith("_"):
            raise ValueError(
                "Not an in-place initializer: Torch weight initializers "
                "need to be provided as their in-place version, i.e. "
                "<initializaer_name> + '_'. See "
                "https://pytorch.org/docs/stable/nn.init.html. "
                f"User provided {name}."
            )

        # First, try to get the initialization directly from `nn.init`.
        # Note, that all initialization methods in `nn.init` are lower
        # case and that `<method>_` defines the "in-place" method.
        fn = getattr(nn.init, name_lower, None)
        if fn is not None:
            # TODO (simon): Raise a warning if not "in-place" method.
            return fn
        # Unknown initializer.
        else:
            # Inform the user that this initializer does not exist.
            raise ValueError(
                f"Unknown initializer name: {name_lower} is not a method in "
                "`torch.nn.init`!"
            )
    elif framework == "tf2":
        # Note, as initializer classes in TensorFlow can be either given by their
        # name in camel toe typing or by their shortcut we use the `name` as it is.
        # See https://www.tensorflow.org/api_docs/python/tf/keras/initializers.

        _, tf, _ = try_import_tf()

        # Try to get the initialization function directly from `tf.keras.initializers`.
        fn = getattr(tf.keras.initializers, name, None)
        if fn is not None:
            return fn
        # Unknown initializer.
        else:
            # Inform the user that this initializer does not exist.
            raise ValueError(
                f"Unknown initializer: {name} is not a initializer in "
                "`tf.keras.initializers`!"
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
    # Dreamer-style (S-sized model) Atari or DM Control Suite.
    filters_64x64 = [
        [32, [4, 4], 2],
        [64, [4, 4], 2],
        [128, [4, 4], 2],
        [256, [4, 4], 2],
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
    if len(shape) in [2, 3] and (shape[:2] == [96, 96] or shape[1:] == [96, 96]):
        return filters_96x96
    elif len(shape) in [2, 3] and (shape[:2] == [84, 84] or shape[1:] == [84, 84]):
        return filters_84x84
    elif len(shape) in [2, 3] and (shape[:2] == [64, 64] or shape[1:] == [64, 64]):
        return filters_64x64
    elif len(shape) in [2, 3] and (shape[:2] == [42, 42] or shape[1:] == [42, 42]):
        return filters_42x42
    elif len(shape) in [2, 3] and (shape[:2] == [10, 10] or shape[1:] == [10, 10]):
        return filters_10x10
    else:
        raise ValueError(
            "No default configuration for obs shape {}".format(shape)
            + ", you must specify `conv_filters` manually as a model option. "
            "Default configurations are only available for inputs of the following "
            "shapes: [42, 42, K], [84, 84, K], [64, 64, K], [10, 10, K]. You may "
            "alternatively want to use a custom model or preprocessor."
        )


@DeveloperAPI
def get_initializer(name, framework="tf"):
    """Returns a framework specific initializer, given a name string.

    Args:
        name: One of "xavier_uniform" (default), "xavier_normal".
        framework: One of "jax", "tf|tf2" or "torch".

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
        assert framework in ["tf", "tf2"], "Unsupported framework `{}`!".format(
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
