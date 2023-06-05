from typing import Callable, List, Optional, Tuple, Union

from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class TfMLP(tf.keras.Model):
    """A multi-layer perceptron with N dense layers.

    All layers (except for an optional additional extra output layer) share the same
    activation function, bias setup (use bias or not), and LayerNorm setup
    (use layer normalization or not).

    If `output_dim` (int) is not None, an additional, extra output dense layer is added,
    which might have its own activation function (e.g. "linear"). However, the output
    layer does NOT use layer normalization.
    """

    def __init__(
        self,
        *,
        input_dim: int,
        hidden_layer_dims: List[int],
        hidden_layer_use_layernorm: bool = False,
        hidden_layer_use_bias: bool = True,
        hidden_layer_activation: Optional[Union[str, Callable]] = "relu",
        output_dim: Optional[int] = None,
        output_use_bias: bool = True,
        output_activation: Optional[Union[str, Callable]] = "linear",
    ):
        """Initialize a TfMLP object.

        Args:
            input_dim: The input dimension of the network. Must not be None.
            hidden_layer_dims: The sizes of the hidden layers. If an empty list, only a
                single layer will be built of size `output_dim`.
            hidden_layer_use_layernorm: Whether to insert a LayerNormalization
                functionality in between each hidden layer's output and its activation.
            hidden_layer_use_bias: Whether to use bias on all dense layers (excluding
                the possible separate output layer).
            hidden_layer_activation: The activation function to use after each layer
                (except for the output). Either a tf.nn.[activation fn] callable or a
                string that's supported by tf.keras.layers.Activation(activation=...),
                e.g. "relu", "ReLU", "silu", or "linear".
            output_dim: The output dimension of the network. If None, no specific output
                layer will be added and the last layer in the stack will have
                size=`hidden_layer_dims[-1]`.
            output_use_bias: Whether to use bias on the separate output layer,
                if any.
            output_activation: The activation function to use for the output layer
                (if any). Either a tf.nn.[activation fn] callable or a string that's
                supported by tf.keras.layers.Activation(activation=...), e.g. "relu",
                "ReLU", "silu", or "linear".
        """
        super().__init__()
        assert input_dim > 0

        layers = []
        # Input layer.
        layers.append(tf.keras.Input(shape=(input_dim,)))

        hidden_activation = get_activation_fn(hidden_layer_activation, framework="tf2")

        for i in range(len(hidden_layer_dims)):
            # Dense layer with activation (or w/o in case we use LayerNorm, in which
            # case the activation is applied after the layer normalization step).
            layers.append(
                tf.keras.layers.Dense(
                    hidden_layer_dims[i],
                    activation=(
                        hidden_activation if not hidden_layer_use_layernorm else None
                    ),
                    use_bias=hidden_layer_use_bias,
                )
            )
            # Add LayerNorm and activation.
            if hidden_layer_use_layernorm:
                # Use epsilon=1e-5 here (instead of default 1e-3) to be unified
                # with torch.
                layers.append(tf.keras.layers.LayerNormalization(epsilon=1e-5))
                layers.append(tf.keras.layers.Activation(hidden_activation))

        if output_dim is not None:
            output_activation = get_activation_fn(output_activation, framework="tf2")
            layers.append(
                tf.keras.layers.Dense(
                    output_dim,
                    activation=output_activation,
                    use_bias=output_use_bias,
                )
            )

        self.network = tf.keras.Sequential(layers)

    def call(self, inputs, **kwargs):
        return self.network(inputs)


class TfCNN(tf.keras.Model):
    """A model containing a CNN with N Conv2D layers.

    All layers share the same activation function, bias setup (use bias or not),
    and LayerNormalization setup (use layer normalization or not).

    Note that there is no flattening nor an additional dense layer at the end of the
    stack. The output of the network is a 3D tensor of dimensions
    [width x height x num output filters].
    """

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]],
        cnn_filter_specifiers: List[List[Union[int, List]]],
        cnn_use_bias: bool = True,
        cnn_use_layernorm: bool = False,
        cnn_activation: Optional[str] = "relu",
    ):
        """Initializes a TfCNN instance.

        Args:
            input_dims: The 3D input dimensions of the network (incoming image).
            cnn_filter_specifiers: A list in which each element is another (inner) list
                of either the following forms:
                `[number of channels/filters, kernel, stride]`
                OR:
                `[number of channels/filters, kernel, stride, padding]`, where `padding`
                can either be "same" or "valid".
                When using the first format w/o the `padding` specifier, `padding` is
                "same" by default. Also, `kernel` and `stride` may be provided either as
                single ints (square) or as a tuple/list of two ints (width- and height
                dimensions) for non-squared kernel/stride shapes.
                A good rule of thumb for constructing CNN stacks is:
                When using padding="same", the input "image" will be reduced in size by
                the factor `stride`, e.g. input=(84, 84, 3) stride=2 kernel=x
                padding="same" filters=16 -> output=(42, 42, 16).
                For example, if you would like to reduce an Atari image from its
                original (84, 84, 3) dimensions down to (6, 6, F), you can construct the
                following stack and reduce the w x h dimension of the image by 2 in each
                layer:
                [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]] -> output=(6, 6, 128)
            cnn_use_bias: Whether to use bias on all Conv2D layers.
            cnn_activation: The activation function to use after each Conv2D layer.
            cnn_use_layernorm: Whether to insert a LayerNormalization functionality
                in between each Conv2D layer's outputs and its activation.
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_activation = get_activation_fn(cnn_activation, framework="tf2")

        layers = []

        # Input layer.
        layers.append(tf.keras.layers.Input(shape=input_dims))

        for filter_specs in cnn_filter_specifiers:
            # Padding information not provided -> Use "same" as default.
            if len(filter_specs) == 3:
                num_filters, kernel_size, strides = filter_specs
                padding = "same"
            # Padding information provided.
            else:
                num_filters, kernel_size, strides, padding = filter_specs

            layers.append(
                tf.keras.layers.Conv2D(
                    filters=num_filters,
                    kernel_size=kernel_size,
                    strides=strides,
                    padding=padding,
                    use_bias=cnn_use_bias,
                    activation=None if cnn_use_layernorm else cnn_activation,
                )
            )
            if cnn_use_layernorm:
                # Use epsilon=1e-5 here (instead of default 1e-3) to be unified with
                # torch. Need to normalize over all axes.
                layers.append(
                    tf.keras.layers.LayerNormalization(axis=[-3, -2, -1], epsilon=1e-5)
                )
                layers.append(tf.keras.layers.Activation(cnn_activation))

        # Create the final CNN network.
        self.cnn = tf.keras.Sequential(layers)

        self.expected_input_dtype = tf.float32

    def call(self, inputs, **kwargs):
        return self.cnn(tf.cast(inputs, self.expected_input_dtype))


class TfCNNTranspose(tf.keras.Model):
    """A model containing a CNNTranspose with N Conv2DTranspose layers.

    All layers share the same activation function, bias setup (use bias or not),
    and LayerNormalization setup (use layer normalization or not), except for the last
    one, which is never activated and never layer norm'd.

    Note that there is no reshaping/flattening nor an additional dense layer at the
    beginning or end of the stack. The input as well as output of the network are 3D
    tensors of dimensions [width x height x num output filters].
    """

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]],
        cnn_transpose_filter_specifiers: List[List[Union[int, List]]],
        cnn_transpose_use_bias: bool = True,
        cnn_transpose_activation: Optional[str] = "relu",
        cnn_transpose_use_layernorm: bool = False,
    ):
        """Initializes a TfCNNTranspose instance.

        Args:
            input_dims: The 3D input dimensions of the network (incoming image).
            cnn_transpose_filter_specifiers: A list of lists, where each item represents
                one Conv2DTranspose layer. Each such Conv2DTranspose layer is further
                specified by the elements of the inner lists. The inner lists follow
                the format: `[number of filters, kernel, stride]` to
                specify a convolutional-transpose layer stacked in order of the
                outer list.
                `kernel` as well as `stride` might be provided as width x height tuples
                OR as single ints representing both dimension (width and height)
                in case of square shapes.
            cnn_transpose_use_bias: Whether to use bias on all Conv2DTranspose layers.
            cnn_transpose_use_layernorm: Whether to insert a LayerNormalization
                functionality in between each Conv2DTranspose layer's outputs and its
                activation.
                The last Conv2DTranspose layer will not be normed, regardless.
            cnn_transpose_activation: The activation function to use after each layer
                (except for the last Conv2DTranspose layer, which is always
                non-activated).
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_transpose_activation = get_activation_fn(
            cnn_transpose_activation, framework="tf2"
        )

        layers = []

        # Input layer.
        layers.append(tf.keras.layers.Input(shape=input_dims))

        for i, (num_filters, kernel_size, strides) in enumerate(
            cnn_transpose_filter_specifiers
        ):
            is_final_layer = i == len(cnn_transpose_filter_specifiers) - 1
            layers.append(
                tf.keras.layers.Conv2DTranspose(
                    filters=num_filters,
                    kernel_size=kernel_size,
                    strides=strides,
                    padding="same",
                    # Last layer is never activated (regardless of config).
                    activation=(
                        None
                        if cnn_transpose_use_layernorm or is_final_layer
                        else cnn_transpose_activation
                    ),
                    # Last layer always uses bias (b/c has no LayerNorm, regardless of
                    # config).
                    use_bias=cnn_transpose_use_bias or is_final_layer,
                )
            )
            if cnn_transpose_use_layernorm and not is_final_layer:
                # Use epsilon=1e-5 here (instead of default 1e-3) to be unified with
                # torch. Need to normalize over all axes.
                layers.append(
                    tf.keras.layers.LayerNormalization(axis=[-3, -2, -1], epsilon=1e-5)
                )
                layers.append(tf.keras.layers.Activation(cnn_transpose_activation))

        # Create the final CNNTranspose network.
        self.cnn_transpose = tf.keras.Sequential(layers)

        self.expected_input_dtype = tf.float32

    def call(self, inputs, **kwargs):
        return self.cnn_transpose(tf.cast(inputs, self.expected_input_dtype))
