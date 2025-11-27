from typing import Callable, Dict, List, Optional, Tuple, Union

from ray.rllib.core.models.torch.utils import Stride2D
from ray.rllib.models.torch.misc import (
    same_padding,
    same_padding_transpose_after_stride,
    valid_padding,
)
from ray.rllib.models.utils import get_activation_fn, get_initializer_fn
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLP(nn.Module):
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
        hidden_layer_activation: Union[str, Callable] = "relu",
        hidden_layer_use_bias: bool = True,
        hidden_layer_use_layernorm: bool = False,
        hidden_layer_weights_initializer: Optional[Union[str, Callable]] = None,
        hidden_layer_weights_initializer_config: Optional[Union[str, Callable]] = None,
        hidden_layer_bias_initializer: Optional[Union[str, Callable]] = None,
        hidden_layer_bias_initializer_config: Optional[Dict] = None,
        output_dim: Optional[int] = None,
        output_use_bias: bool = True,
        output_activation: Union[str, Callable] = "linear",
        output_weights_initializer: Optional[Union[str, Callable]] = None,
        output_weights_initializer_config: Optional[Dict] = None,
        output_bias_initializer: Optional[Union[str, Callable]] = None,
        output_bias_initializer_config: Optional[Dict] = None,
    ):
        """Initialize a TorchMLP object.

        Args:
            input_dim: The input dimension of the network. Must not be None.
            hidden_layer_dims: The sizes of the hidden layers. If an empty list, only a
                single layer will be built of size `output_dim`.
            hidden_layer_use_layernorm: Whether to insert a LayerNormalization
                functionality in between each hidden layer's output and its activation.
            hidden_layer_use_bias: Whether to use bias on all dense layers (excluding
                the possible separate output layer).
            hidden_layer_activation: The activation function to use after each layer
                (except for the output). Either a torch.nn.[activation fn] callable or
                the name thereof, or an RLlib recognized activation name,
                e.g. "ReLU", "relu", "tanh", "SiLU", or "linear".
            hidden_layer_weights_initializer: The initializer function or class to use
                forweights initialization in the hidden layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            hidden_layer_weights_initializer_config: Configuration to pass into the
                initializer defined in `hidden_layer_weights_initializer`.
            hidden_layer_bias_initializer: The initializer function or class to use for
                bias initialization in the hidden layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            hidden_layer_bias_initializer_config: Configuration to pass into the
                initializer defined in `hidden_layer_bias_initializer`.
            output_dim: The output dimension of the network. If None, no specific output
                layer will be added and the last layer in the stack will have
                size=`hidden_layer_dims[-1]`.
            output_use_bias: Whether to use bias on the separate output layer,
                if any.
            output_activation: The activation function to use for the output layer
                (if any). Either a torch.nn.[activation fn] callable or
                the name thereof, or an RLlib recognized activation name,
                e.g. "ReLU", "relu", "tanh", "SiLU", or "linear".
            output_layer_weights_initializer: The initializer function or class to use
                for weights initialization in the output layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            output_layer_weights_initializer_config: Configuration to pass into the
                initializer defined in `output_layer_weights_initializer`.
            output_layer_bias_initializer: The initializer function or class to use for
                bias initialization in the output layers. If `None` the default
                initializer of the respective dense layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            output_layer_bias_initializer_config: Configuration to pass into the
                initializer defined in `output_layer_bias_initializer`.
        """
        super().__init__()
        assert input_dim > 0

        self.input_dim = input_dim

        hidden_activation = get_activation_fn(
            hidden_layer_activation, framework="torch"
        )
        hidden_weights_initializer = get_initializer_fn(
            hidden_layer_weights_initializer, framework="torch"
        )
        hidden_bias_initializer = get_initializer_fn(
            hidden_layer_bias_initializer, framework="torch"
        )
        output_weights_initializer = get_initializer_fn(
            output_weights_initializer, framework="torch"
        )
        output_bias_initializer = get_initializer_fn(
            output_bias_initializer, framework="torch"
        )

        layers = []
        dims = (
            [self.input_dim]
            + list(hidden_layer_dims)
            + ([output_dim] if output_dim else [])
        )
        for i in range(0, len(dims) - 1):
            # Whether we are already processing the last (special) output layer.
            is_output_layer = output_dim is not None and i == len(dims) - 2

            layer = nn.Linear(
                dims[i],
                dims[i + 1],
                bias=output_use_bias if is_output_layer else hidden_layer_use_bias,
            )
            # Initialize layers, if necessary.
            if is_output_layer:
                # Initialize output layer weigths if necessary.
                if output_weights_initializer:
                    output_weights_initializer(
                        layer.weight, **output_weights_initializer_config or {}
                    )
                # Initialize output layer bias if necessary.
                if output_bias_initializer:
                    output_bias_initializer(
                        layer.bias, **output_bias_initializer_config or {}
                    )
            # Must be hidden.
            else:
                # Initialize hidden layer weights if necessary.
                if hidden_layer_weights_initializer:
                    hidden_weights_initializer(
                        layer.weight, **hidden_layer_weights_initializer_config or {}
                    )
                # Initialize hidden layer bias if necessary.
                if hidden_layer_bias_initializer:
                    hidden_bias_initializer(
                        layer.bias, **hidden_layer_bias_initializer_config or {}
                    )

            layers.append(layer)

            # We are still in the hidden layer section: Possibly add layernorm and
            # hidden activation.
            if not is_output_layer:
                # Insert a layer normalization in between layer's output and
                # the activation.
                if hidden_layer_use_layernorm:
                    # We use an epsilon of 0.001 here to mimick the Tf default behavior.
                    layers.append(nn.LayerNorm(dims[i + 1], eps=0.001))
                # Add the activation function.
                if hidden_activation is not None:
                    layers.append(hidden_activation())

        # Add output layer's (if any) activation.
        output_activation = get_activation_fn(output_activation, framework="torch")
        if output_dim is not None and output_activation is not None:
            layers.append(output_activation())

        self.mlp = nn.Sequential(*layers)

    def forward(self, x):
        return self.mlp(x)


class TorchCNN(nn.Module):
    """A model containing a CNN with N Conv2D layers.

    All layers share the same activation function, bias setup (use bias or not),
    and LayerNorm setup (use layer normalization or not).

    Note that there is no flattening nor an additional dense layer at the end of the
    stack. The output of the network is a 3D tensor of dimensions
    [width x height x num output filters].
    """

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int, ...]],
        cnn_filter_specifiers: List[List[Union[int, List]]],
        cnn_use_bias: bool = True,
        cnn_use_layernorm: bool = False,
        cnn_activation: str = "relu",
        cnn_kernel_initializer: Optional[Union[str, Callable]] = None,
        cnn_kernel_initializer_config: Optional[Dict] = None,
        cnn_bias_initializer: Optional[Union[str, Callable]] = None,
        cnn_bias_initializer_config: Optional[Dict] = None,
    ):
        """Initializes a TorchCNN instance.

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
            cnn_kernel_initializer: The initializer function or class to use for kernel
                initialization in the CNN layers. If `None` the default initializer of
                the respective CNN layer is used. Note, only the in-place
                initializers, i.e. ending with an underscore "_" are allowed.
            cnn_kernel_initializer_config: Configuration to pass into the initializer
                defined in `cnn_kernel_initializer`.
            cnn_bias_initializer: The initializer function or class to use for bias
                initializationcin the CNN layers. If `None` the default initializer of
                the respective CNN layer is used. Note, only the in-place initializers,
                i.e. ending with an underscore "_" are allowed.
            cnn_bias_initializer_config: Configuration to pass into the initializer
                defined in `cnn_bias_initializer`.
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_activation = get_activation_fn(cnn_activation, framework="torch")
        cnn_kernel_initializer = get_initializer_fn(
            cnn_kernel_initializer, framework="torch"
        )
        cnn_bias_initializer = get_initializer_fn(
            cnn_bias_initializer, framework="torch"
        )
        layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for filter_specs in cnn_filter_specifiers:
            # Padding information not provided -> Use "same" as default.
            if len(filter_specs) == 3:
                out_depth, kernel_size, strides = filter_specs
                padding = "same"
            # Padding information provided.
            else:
                out_depth, kernel_size, strides, padding = filter_specs

            # Pad like in tensorflow's SAME/VALID mode.
            if padding == "same":
                padding_size, out_size = same_padding(in_size, kernel_size, strides)
                layers.append(nn.ZeroPad2d(padding_size))
            # No actual padding is performed for "valid" mode, but we will still
            # compute the output size (input for the next layer).
            else:
                out_size = valid_padding(in_size, kernel_size, strides)

            layer = nn.Conv2d(
                in_depth, out_depth, kernel_size, strides, bias=cnn_use_bias
            )

            # Initialize CNN layer kernel if necessary.
            if cnn_kernel_initializer:
                cnn_kernel_initializer(
                    layer.weight, **cnn_kernel_initializer_config or {}
                )
            # Initialize CNN layer bias if necessary.
            if cnn_bias_initializer:
                cnn_bias_initializer(layer.bias, **cnn_bias_initializer_config or {})

            layers.append(layer)

            # Layernorm.
            if cnn_use_layernorm:
                # We use an epsilon of 0.001 here to mimick the Tf default behavior.
                layers.append(LayerNorm1D(out_depth, eps=0.001))
            # Activation.
            if cnn_activation is not None:
                layers.append(cnn_activation())

            in_size = out_size
            in_depth = out_depth

        # Create the CNN.
        self.cnn = nn.Sequential(*layers)

    def forward(self, inputs):
        # Permute b/c data comes in as channels_last ([B, dim, dim, channels]) ->
        # Convert to `channels_first` for torch:
        inputs = inputs.permute(0, 3, 1, 2)
        out = self.cnn(inputs)
        # Permute back to `channels_last`.
        return out.permute(0, 2, 3, 1)


class TorchCNNTranspose(nn.Module):
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
        input_dims: Union[List[int], Tuple[int, ...]],
        cnn_transpose_filter_specifiers: List[List[Union[int, List]]],
        cnn_transpose_use_bias: bool = True,
        cnn_transpose_activation: str = "relu",
        cnn_transpose_use_layernorm: bool = False,
        cnn_transpose_kernel_initializer: Optional[Union[str, Callable]] = None,
        cnn_transpose_kernel_initializer_config: Optional[Dict] = None,
        cnn_transpose_bias_initializer: Optional[Union[str, Callable]] = None,
        cnn_transpose_bias_initializer_config: Optional[Dict] = None,
    ):
        """Initializes a TorchCNNTranspose instance.

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
            cnn_transpose_kernel_initializer: The initializer function or class to use
                for kernel initialization in the CNN layers. If `None` the default
                initializer of the respective CNN layer is used. Note, only the
                in-place initializers, i.e. ending with an underscore "_" are allowed.
            cnn_transpose_kernel_initializer_config: Configuration to pass into the
                initializer defined in `cnn_transpose_kernel_initializer`.
            cnn_transpose_bias_initializer: The initializer function or class to use for
                bias initialization in the CNN layers. If `None` the default initializer
                of the respective CNN layer is used. Note, only the in-place
                initializers, i.e. ending with an underscore "_" are allowed.
            cnn_transpose_bias_initializer_config: Configuration to pass into the
                initializer defined in `cnn_transpose_bias_initializer`.
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_transpose_activation = get_activation_fn(
            cnn_transpose_activation, framework="torch"
        )
        cnn_transpose_kernel_initializer = get_initializer_fn(
            cnn_transpose_kernel_initializer, framework="torch"
        )
        cnn_transpose_bias_initializer = get_initializer_fn(
            cnn_transpose_bias_initializer, framework="torch"
        )

        layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for i, (out_depth, kernel, stride) in enumerate(
            cnn_transpose_filter_specifiers
        ):
            is_final_layer = i == len(cnn_transpose_filter_specifiers) - 1

            # Resolve stride and kernel width/height values if only int given (squared).
            s_w, s_h = (stride, stride) if isinstance(stride, int) else stride
            k_w, k_h = (kernel, kernel) if isinstance(kernel, int) else kernel

            # Stride the incoming image first.
            stride_layer = Stride2D(in_size[0], in_size[1], s_w, s_h)
            layers.append(stride_layer)
            # Then 0-pad (like in tensorflow's SAME mode).
            # This will return the necessary padding such that for stride=1, the output
            # image has the same size as the input image, for stride=2, the output image
            # is 2x the input image, etc..
            padding, out_size = same_padding_transpose_after_stride(
                (stride_layer.out_width, stride_layer.out_height), kernel, stride
            )
            layers.append(nn.ZeroPad2d(padding))  # left, right, top, bottom
            # Then do the Conv2DTranspose operation
            # (now that we have padded and strided manually, w/o any more padding using
            # stride=1).

            layer = nn.ConvTranspose2d(
                in_depth,
                out_depth,
                kernel,
                # Force-set stride to 1 as we already took care of it.
                1,
                # Disable torch auto-padding (torch interprets the padding setting
                # as: dilation (==1.0) * [`kernel` - 1] - [`padding`]).
                padding=(k_w - 1, k_h - 1),
                # Last layer always uses bias (b/c has no LayerNorm, regardless of
                # config).
                bias=cnn_transpose_use_bias or is_final_layer,
            )

            # Initialize CNN Transpose layer kernel if necessary.
            if cnn_transpose_kernel_initializer:
                cnn_transpose_kernel_initializer(
                    layer.weight, **cnn_transpose_kernel_initializer_config or {}
                )
            # Initialize CNN Transpose layer bias if necessary.
            if cnn_transpose_bias_initializer:
                cnn_transpose_bias_initializer(
                    layer.bias, **cnn_transpose_bias_initializer_config or {}
                )

            layers.append(layer)
            # Layernorm (never for final layer).
            if cnn_transpose_use_layernorm and not is_final_layer:
                layers.append(LayerNorm1D(out_depth, eps=0.001))
            # Last layer is never activated (regardless of config).
            if cnn_transpose_activation is not None and not is_final_layer:
                layers.append(cnn_transpose_activation())

            in_size = (out_size[0], out_size[1])
            in_depth = out_depth

        # Create the final CNNTranspose network.
        self.cnn_transpose = nn.Sequential(*layers)

    def forward(self, inputs):
        # Permute b/c data comes in as [B, dim, dim, channels]:
        out = inputs.permute(0, 3, 1, 2)
        out = self.cnn_transpose(out)
        return out.permute(0, 2, 3, 1)


class LayerNorm1D(nn.Module):
    def __init__(self, num_features, **kwargs):
        super().__init__()
        self.layer_norm = nn.LayerNorm(num_features, **kwargs)

    def forward(self, x):
        # x shape: (B, dim, dim, channels).
        batch_size, channels, h, w = x.size()
        # Reshape to (batch_size * height * width, channels) for LayerNorm
        x = x.permute(0, 2, 3, 1).reshape(-1, channels)
        # Apply LayerNorm
        x = self.layer_norm(x)
        # Reshape back to (batch_size, dim, dim, channels)
        x = x.reshape(batch_size, h, w, channels).permute(0, 3, 1, 2)
        return x
