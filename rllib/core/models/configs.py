from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, Union
from ray.rllib.utils.typing import ViewRequirementsDict
import functools

import gymnasium as gym

from ray.rllib.core.models.base import ModelConfig, Model, Encoder
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.annotations import ExperimentalAPI


@ExperimentalAPI
def _framework_implemented(torch: bool = True, tf2: bool = True):
    """Decorator to check if a model was implemented in a framework.

    Args:
        torch: Whether we can build this model with torch.
        tf2: Whether we can build this model with tf2.

    Returns:
        The decorated function.

    Raises:
        ValueError: If the framework is not available to build.
    """
    accepted = []
    if torch:
        accepted.append("torch")
    if tf2:
        accepted.append("tf2")

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def checked_build(self, framework, **kwargs):
            if framework not in accepted:
                raise ValueError(
                    f"This config does not support framework "
                    f"{framework}. Only frameworks in {accepted} are "
                    f"supported."
                )
            return fn(self, framework, **kwargs)

        return checked_build

    return decorator


@ExperimentalAPI
@dataclass
class _MLPConfig(ModelConfig):
    """Generic configuration class for multi-layer-perceptron based Model classes.

    This is a private class as users should not configure their models directly
    through this class, but use one of the sub-classes, e.g. `MLPHeadConfig` or
    `MLPEncoderConfig`.

    Attributes:
        input_dims: A 1D tensor indicating the input dimension, e.g. `[32]`.
        hidden_layer_dims: The sizes of the hidden layers. If an empty list, only a
            single layer will be built of size `output_dims[0]`.
        hidden_layer_activation: The activation function to use after each layer (
            except for the output).
        hidden_layer_use_layernorm: Whether to insert a LayerNorm functionality
            in between each hidden layer's output and its activation.
        output_dims: A 1D Tensor indicating the size of the output layer. This may be
            set to `None` in case no extra output layer should be built and only the
            layers specified by `hidden_layer_dims` will part of the network.
        output_activation: The activation function to use for the output layer, if any.
        use_bias: Whether to use bias on all dense layers in the network (including
            a possible output layer).
    """

    input_dims: Union[List[int], Tuple[int]] = None
    hidden_layer_dims: Union[List[int], Tuple[int]] = (256, 256)
    hidden_layer_activation: str = "relu"
    hidden_layer_use_layernorm: bool = False
    output_dims: Optional[Union[List[int], Tuple[int]]] = None
    output_activation: str = "linear"
    use_bias: bool = True

    def _validate(self, framework: str = "torch"):
        """Makes sure that settings are valid."""
        if self.input_dims is not None and len(self.input_dims) != 1:
            raise ValueError(
                f"`input_dims` ({self.input_dims}) of MLPConfig must be 1D, "
                "e.g. `[32]`!"
            )
        if self.output_dims is not None and len(self.output_dims) != 1:
            raise ValueError(
                f"`output_dims` ({self.output_dims}) of MLPConfig must be 1D, "
                "e.g. `[32]`!"
            )
        if self.output_dims is None and not self.hidden_layer_dims:
            raise ValueError(
                "If `output_dims` is None, you must specify at least one hidden layer "
                "dim, e.g. `hidden_layer_dims=[32]`! `hidden_layer_dims` must not "
                "be empty in this case."
            )

        # Call these already here to catch errors early on.
        get_activation_fn(self.hidden_layer_activation, framework=framework)
        get_activation_fn(self.output_activation, framework=framework)


@ExperimentalAPI
@dataclass
class MLPHeadConfig(_MLPConfig):
    """Configuration for an MLP head.

    See _MLPConfig for usage details.
    Note that MLPHeads must specify `output_dims` as a 1D tensor. It is not allowed to
    leave `output_dims` as None.

    Example:
    .. code-block:: python
        # Configuration:
        config = MLPHeadConfig(
            input_dims=[4],  # must be 1D tensor
            hidden_layer_dims=[8, 8],
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=False,
            output_dims=[2],  # must be 1D tensor
            output_activation="linear",
        )
        model = config.build(framework="tf2")

        # Resulting stack in pseudocode:
        # Linear(4, 8, bias=True)
        # ReLU()
        # Linear(8, 8, bias=True)
        # ReLU()
        # Linear(8, 2, bias=True)

    Example:
    .. code-block:: python
        # Configuration:
        config = MLPHeadConfig(
            input_dims=[2],
            hidden_layer_dims=[10],
            hidden_layer_activation="silu",
            hidden_layer_use_layernorm=True,
            output_dims=[4],
            output_activation="tanh",
            use_bias=False,
        )
        model = config.build(framework="torch")

        # Resulting stack in pseudocode:
        # Linear(2, 10, bias=False)
        # LayerNorm((10,))  # layer norm always before activation
        # SiLU()
        # Linear(10, 4, bias=False)
        # Tanh()
    """

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        self._validate(framework=framework)

        if framework == "torch":
            from ray.rllib.core.models.torch.heads import TorchMLPHead

            return TorchMLPHead(self)
        else:
            from ray.rllib.core.models.tf.heads import TfMLPHead

            return TfMLPHead(self)


@ExperimentalAPI
@dataclass
class FreeLogStdMLPHeadConfig(_MLPConfig):
    """Configuration for an MLPHead with a floating second half of outputs.

    This model can be useful together with Gaussian Distributions.
    This gaussian distribution would be conditioned as follows:
        - The first half of outputs from this model can be used as
        state-dependent means when conditioning a gaussian distribution
        - The second half are floating free biases that can be used as
        state-independent standard deviations to condition a gaussian distribution.
    The mean values are produced by an MLPHead, while the standard
    deviations are added as floating free biases from a single 1D trainable variable
    (not dependent on the net's inputs).

    The output dimensions of the configured MLPHeadConfig must be even and are
    divided by two to gain the output dimensions of each the mean-net and the
    free std-variable.

    Example:
    .. code-block:: python
        # Configuration:
        config = FreeLogStdMLPHeadConfig(
            input_dims=[2],
            hidden_layer_dims=[16],
            hidden_layer_activation=None,
            hidden_layer_use_layernorm=False,
            output_dims=[8],  # <- this must be an even size
            use_bias=True,
        )
        model = config.build(framework="tf2")

        # Resulting stack in pseudocode:
        # Linear(2, 16, bias=True)
        # Linear(8, 8, bias=True)  # 16 / 2 = 8 -> 8 nodes for the mean
        # Extra variable:
        # Tensor((8,), float32)  # for the free (observation independent) std outputs

    Example:
    .. code-block:: python
        # Configuration:
        config = FreeLogStdMLPHeadConfig(
            input_dims=[2],
            hidden_layer_dims=[31, 100],   # <- last idx must be an even size
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=False,
            output_dims=None,  # use the last hidden layer
            use_bias=False,
        )
        model = config.build(framework="torch")

        # Resulting stack in pseudocode:
        # Linear(2, 31, bias=False)
        # ReLu()
        # Linear(31, 50, bias=False)  # 100 / 2 = 50 -> 50 nodes for the mean
        # ReLu()
        # Extra variable:
        # Tensor((50,), float32)  # for the free (observation independent) std outputs
    """

    def _validate(self, framework: str = "torch"):
        actual_output_dims = (
            [self.hidden_layer_dims[-1]]
            if self.output_dims is None
            else self.output_dims
        )
        if len(actual_output_dims) > 1 or actual_output_dims[0] % 2 == 1:
            raise ValueError(
                "`output_dims` or last value in `hidden_layer_dims` "
                f"({actual_output_dims}) of a FreeLogStdMLPHeadConfig must be a "
                "1D tensor, whose only item is dividable by 2, e.g. `[2]` or `[128]`!"
            )

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        self._validate(framework=framework)

        if framework == "torch":
            from ray.rllib.core.models.torch.heads import TorchFreeLogStdMLPHead

            return TorchFreeLogStdMLPHead(self)
        else:
            from ray.rllib.core.models.tf.heads import TfFreeLogStdMLPHead

            return TfFreeLogStdMLPHead(self)


@ExperimentalAPI
@dataclass
class CNNTransposeHeadConfig(ModelConfig):
    """Configuration for a convolutional transpose head (decoder) network.

    The configured Model transforms 1D-observations into an image space.
    The stack of layers is composed of an initial Dense layer, followed by a sequence
    of Conv2DTranspose layers.
    `input_dims` describes the shape of the (1D) input tensor,
    `initial_image_dims` describes the input into the first Conv2DTranspose
    layer, where the translation from `input_dim` to `initial_image_dims` is done
    via the initial Dense layer (w/o activation, w/o layer-norm, and w/ bias).

    Beyond that, each layer specified by `cnn_transpose_filter_specifiers`
    is followed by an activation function according to `cnn_transpose_activation`.

    `output_dims` is reached after the final Conv2DTranspose layer.
    Not that the last Conv2DTranspose layer is never activated and never layer-norm'd
    regardless of the other settings.

    An example for a single conv2d operation is as follows:
    Input "image" is (4, 4, 24) (not yet strided), padding is "same", stride=2,
    kernel=5.

    First, the input "image" is strided (with stride=2):

    Input image (4x4 (x24)):
    A B C D
    E F G H
    I J K L
    M N O P

    Stride with stride=2 -> (7x7 (x24))
    A 0 B 0 C 0 D
    0 0 0 0 0 0 0
    E 0 F 0 G 0 H
    0 0 0 0 0 0 0
    I 0 J 0 K 0 L
    0 0 0 0 0 0 0
    M 0 N 0 O 0 P

    Then this strided "image" (strided_size=7x7) is padded (exact padding values will be
    computed by the model):

    Padding -> (left=3, right=2, top=3, bottom=2)

    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 A 0 B 0 C 0 D 0 0
    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 E 0 F 0 G 0 H 0 0
    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 I 0 J 0 K 0 L 0 0
    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 M 0 N 0 O 0 P 0 0
    0 0 0 0 0 0 0 0 0 0 0 0
    0 0 0 0 0 0 0 0 0 0 0 0

    Then deconvolution with kernel=5 yields an output "image" of 8x8 (x num output
    filters).

    Attributes:
        input_dims: The input dimensions of the network. This must be a 1D tensor.
        initial_image_dims: The shape of the input to the first
            Conv2DTranspose layer. We will make sure the input is transformed to
            these dims via a preceding initial Dense layer, followed by a reshape,
            before entering the Conv2DTranspose stack.
        cnn_transpose_filter_specifiers: A list of lists, where each element of an inner
            list contains elements of the form
            `[number of channels/filters, [kernel width, kernel height], stride]` to
            specify a convolutional layer stacked in order of the outer list.
        cnn_transpose_activation: The activation function to use after each layer
            (except for the output).
        cnn_transpose_use_layernorm: Whether to insert a LayerNorm functionality
            in between each Conv2DTranspose layer's output and its activation.
        use_bias: Whether to use bias on all Conv2D layers.

    Example:
    .. code-block:: python
        # Configuration:
        config = CNNTransposeHeadConfig(
            input_dims=[10],  # 1D input vector (possibly coming from another NN)
            initial_image_dims=[4, 4, 96],  # first image input to deconv stack
            cnn_transpose_filter_specifiers=[
                [48, [4, 4], 2],
                [24, [4, 4], 2],
                [3, [4, 4], 2],
            ],
            cnn_transpose_activation="silu",  # or "swish", which is the same
            cnn_transpose_use_layernorm=False,
            use_bias=True,
        )
        model = config.build(framework="torch)

        # Resulting stack in pseudocode:
        # Linear(10, 4*4*24)
        # Conv2DTranspose(
        #   in_channels=96, out_channels=48,
        #   kernel_size=[4, 4], stride=2, bias=True,
        # )
        # Swish()
        # Conv2DTranspose(
        #   in_channels=48, out_channels=24,
        #   kernel_size=[4, 4], stride=2, bias=True,
        # )
        # Swish()
        # Conv2DTranspose(
        #   in_channels=24, out_channels=3,
        #   kernel_size=[4, 4], stride=2, bias=True,
        # )

    Example:
    .. code-block:: python
        # Configuration:
        config = CNNTransposeHeadConfig(
            input_dims=[128],  # 1D input vector (possibly coming from another NN)
            initial_image_dims=[4, 4, 32],  # first image input to deconv stack
            cnn_transpose_filter_specifiers=[
                [16, 4, 2],
                [3, 4, 2],
            ],
            cnn_transpose_activation="relu",
            cnn_transpose_use_layernorm=True,
            use_bias=False,
        )
        model = config.build(framework="torch)

        # Resulting stack in pseudocode:
        # Linear(128, 4*4*32, bias=True)  # bias always True for initial dense layer
        # Conv2DTranspose(
        #   in_channels=32, out_channels=16,
        #   kernel_size=[4, 4], stride=2, bias=False,
        # )
        # LayerNorm((-3, -2, -1))  # layer normalize over last 3 axes
        # ReLU()
        # Conv2DTranspose(
        #   in_channels=16, out_channels=3,
        #   kernel_size=[4, 4], stride=2, bias=False,
        # )
    """

    input_dims: Union[List[int], Tuple[int]] = None
    initial_image_dims: Union[List[int], Tuple[int]] = field(
        default_factory=lambda: [4, 4, 96]
    )
    cnn_transpose_filter_specifiers: List[List[Union[int, List[int]]]] = field(
        default_factory=lambda: [[48, [4, 4], 2], [24, [4, 4], 2], [3, [4, 4], 2]]
    )
    cnn_transpose_activation: str = "relu"
    cnn_transpose_use_layernorm: bool = False
    use_bias: bool = True

    def _validate(self, framework: str = "torch"):
        if len(self.input_dims) != 1:
            raise ValueError(
                f"`input_dims` ({self.input_dims}) of CNNTransposeHeadConfig must be a "
                "3D tensor (image-like) with the dimensions meaning: width x height x "
                "num_filters, e.g. `[4, 4, 92]`!"
            )

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        self._validate()

        if framework == "torch":
            from ray.rllib.core.models.torch.heads import TorchCNNTransposeHead

            return TorchCNNTransposeHead(self)

        elif framework == "tf2":
            from ray.rllib.core.models.tf.heads import TfCNNTransposeHead

            return TfCNNTransposeHead(self)


@ExperimentalAPI
@dataclass
class CNNEncoderConfig(ModelConfig):
    """Configuration for a convolutional network.

    The configured CNN encodes 3D-observations into a latent space.
    The stack of layers is composed of a sequence of convolutional layers.
    `input_dims` describes the shape of the input tensor. Beyond that, each layer
    specified by `filter_specifiers` is followed by an activation function according
    to `filter_activation`. `output_dims` is reached by flattening a final
    convolutional layer and applying a linear layer with `output_activation`.
    See ModelConfig for usage details.

    Example:

    .. code-block:: python
        # Configuration:
        config = CNNEncoderConfig(
            input_dims=[84, 84, 3],  # must be 3D tensor (image: w x h x C)
            cnn_filter_specifiers=[
                [16, [8, 8], 4],
                [32, [4, 4], 2],
            ],
            cnn_activation="relu",
            cnn_use_layernorm=False,
            output_dims=[256],  # must be 1D tensor
            output_activation="linear",
            use_bias=True,
        )
        model = config.build(framework="torch")

        # Resulting stack in pseudocode:
        # Conv2D(
        #   in_channels=3, out_channels=16,
        #   kernel_size=[8, 8], stride=[4, 4], bias=True,
        # )
        # ReLU()
        # Conv2D(
        #   in_channels=16, out_channels=32,
        #   kernel_size=[4, 4], stride=[2, 2], bias=True,
        # )
        # ReLU()
        # Conv2D(
        #   in_channels=32, out_channels=1,
        #   kernel_size=[1, 1], stride=[1, 1], bias=True,
        # )
        # Flatten()
        # Linear(121, 256)

    Attributes:
        input_dims: The input dimension of the network. These must be given in the
            form of `(width, height, channels)`.
        cnn_filter_specifiers: A list of lists, where each element of an inner list
            contains elements of the form
            `[number of channels/filters, [kernel width, kernel height], stride]` to
            specify a convolutional layer stacked in order of the outer list.
        cnn_activation: The activation function to use after each layer (
            except for the output).
        cnn_use_layernorm: Whether to insert a LayerNorm functionality
            in between each CNN layer's output and its activation. Note that
            the output layer
        output_activation: The activation function to use for the dense output layer.
        use_bias: Whether to use bias on all Conv2D layers.
    """

    input_dims: Union[List[int], Tuple[int]] = None
    cnn_filter_specifiers: List[List[Union[int, List[int]]]] = field(
        default_factory=lambda: [[16, [4, 4], 2], [32, [4, 4], 2], [64, [8, 8], 2]]
    )
    cnn_activation: str = "relu"
    cnn_use_layernorm: bool = False
    output_dims: Union[List[int], Tuple[int]] = None
    output_activation: str = "linear"
    use_bias: bool = True

    def _validate(self, framework: str = "torch"):
        if len(self.input_dims) != 3:
            raise ValueError(
                f"`input_dims` ({self.input_dims}) of CNNEncoderConfig must be a 3D "
                "tensor (image) with the dimensions meaning: width x height x "
                "channels, e.g. `[64, 64, 3]`!"
            )
        if self.output_dims is None or len(self.output_dims) != 1:
            raise ValueError(
                f"`output_dims` ({self.output_dims}) of CNNEncoderConfig must be "
                "a 1D tensor describing the (after flattening) output dimension of "
                "the CNN, e.g. `[256]`!"
            )

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        self._validate()

        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import TorchCNNEncoder

            return TorchCNNEncoder(self)

        elif framework == "tf2":
            from ray.rllib.core.models.tf.encoder import TfCNNEncoder

            return TfCNNEncoder(self)


@ExperimentalAPI
@dataclass
class MLPEncoderConfig(_MLPConfig):
    """Configuration for an MLP that acts as an encoder.

    See _MLPConfig for usage details.
    Note that MLPEncoders may specify `output_dims` as None in case only the hidden
    layers with their specified activations should be part of the resulting network.

    Example:
    .. code-block:: python
        # Configuration:
        config = MLPEncoderConfig(
            input_dims=[4],  # must be 1D tensor
            hidden_layer_dims=[16],
            hidden_layer_activation="relu",
            hidden_layer_use_layernorm=False,
            output_dims=None,  # maybe None or a 1D tensor
        )
        model = config.build(framework="torch")

        # Resulting stack in pseudocode:
        # Linear(4, 16, bias=True)
        # ReLU()

    Example:
    .. code-block:: python
        # Configuration:
        config = MLPEncoderConfig(
            input_dims=[2],
            hidden_layer_dims=[8, 8],
            hidden_layer_activation="silu",
            hidden_layer_use_layernorm=True,
            output_dims=[4],
            output_activation="tanh",
            use_bias=False,
        )
        model = config.build(framework="tf2")

        # Resulting stack in pseudocode:
        # Linear(2, 8, bias=False)
        # LayerNorm((8,))  # layernorm always before activation
        # SiLU()
        # Linear(8, 8, bias=False)
        # LayerNorm((8,))  # layernorm always before activation
        # SiLU()
        # Linear(8, 4, bias=False)
        # Tanh()
    """

    def _validate(self, framework: str = "torch"):
        super()._validate(framework)
        if self.output_dims is None:
            raise ValueError(
                f"`output_dims` ({self.output_dims}) of MLPEncoderConfig must not be "
                "None! Use a 1D tensor describing the output dimension, e.g. `[32]`!"
            )

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Encoder:
        self._validate()

        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import TorchMLPEncoder

            return TorchMLPEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfMLPEncoder

            return TfMLPEncoder(self)


@ExperimentalAPI
@dataclass
class RecurrentEncoderConfig(ModelConfig):
    """Configuration for an LSTM-based or a GRU-based encoder.

    The encoder consists of N LSTM/GRU layers stacked on top of each other and feeding
    their outputs as inputs to the respective next layer. The internal state is
    structued as (num_layers, B, hidden-size) for all hidden state components, e.g.
    h- and c-states of the LSTM layer(s) or h-state of the GRU layer(s).
    For example, the hidden states of an LSTMEncoder with num_layers=2 and hidden_dim=8
    would be: {"h": (2, B, 8), "c": (2, B, 8)}.

    Example:
    .. code-block:: python
        # Configuration:
        config = RecurrentEncoderConfig(
            recurrent_layer_type="lstm",
            input_dims=[16],  # must be 1D tensor
            hidden_dim=128,
            num_layers=2,
            output_dims=[256],  # maybe None or a 1D tensor
            output_activation="linear",
            use_bias=True,
        )
        model = config.build(framework="torch")

        # Resulting stack in pseudocode:
        # LSTM(16, 128, bias=True)
        # LSTM(128, 128, bias=True)
        # Linear(128, 256, bias=True)

        # Resulting shape of the internal states (c- and h-states):
        # (2, B, 128) for each c- and h-states.

    Example:
    .. code-block:: python
        # Configuration:
        config = RecurrentEncoderConfig(
            recurrent_layer_type="gru",
            input_dims=[32],  # must be 1D tensor
            hidden_dim=64,
            num_layers=1,
            output_dims=None,  # maybe None or a 1D tensor
            use_bias=False,
        )
        model = config.build(framework="torch")

        # Resulting stack in pseudocode:
        # GRU(32, 64, bias=False)

        # Resulting shape of the internal state:
        # (1, B, 64)

    Attributes:
        recurrent_layer_type: The type of the recurrent layer(s).
            Either "lstm" or "gru".
        input_dims: The input dimensions. Must be 1D. This is the 1D shape of the tensor
            that goes into the first recurrent layer.
        hidden_dim: The size of the hidden internal state(s) of the recurrent layer(s).
            For example, for an LSTM, this would be the size of the c- and h-tensors.
        num_layers: The number of recurrent (LSTM or GRU) layers to stack.
        batch_major: Wether the input is batch major (B, T, ..) or
            time major (T, B, ..).
        output_activation: The activation function to use for the linear output layer.
        use_bias: Whether to use bias on all layers in the network.
        view_requirements_dict: The view requirements to use if anything else than
            observation_space or action_space is to be encoded. This signifies an
            advanced use case.
        get_tokenizer_config: A callable that takes a gym.Space and a dict and
            returns a ModelConfig to build tokenizers for observations, actions and
            other spaces that might be present in the view_requirements_dict.
    """

    recurrent_layer_type: str = "lstm"
    hidden_dim: int = None
    num_layers: int = None
    batch_major: bool = True
    output_activation: str = "linear"
    use_bias: bool = True
    view_requirements_dict: ViewRequirementsDict = None
    get_tokenizer_config: Callable[[gym.Space, Dict], ModelConfig] = None

    def _validate(self, framework: str = "torch"):
        """Makes sure that settings are valid."""
        if self.recurrent_layer_type not in ["gru", "lstm"]:
            raise ValueError(
                f"`recurrent_layer_type` ({self.recurrent_layer_type}) of "
                "RecurrentEncoderConfig must be 'gru' or 'lstm'!"
            )
        if self.input_dims is not None and len(self.input_dims) != 1:
            raise ValueError(
                f"`input_dims` ({self.input_dims}) of RecurrentEncoderConfig must be "
                "1D, e.g. `[32]`!"
            )

        # Call these already here to catch errors early on.
        get_activation_fn(self.output_activation, framework=framework)

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Encoder:
        if (
            self.get_tokenizer_config is not None
            or self.view_requirements_dict is not None
        ):
            raise NotImplementedError(
                "RecurrentEncoderConfig does not support configuring Models that "
                "encode depending on view_requirements or have a custom tokenizer. "
                "Therefore, this config expects `view_requirements_dict=None` and "
                "`get_tokenizer_config=None`."
            )

        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import (
                TorchGRUEncoder as GRU,
                TorchLSTMEncoder as LSTM,
            )
        else:
            from ray.rllib.core.models.tf.encoder import (
                TfGRUEncoder as GRU,
                TfLSTMEncoder as LSTM,
            )

        if self.recurrent_layer_type == "lstm":
            return LSTM(self)
        else:
            return GRU(self)


@ExperimentalAPI
@dataclass
class ActorCriticEncoderConfig(ModelConfig):
    """Configuration for an ActorCriticEncoder.

    The base encoder functions like other encoders in RLlib. It is wrapped by the
    ActorCriticEncoder to provides a shared encoder Model to use in RLModules that
    provides twofold outputs: one for the actor and one for the critic. See
    ModelConfig for usage details.

    Attributes:
        base_encoder_config: The configuration for the wrapped encoder(s).
        shared: Whether the base encoder is shared between the actor and critic.
    """

    base_encoder_config: ModelConfig = None
    shared: bool = True

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import (
                TorchActorCriticEncoder,
            )

            return TorchActorCriticEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfActorCriticEncoder

            return TfActorCriticEncoder(self)
