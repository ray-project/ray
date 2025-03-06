from dataclasses import dataclass, field
from typing import Callable, List, Optional, Union

from ray.rllib.utils.typing import ConvFilterSpec
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class DefaultModelConfig:
    """Dataclass to configure all default RLlib RLModules.

    Users should NOT use this class for configuring their own custom RLModules, but
    use a custom `model_config` dict with arbitrary (str) keys passed into the
    `RLModuleSpec` used to define the custom RLModule.
    For example:

    .. testcode::

        import gymnasium as gym
        import numpy as np
        from ray.rllib.core.rl_module.rl_module import RLModuleSpec
        from ray.rllib.examples.rl_modules.classes.tiny_atari_cnn_rlm import (
            TinyAtariCNN
        )

        my_rl_module = RLModuleSpec(
            module_class=TinyAtariCNN,
            observation_space=gym.spaces.Box(-1.0, 1.0, (64, 64, 4), np.float32),
            action_space=gym.spaces.Discrete(7),
            # DreamerV3-style stack working on a 64x64, color or 4x-grayscale-stacked,
            # normalized image.
            model_config={
                "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            },
        ).build()

    Only RLlib's default RLModules (defined by the various algorithms) should use
    this dataclass. Pass an instance of it into your algorithm config like so:

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig
        from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

        config = (
            PPOConfig()
            .rl_module(
                model_config=DefaultModelConfig(fcnet_hiddens=[32, 32]),
            )
        )
    """

    # ====================================================
    # MLP stacks
    # ====================================================
    # __sphinx_doc_default_model_config_fcnet_begin__
    #: List containing the sizes (number of nodes) of a fully connected (MLP) stack.
    #: Note that in an encoder-based default architecture with a policy head (and
    #: possible value head), this setting only affects the encoder component. To set the
    #: policy (and value) head sizes, use `post_fcnet_hiddens`, instead. For example,
    #: if you set `fcnet_hiddens=[32, 32]` and `post_fcnet_hiddens=[64]`, you would get
    #: an RLModule with a [32, 32] encoder, a [64, act-dim] policy head, and a [64, 1]
    #: value head (if applicable).
    fcnet_hiddens: List[int] = field(default_factory=lambda: [256, 256])
    #: Activation function descriptor for the stack configured by `fcnet_hiddens`.
    #: Supported values are: 'tanh', 'relu', 'swish' (or 'silu', which is the same),
    #: and 'linear' (or None).
    fcnet_activation: str = "tanh"
    #: Initializer function or class descriptor for the weight/kernel matrices in the
    #: stack configured by `fcnet_hiddens`. Supported values are the initializer names
    #: (str), classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    fcnet_kernel_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `fcnet_kernel_initializer`.
    fcnet_kernel_initializer_kwargs: Optional[dict] = None
    #: Initializer function or class descriptor for the bias vectors in the stack
    #: configured by `fcnet_hiddens`. Supported values are the initializer names (str),
    #: classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    fcnet_bias_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `fcnet_bias_initializer`.
    fcnet_bias_initializer_kwargs: Optional[dict] = None
    # __sphinx_doc_default_model_config_fcnet_end__

    # ====================================================
    # Conv2D stacks
    # ====================================================
    # __sphinx_doc_default_model_config_conv_begin__
    #: List of lists of format [num_out_channels, kernel, stride] defining a Conv2D
    #: stack if the input space is 2D. Each item in the outer list represents one Conv2D
    #: layer. `kernel` and `stride` may be single ints (width and height have same
    #: value) or 2-tuples (int, int) specifying width and height dimensions separately.
    #: If None (default) and the input space is 2D, RLlib tries to find a default filter
    #: setup given the exact input dimensions.
    conv_filters: Optional[ConvFilterSpec] = None
    #: Activation function descriptor for the stack configured by `conv_filters`.
    #: Supported values are: 'tanh', 'relu', 'swish' (or 'silu', which is the same), and
    #: 'linear' (or None).
    conv_activation: str = "relu"
    #: Initializer function or class descriptor for the weight/kernel matrices in the
    #: stack configured by `conv_filters`. Supported values are the initializer names
    #: (str), classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    conv_kernel_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `conv_kernel_initializer`.
    conv_kernel_initializer_kwargs: Optional[dict] = None
    #: Initializer function or class descriptor for the bias vectors in the stack
    #: configured by `conv_filters`. Supported values are the initializer names (str),
    #: classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    conv_bias_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `conv_bias_initializer`.
    conv_bias_initializer_kwargs: Optional[dict] = None
    # __sphinx_doc_default_model_config_conv_end__

    # ====================================================
    # Head configs (e.g. policy- or value function heads)
    # ====================================================
    #: List containing the sizes (number of nodes) of a fully connected (MLP) head (ex.
    #: policy-, value-, or Q-head). Note that in order to configure the encoder
    #: architecture, use `fcnet_hiddens`, instead.
    head_fcnet_hiddens: List[int] = field(default_factory=lambda: [])
    #: Activation function descriptor for the stack configured by `head_fcnet_hiddens`.
    #: Supported values are: 'tanh', 'relu', 'swish' (or 'silu', which is the same),
    #: and 'linear' (or None).
    head_fcnet_activation: str = "relu"
    #: Initializer function or class descriptor for the weight/kernel matrices in the
    #: stack configured by `head_fcnet_hiddens`. Supported values are the initializer
    #: names (str), classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    head_fcnet_kernel_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `head_fcnet_kernel_initializer`.
    head_fcnet_kernel_initializer_kwargs: Optional[dict] = None
    #: Initializer function or class descriptor for the bias vectors in the stack
    #: configured by `head_fcnet_hiddens`. Supported values are the initializer names
    #: (str), classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    head_fcnet_bias_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `head_fcnet_bias_initializer`.
    head_fcnet_bias_initializer_kwargs: Optional[dict] = None

    # ====================================================
    # Continuous action settings
    # ====================================================
    #: If True, for DiagGaussian action distributions (or any other continuous control
    #: distribution), make the second half of the policy's outputs a "free" bias
    #: parameter, rather than state-/NN-dependent nodes. In this case, the number of
    #: nodes of the policy head have the same dimension as the action space as no slots
    #: for log(stddev) are required (only for the mean values).
    free_log_std: bool = False
    #: Whether to clip the log(stddev) when using a DiagGaussian action distribution
    #: (or any other continuous control distribution). This can stabilize training and
    #: avoid very small or large log(stddev) values leading to numerical instabilities
    #: turning outputs to `nan`. The default is to clamp the log(stddev) in between
    #: -20 and 20. Set to float("inf") for no clamping.
    log_std_clip_param: float = 20.0
    #: Whether encoder layers (defined by `fcnet_hiddens` or `conv_filters`) should be
    #: shared between policy- and value function.
    vf_share_layers: bool = True

    # ====================================================
    # LSTM settings
    # ====================================================
    #: Whether to wrap the encoder component (defined by `fcnet_hiddens` or
    #: `conv_filters`) with an LSTM.
    use_lstm: bool = False
    #: The maximum seq len for building the train batch for an LSTM model.
    #:  Defaults to 20.
    max_seq_len: int = 20
    #: The size of the LSTM cell.
    lstm_cell_size: int = 256
    lstm_use_prev_action: bool = False
    lstm_use_prev_reward: bool = False
    #: Initializer function or class descriptor for the weight/kernel matrices in the
    #: LSTM layer. Supported values are the initializer names (str), classes or
    #: functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    lstm_kernel_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `lstm_kernel_initializer`.
    lstm_kernel_initializer_kwargs: Optional[dict] = None
    #: Initializer function or class descriptor for the bias vectors in the stack
    #: configured by the LSTM layer. Supported values are the initializer names (str),
    #: classes or functions listed by the frameworks (`torch`). See
    #: https://pytorch.org/docs/stable/nn.init.html for `torch`. If `None` (default),
    #: the default initializer defined by `torch` is used.
    lstm_bias_initializer: Optional[Union[str, Callable]] = None
    #: Kwargs passed into the initializer function defined through
    #: `lstm_bias_initializer`.
    lstm_bias_initializer_kwargs: Optional[dict] = None
