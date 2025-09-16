import abc
from typing import List, Optional, Tuple, Union


from ray.rllib.core.columns import Columns
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.policy.rnn_sequencing import get_fold_unfold_fns
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import DeveloperAPI

# Top level keys that unify model i/o.
ENCODER_OUT: str = "encoder_out"
# For Actor-Critic algorithms, these signify data related to the actor and critic
ACTOR: str = "actor"
CRITIC: str = "critic"


@ExperimentalAPI
class Model(abc.ABC):
    """Framework-agnostic base class for RLlib models.

    Models are low-level neural network components that offer input- and
    output-specification, a forward method, and a get_initial_state method. Models
    are composed in RLModules.

    Usage Example together with ModelConfig:

    .. testcode::

        from ray.rllib.core.models.base import Model
        from ray.rllib.core.models.configs import ModelConfig
        from ray.rllib.core.models.configs import ModelConfig
        from dataclasses import dataclass

        class MyModel(Model):
            def __init__(self, config):
                super().__init__(config)
                self.my_param = config.my_param * 2

            def _forward(self, input_dict):
                return input_dict["obs"] * self.my_param


        @dataclass
        class MyModelConfig(ModelConfig):
            my_param: int = 42

            def build(self, framework: str):
                if framework == "bork":
                    return MyModel(self)


        config = MyModelConfig(my_param=3)
        model = config.build(framework="bork")
        print(model._forward({"obs": 1}))

    .. testoutput::

        6

    """

    def __init__(self, config: ModelConfig):
        self.config = config

    def __init_subclass__(cls, **kwargs):
        # Automatically add a __post_init__ method to all subclasses of Model.
        # This method is called after the __init__ method of the subclass.
        def init_decorator(previous_init):
            def new_init(self, *args, **kwargs):
                previous_init(self, *args, **kwargs)
                if type(self) is cls:
                    self.__post_init__()

            return new_init

        cls.__init__ = init_decorator(cls.__init__)

    def __post_init__(self):
        """Called automatically after the __init__ method of the subclasses.

        The module first calls the __init__ method of the subclass, With in the
        __init__ you should call the super().__init__ method. Then after the __init__
        method of the subclass is called, the __post_init__ method is called.

        This is a good place to do any initialization that requires access to the
        subclass's attributes.
        """
        self._input_specs = self.get_input_specs()
        self._output_specs = self.get_output_specs()

    def get_input_specs(self) -> Optional[Spec]:
        """Returns the input specs of this model.

        Override `get_input_specs` to define your own input specs.
        This method should not be called often, e.g. every forward pass.
        Instead, it should be called once at instantiation to define Model.input_specs.

        Returns:
            Spec: The input specs.
        """
        return None

    def get_output_specs(self) -> Optional[Spec]:
        """Returns the output specs of this model.

        Override `get_output_specs` to define your own output specs.
        This method should not be called often, e.g. every forward pass.
        Instead, it should be called once at instantiation to define Model.output_specs.

        Returns:
            Spec: The output specs.
        """
        return None

    @property
    def input_specs(self) -> Spec:
        """Returns the input spec of this model."""
        return self._input_specs

    @input_specs.setter
    def input_specs(self, spec: Spec) -> None:
        raise ValueError(
            "`input_specs` cannot be set directly. Override "
            "Model.get_input_specs() instead. Set Model._input_specs if "
            "you want to override this behavior."
        )

    @property
    def output_specs(self) -> Spec:
        """Returns the output specs of this model."""
        return self._output_specs

    @output_specs.setter
    def output_specs(self, spec: Spec) -> None:
        raise ValueError(
            "`output_specs` cannot be set directly. Override "
            "Model.get_output_specs() instead. Set Model._output_specs if "
            "you want to override this behavior."
        )

    def get_initial_state(self) -> Union[dict, List[TensorType]]:
        """Returns the initial state of the Model.

        It can be left empty if this Model is not stateful.
        """
        return dict()

    @abc.abstractmethod
    def _forward(self, input_dict: dict, **kwargs) -> dict:
        """Returns the output of this model for the given input.

        This method is called by the forwarding method of the respective framework
        that is itself wrapped by RLlib in order to check model inputs and outputs.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            dict: The output tensors.
        """

    @abc.abstractmethod
    def get_num_parameters(self) -> Tuple[int, int]:
        """Returns a tuple of (num trainable params, num non-trainable params)."""

    @abc.abstractmethod
    def _set_to_dummy_weights(self, value_sequence=(-0.02, -0.01, 0.01, 0.02)) -> None:
        """Helper method to set all weights to deterministic dummy values.

        Calling this method on two `Models` that have the same architecture using
        the exact same `value_sequence` arg should make both models output the exact
        same values on arbitrary inputs. This will work, even if the two `Models`
        are of different DL frameworks.

        Args:
            value_sequence: Looping through the list of all parameters (weight matrices,
                bias tensors, etc..) of this model, in each iteration i, we set all
                values in this parameter to `value_sequence[i % len(value_sequence)]`
                (round robin).

        Example:
            TODO:
        """


@ExperimentalAPI
class Encoder(Model, abc.ABC):
    """The framework-agnostic base class for all RLlib encoders.

    Encoders are used to transform observations to a latent space.
    Therefore, their `input_specs` contains the observation space dimensions.
    Similarly, their `output_specs` contains the latent space dimensions.
    Encoders can be recurrent, in which case the state should be part of input- and
    output_specs. The latent vectors produced by an encoder are fed into subsequent
    "heads". Any implementation of Encoder should also be callable. This should be done
    by also inheriting from a framework-specific model base-class, s.a. TorchModel or
    TfModel.

    Abstract illustration of typical flow of tensors:

    Inputs
    |
    Encoder
    |      \
    Head1  Head2
    |      /
    Outputs

    Outputs of encoders are generally of shape (B, latent_dim) or (B, T, latent_dim).
    That is, for time-series data, we encode into the latent space for each time step.
    This should be reflected in the `output_specs`.

    Usage example together with a ModelConfig:

    .. testcode::

        from dataclasses import dataclass
        import numpy as np

        from ray.rllib.core.columns import Columns
        from ray.rllib.core.models.base import Encoder, ENCODER_OUT
        from ray.rllib.core.models.configs import ModelConfig
        from ray.rllib.policy.sample_batch import SampleBatch

        class NumpyEncoder(Encoder):
            def __init__(self, config):
                super().__init__(config)
                self.factor = config.factor

            def __call__(self, *args, **kwargs):
                # This is a dummy method to do checked forward passes.
                return self._forward(*args, **kwargs)

            def _forward(self, input_dict, **kwargs):
                obs = input_dict[Columns.OBS]
                return {
                    ENCODER_OUT: np.array(obs) * self.factor,
                    Columns.STATE_OUT: (
                        np.array(input_dict[Columns.STATE_IN])
                        * self.factor
                    ),
                }

        @dataclass
        class NumpyEncoderConfig(ModelConfig):
            factor: int = None

            def build(self, framework: str):
                return NumpyEncoder(self)

        config = NumpyEncoderConfig(factor=2)
        encoder = NumpyEncoder(config)
        print(encoder({Columns.OBS: 1, Columns.STATE_IN: 2}))

    .. testoutput::

        {'encoder_out': 2, 'state_out': 4}

    """

    @abc.abstractmethod
    def _forward(self, input_dict: dict, **kwargs) -> dict:
        """Returns the latent of the encoder for the given inputs.

        This method is called by the forwarding method of the respective framework
        that is itself wrapped by RLlib in order to check model inputs and outputs.

        The input dict contains at minimum the observation and the state of the encoder
        (None for stateless encoders).
        The output dict contains at minimum the latent and the state of the encoder
        (None for stateless encoders).
        To establish an agreement between the encoder and RLModules, these values
        have the fixed keys `Columns.OBS` for the `input_dict`,
        and `ACTOR` and `CRITIC` for the returned dict.

        Args:
            input_dict: The input tensors. Must contain at a minimum the keys
                Columns.OBS and Columns.STATE_IN (which might be None for stateless
                encoders).
            **kwargs: Forward compatibility kwargs.

        Returns:
            The output tensors. Must contain at a minimum the key ENCODER_OUT.
        """


@ExperimentalAPI
class ActorCriticEncoder(Encoder):
    """An encoder that potentially holds two stateless encoders.

    This is a special case of Encoder that can either enclose a single,
    shared encoder or two separate encoders: One for the actor and one for the
    critic. The two encoders are of the same type, and we can therefore make the
    assumption that they have the same input and output specs.
    """

    framework = None

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)

        if config.shared:
            self.encoder = config.base_encoder_config.build(framework=self.framework)
        else:
            self.actor_encoder = config.base_encoder_config.build(
                framework=self.framework
            )
            self.critic_encoder = None
            if not config.inference_only:
                self.critic_encoder = config.base_encoder_config.build(
                    framework=self.framework
                )

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        if self.config.shared:
            encoder_outs = self.encoder(inputs, **kwargs)
            return {
                ENCODER_OUT: {
                    ACTOR: encoder_outs[ENCODER_OUT],
                    **(
                        {}
                        if self.config.inference_only
                        else {CRITIC: encoder_outs[ENCODER_OUT]}
                    ),
                }
            }
        else:
            # Encoders should not modify inputs, so we can pass the same inputs
            actor_out = self.actor_encoder(inputs, **kwargs)
            if self.critic_encoder:
                critic_out = self.critic_encoder(inputs, **kwargs)

            return {
                ENCODER_OUT: {
                    ACTOR: actor_out[ENCODER_OUT],
                    **(
                        {}
                        if self.config.inference_only
                        else {CRITIC: critic_out[ENCODER_OUT]}
                    ),
                }
            }


@ExperimentalAPI
class StatefulActorCriticEncoder(Encoder):
    """An encoder that potentially holds two potentially stateful encoders.

    This is a special case of Encoder that can either enclose a single,
    shared encoder or two separate encoders: One for the actor and one for the
    critic. The two encoders are of the same type, and we can therefore make the
    assumption that they have the same input and output specs.

    If this encoder wraps a single encoder, state in input- and output dicts
    is simply stored under the key `STATE_IN` and `STATE_OUT`, respectively.
    If this encoder wraps two encoders, state in input- and output dicts is
    stored under the keys `(STATE_IN, ACTOR)` and `(STATE_IN, CRITIC)` and
    `(STATE_OUT, ACTOR)` and `(STATE_OUT, CRITIC)`, respectively.
    """

    framework = None

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)

        if config.shared:
            self.encoder = config.base_encoder_config.build(framework=self.framework)
        else:
            self.actor_encoder = config.base_encoder_config.build(
                framework=self.framework
            )
            self.critic_encoder = config.base_encoder_config.build(
                framework=self.framework
            )

    @override(Model)
    def get_initial_state(self):
        if self.config.shared:
            return self.encoder.get_initial_state()
        else:
            return {
                ACTOR: self.actor_encoder.get_initial_state(),
                CRITIC: self.critic_encoder.get_initial_state(),
            }

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        outputs = {}

        if self.config.shared:
            outs = self.encoder(inputs, **kwargs)
            encoder_out = outs.pop(ENCODER_OUT)
            outputs[ENCODER_OUT] = {ACTOR: encoder_out, CRITIC: encoder_out}
            outputs[Columns.STATE_OUT] = outs[Columns.STATE_OUT]
        else:
            # Shallow copy inputs so that we can add states without modifying
            # original dict.
            actor_inputs = inputs.copy()
            critic_inputs = inputs.copy()
            actor_inputs[Columns.STATE_IN] = inputs[Columns.STATE_IN][ACTOR]
            critic_inputs[Columns.STATE_IN] = inputs[Columns.STATE_IN][CRITIC]

            actor_out = self.actor_encoder(actor_inputs, **kwargs)
            critic_out = self.critic_encoder(critic_inputs, **kwargs)

            outputs[ENCODER_OUT] = {
                ACTOR: actor_out[ENCODER_OUT],
                CRITIC: critic_out[ENCODER_OUT],
            }

            outputs[Columns.STATE_OUT] = {
                ACTOR: actor_out[Columns.STATE_OUT],
                CRITIC: critic_out[Columns.STATE_OUT],
            }

        return outputs


@DeveloperAPI
def tokenize(tokenizer: Encoder, inputs: dict, framework: str) -> dict:
    """Tokenizes the observations from the input dict.

    Args:
        tokenizer: The tokenizer to use.
        inputs: The input dict.

    Returns:
        The output dict.
    """
    # Tokenizer may depend solely on observations.
    obs = inputs[Columns.OBS]
    tokenizer_inputs = {Columns.OBS: obs}
    size = list(obs.size() if framework == "torch" else obs.shape)
    b_dim, t_dim = size[:2]
    fold, unfold = get_fold_unfold_fns(b_dim, t_dim, framework=framework)
    # Push through the tokenizer encoder.
    out = tokenizer(fold(tokenizer_inputs))
    out = out[ENCODER_OUT]
    # Then unfold batch- and time-dimensions again.
    return unfold(out)
