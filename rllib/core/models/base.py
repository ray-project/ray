import abc
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from ray.rllib import SampleBatch
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.core.models.specs.checker import convert_to_canonical_format
from ray.rllib.utils.typing import TensorType

# Top level keys that unify model i/o.
STATE_IN: str = "state_in"
STATE_OUT: str = "state_out"
ENCODER_OUT: str = "encoder_out"
# For Actor-Critic algorithms, these signify data related to the actor and critic
ACTOR: str = "actor"
CRITIC: str = "critic"


@ExperimentalAPI
@dataclass
class ModelConfig(abc.ABC):
    """Base class for model configurations.

    ModelConfigs are framework-agnostic.
    A ModelConfig is usually built by RLModules after getting it from a Catalog object.
    It is therefore a means of configuration for RLModules. However, ModelConfigs are
    not restricted to be used only with Catalog or RLModules.
    A usage Example together with a Model can be found in the Model.

    Args:
        input_dims: The input dimensions of the network
        output_dims: The output dimensions of the network.
    """

    input_dims: Union[List[int], Tuple[int]] = None
    output_dims: Union[List[int], Tuple[int]] = None

    @abc.abstractmethod
    def build(self, framework: str):
        """Builds the model.

        Args:
            framework: The framework to use for building the model.
        """
        raise NotImplementedError


class Model(abc.ABC):
    """Framework-agnostic base class for RLlib models.

    Models are low-level neural network components that offer input- and
    output-specification, a forward method, and a get_initial_state method. Models
    are composed in RLModules.

    Usage Example together with ModelConfig:

    .. testcode::

        from ray.rllib.core.models.base import Model, ModelConfig
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
                if type(self) == cls:
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

    def get_initial_state(self) -> Union[NestedDict, List[TensorType]]:
        """Returns the initial state of the Model.

        It can be left empty if this Model is not stateful.
        """
        return NestedDict()

    @abc.abstractmethod
    def _forward(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        """Returns the output of this model for the given input.

        This method is called by the forwarding method of the respective framework
        that is itself wrapped by RLlib in order to check model inputs and outputs.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        raise NotImplementedError


class Encoder(Model, abc.ABC):
    """The framework-agnostic base class for all encoders RLlib produces.

    Encoders are used to encode observations into a latent space in RLModules.
    Therefore, their `input_specs` contains the observation space dimensions.
    Similarly, their `output_specs` contains the latent space dimensions.
    Encoders can be recurrent, in which case the state should be part of input- and
    output_specs. The latents that are produced by an encoder are fed into subsequent
    heads. Any implementation of Encoder should also be callable. This should be done
    by also inheriting from a framework-specific model base-class, s.a. TorchModel.

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

    Usage Example together with a ModelConfig:

    .. testcode::

        from ray.rllib.core.models.base import ModelConfig
        from ray.rllib.core.models.base import ENCODER_OUT, STATE_IN, STATE_OUT, Encoder
        from ray.rllib.policy.sample_batch import SampleBatch
        from dataclasses import dataclass
        import numpy as np

        class NumpyEncoder(Encoder):
            def __init__(self, config):
                super().__init__(config)
                self.factor = config.factor

            @check_input_specs("input_specs")
            @check_output_specs("output_specs")
            def __call__(self, *args, **kwargs):
                # This is a dummy method to do checked forward passes.
                return self._forward(*args, **kwargs)

            def _forward(self, input_dict, **kwargs):
                obs = input_dict[SampleBatch.OBS]
                return {
                    ENCODER_OUT: np.array(obs) * self.factor,
                    STATE_OUT: np.array(input_dict[STATE_IN]) * self.factor,
                }

        @dataclass
        class NumpyEncoderConfig(ModelConfig):
            factor: int = None

            def build(self, framework: str):
                return NumpyEncoder(self)

        config = NumpyEncoderConfig(factor=2)
        encoder = NumpyEncoder(config)
        print(encoder({SampleBatch.OBS: 1, STATE_IN: 2}))

    .. testoutput::

        {'encoder_out': 2, 'state_out': 4}

    """

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return convert_to_canonical_format([SampleBatch.OBS, STATE_IN])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return convert_to_canonical_format([ENCODER_OUT, STATE_OUT])

    @abc.abstractmethod
    def _forward(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        """Returns the latent of the encoder for the given inputs.

        This method is called by the forwarding method of the respective framework
        that is itself wrapped by RLlib in order to check model inputs and outputs.

        The input dict contains at minimum the observation and the state of the encoder.
        The output dict contains at minimum the latent and the state of the encoder.
        These values have the keys `SampleBatch.OBS` and `STATE_IN` in the inputs, and
        `STATE_OUT` and `ENCODER_OUT` and outputs to establish an agreement
        between the encoder and RLModules. For stateless encoders, states can be None.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        raise NotImplementedError


class ActorCriticEncoder(Encoder):
    """An encoder that potentially holds two encoders.

    This is a special case of encoder that can either enclose a single,
    shared encoder or two separate encoders: One for the actor and one for the
    critic. The two encoders are of the same type and we can therefore make the
    assumption that they have the same input and output specs.
    """

    framework = None

    def __init__(self, config: ModelConfig) -> None:
        if config.shared:
            self.encoder = config.base_encoder_config.build(framework=self.framework)
        else:
            self.actor_encoder = config.base_encoder_config.build(
                framework=self.framework
            )
            self.critic_encoder = config.base_encoder_config.build(
                framework=self.framework
            )

        # We need to call Encoder.__init__() after initializing the encoder(s) in
        # order to build on their specs.
        super().__init__(config)

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        if self.config.shared:
            state_in_spec = self.encoder.input_specs[STATE_IN]
        else:
            state_in_spec = {
                ACTOR: self.actor_encoder.input_specs[STATE_IN],
                CRITIC: self.critic_encoder.input_specs[STATE_IN],
            }

        return SpecDict(
            {
                SampleBatch.OBS: None,
                STATE_IN: state_in_spec,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        if self.config.shared:
            state_out_spec = self.encoder.output_specs[STATE_OUT]
        else:
            state_out_spec = {
                ACTOR: self.actor_encoder.output_specs[STATE_OUT],
                CRITIC: self.critic_encoder.output_specs[STATE_OUT],
            }

        return SpecDict(
            {
                ENCODER_OUT: {
                    ACTOR: None,
                    CRITIC: None,
                },
                STATE_OUT: state_out_spec,
            }
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
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        if self.config.shared:
            outs = self.encoder(inputs, **kwargs)
            return NestedDict(
                {
                    ENCODER_OUT: {ACTOR: outs[ENCODER_OUT], CRITIC: outs[ENCODER_OUT]},
                    STATE_OUT: outs[STATE_OUT],
                }
            )
        else:
            actor_inputs = NestedDict({**inputs, **{STATE_IN: inputs[STATE_IN][ACTOR]}})
            critic_inputs = NestedDict(
                {**inputs, **{STATE_IN: inputs[STATE_IN][CRITIC]}}
            )
            actor_out = self.actor_encoder(actor_inputs, **kwargs)
            critic_out = self.critic_encoder(critic_inputs, **kwargs)
            return NestedDict(
                {
                    ENCODER_OUT: {
                        ACTOR: actor_out[ENCODER_OUT],
                        CRITIC: critic_out[ENCODER_OUT],
                    },
                    STATE_OUT: {
                        ACTOR: actor_out[STATE_OUT],
                        CRITIC: critic_out[STATE_OUT],
                    },
                }
            )
