import abc
from dataclasses import dataclass
from typing import List, Union

from ray.rllib import SampleBatch
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.nested_dict import NestedDict
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
    not tied to usage with Catalog or RLModules.
    A usage Example together with a Model can be found in the Model.

    Attributes:
        output_dim: The output dimension of the network.
    """

    output_dim: int = None

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

        # These should not be overridden by subclasses. Instead, get_input_spec and
        # get_output_spec should be overridden.
        self._input_spec = self.get_input_spec()
        self._output_spec = self.get_output_spec()

    def get_input_spec(self) -> Union[Spec, None]:
        """Returns the input spec of this model.

        Override `get_input_spec` to define your own input spec.
        This method should not be called often, e.g. every forward pass.
        Instead, it should be called once at instantiation to define Model.input_spec.

        Returns:
            Spec: The input spec.
        """
        return None

    def get_output_spec(self) -> Union[Spec, None]:
        """Returns the output spec of this model.

        Override `get_output_spec` to define your own output spec.
        This method should not be called often, e.g. every forward pass.
        Instead, it should be called once at instantiation to define Model.output_spec.

        Returns:
            Spec: The output spec.
        """
        return None

    @property
    def input_spec(self) -> Spec:
        """Returns the input spec of this model."""
        return self._input_spec

    @input_spec.setter
    def input_spec(self, spec: Spec) -> None:
        raise ValueError(
            "Input spec cannot be set directly. Override "
            "Model.get_input_spec() instead. Set Model._input_spec if "
            "you want to override this behavior."
        )

    @property
    def output_spec(self) -> Spec:
        """Returns the output spec of this model."""
        return self._output_spec

    @output_spec.setter
    def output_spec(self, spec: Spec) -> None:
        raise ValueError(
            "Output spec cannot be set directly. Override "
            "Model.get_input_spec() instead. Set Model._input_spec if "
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
    Therefore, their input_spec contains the observation space dimensions.
    Similarly, their output_spec contains the latent space dimensions.
    Encoders can be recurrent, in which case the state should be part of input- and
    output_specs. The latents that are produced by an encoder are fed into subsequent
    heads.

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
    This should be reflected in the output_spec.

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

            @check_input_specs("input_spec")
            @check_output_specs("output_spec")
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

    def __init__(self, config: ModelConfig):
        super().__init__(config)
        self.input_spec = [SampleBatch.OBS, STATE_IN]
        self.output_spec = [ENCODER_OUT, STATE_OUT]

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
