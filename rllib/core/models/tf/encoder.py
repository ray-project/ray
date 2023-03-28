from typing import Union

from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
)
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    MLPEncoderConfig,
)
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.tf.primitives import TfMLP, TfCNN
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class TfMLPEncoder(Encoder, TfModel):
    def __init__(self, config: MLPEncoderConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        self.net = TfMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            output_dim=config.output_dims[0],
        )

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                SampleBatch.OBS: TFTensorSpecs("b, h", h=self.config.input_dims[0]),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                ENCODER_OUT: TFTensorSpecs("b, h", h=self.config.output_dims[0]),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict) -> NestedDict:
        return NestedDict(
            {
                ENCODER_OUT: self.net(inputs[SampleBatch.OBS]),
                STATE_OUT: inputs[STATE_IN],
            }
        )


class TfCNNEncoder(TfModel, Encoder):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        output_activation = get_activation_fn(
            config.output_activation, framework="tf2"
        )

        layers = []
        cnn = TfCNN(
            input_dims=config.input_dims,
            filter_specifiers=config.filter_specifiers,
            filter_layer_activation=config.filter_layer_activation,
            output_activation=output_activation,
        )
        layers.append(cnn)

        layers.append(nn.Flatten())

        # Add a final linear layer to make sure that the outputs have the correct
        # dimensionality.
        layers.append(
            nn.Linear(
                int(cnn.output_width) * int(cnn.output_height), config.output_dims[0]
            )
        )
        if output_activation is not None:
            layers.append(output_activation())

        self.net = nn.Sequential(*layers)

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                SampleBatch.OBS: TorchTensorSpec(
                    "b, w, h, d",
                    w=self.config.input_dims[0],
                    h=self.config.input_dims[1],
                    d=self.config.input_dims[2],
                ),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                ENCODER_OUT: TorchTensorSpec("b, h", h=self.config.output_dims[0]),
                STATE_OUT: None,
            }
        )

    def _forward(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        return NestedDict(
            {
                ENCODER_OUT: self.net(input_dict[SampleBatch.OBS]),
                STATE_OUT: input_dict[STATE_IN],
            }
        )


class TfActorCriticEncoder(TfModel, ActorCriticEncoder):
    """An encoder that can hold two encoders."""

    framework = "tf2"

    def __init__(self, config: ActorCriticEncoderConfig) -> None:
        # We have to call TfModel.__init__ first, because it calls the constructor of
        # tf.keras.Model, which is required to be called before models are created.
        TfModel.__init__(self, config)
        ActorCriticEncoder.__init__(self, config)
