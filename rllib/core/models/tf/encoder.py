from typing import Union

from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
)
from ray.rllib.core.models.base import ModelConfig, Model
from ray.rllib.core.models.tf.primitives import TfMLP
from ray.rllib.core.models.tf.primitives import TfModel
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class TfMLPEncoder(Encoder, TfModel):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        self.net = TfMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dims[0],
            hidden_layer_activation=config.hidden_layer_activation,
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return SpecDict(
            {
                SampleBatch.OBS: TFTensorSpecs("b, h", h=self.config.input_dims[0]),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
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


class TfActorCriticEncoder(TfModel, ActorCriticEncoder):
    """An encoder that can hold two encoders."""

    framework = "tf2"

    def __init__(self, config: ModelConfig) -> None:
        # We have to call TfModel.__init__ first, because it calls the constructor of
        # tf.keras.Model, which is required to be called before models are created.
        TfModel.__init__(self, config)
        ActorCriticEncoder.__init__(self, config)
