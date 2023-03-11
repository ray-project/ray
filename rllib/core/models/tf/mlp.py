from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.tf.primitives import TfMLP, TfModel
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict

tf1, tf, tfv = try_import_tf()


class TfMLPHead(TfModel):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)

        self.net = TfMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dims[0],
            hidden_layer_activation=config.hidden_layer_activation,
            output_activation=config.output_activation,
        )

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return self.net(inputs)
