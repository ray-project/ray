from ray.rllib.models.experimental.base import Model
from ray.rllib.models.experimental.base import ModelConfig
from ray.rllib.models.experimental.tf.primitives import TfMLP, TfModel
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict

tf1, tf, tfv = try_import_tf()


class TfMLPModel(TfModel):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)

        self.net = TfMLP(
            input_dim=config.input_dim,
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dim,
            hidden_layer_activation=config.hidden_layer_activation,
            output_activation=config.output_activation,
        )

        self.input_spec = TFTensorSpecs("b, h", h=self.config.input_dim)
        self.output_spec = TFTensorSpecs("b, h", h=self.config.output_dim)

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return self.net(inputs)
