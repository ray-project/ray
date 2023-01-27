from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.utils import try_import_tf
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.experimental.tf.primitives import TfMLP, TfModel
from ray.rllib.models.experimental.base import ModelConfig, ForwardOutputType

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

    @property
    def input_spec(self):
        return TFTensorSpecs("b, h", h=self.config.input_dim)

    @property
    def output_spec(self):
        return TFTensorSpecs("b, h", h=self.config.output_dim)

    @check_input_specs("input_spec", cache=False)
    @check_output_specs("output_spec", cache=False)
    def __call__(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return self.net(inputs)
