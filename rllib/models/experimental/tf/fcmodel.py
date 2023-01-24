from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.utils import try_import_tf
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.tf.primitives import FCNet, TFModel
from ray.rllib.models.experimental.base import ModelConfig, ForwardOutputType

tf1, tf, tfv = try_import_tf()


class FCModel(tf.Module, TFModel):
    def __init__(self, config: ModelConfig) -> None:
        tf.Module.__init__(self)
        TFModel.__init__(self, config)

        self.net = FCNet(
            input_dim=config.input_dim,
            hidden_layers=config.hidden_layers,
            output_dim=config.output_dim,
            activation=config.activation,
        )

    @property
    def input_spec(self):
        return TFTensorSpecs("b, h", h=self.config.input_dim)

    @property
    def output_spec(self):
        return TFTensorSpecs("b, h", h=self.config.output_dim)

    @check_input_specs("input_spec", filter=True, cache=False)
    @check_output_specs("output_spec", cache=False)
    def __call__(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return self.net(inputs)
