from ray.rllib.models.tf.fcnet import FullyConnectedNetwork as TFFCNet
from ray.rllib.utils.deprecation import renamed_class

FullyConnectedNetwork = renamed_class(
    cls=TFFCNet,
    old_name="ray.rllib.models.tf.fcnet_v2.FullyConnectedNetwork",
)
