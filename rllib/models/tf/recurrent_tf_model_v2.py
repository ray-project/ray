from ray.rllib.models.tf.recurrent_net import RecurrentNetwork
from ray.rllib.utils.deprecation import renamed_class

RecurrentTFModelV2 = renamed_class(
    cls=RecurrentNetwork,
    old_name="ray.rllib.models.tf.recurrent_tf_model_v2.RecurrentTFModelV2",
)
