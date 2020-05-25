from ray.rllib.models.tf.vision_net import VisionNetwork as TFVision
from ray.rllib.utils.deprecation import renamed_class

VisionNetwork = renamed_class(
    cls=TFVision,
    old_name="ray.rllib.models.tf.visionnet_v2.VisionNetwork",
)
