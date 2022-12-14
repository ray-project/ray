def get_fc_net_class(
        framework,
        # input_spec,
        # hiddens,
        # activations,
    ):
    if framework in ["tf2", "tf"]:
        from ray.rllib.models.tf.fcnet import FullyConnectedNetwork as FCNet
    elif framework == "torch":
        from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as FCNet
    elif framework == "jax":
        from ray.rllib.models.jax.fcnet import FullyConnectedNetwork as FCNet
    else:
        raise ValueError(
            "framework={} not supported in `ModelCatalog._get_v2_model_"
            "class`!".format(framework)
        )
    return FCNet


def get_vision_net_class(
        framework,
        # input_spec,
        # conv_filters,
        # conv_activations,
        # post_fcnet_hiddens,
        # post_fcnet_activations,
        # no_final_linear,
        # vf_share_layers
    ):
    # Get appropriate model class
    if framework in ["tf2", "tf"]:
        from ray.rllib.models.tf.visionnet import VisionNetwork as VisionNet
    elif framework == "torch":
        from ray.rllib.models.torch.visionnet import VisionNetwork as VisionNet
    else:
        raise ValueError(
            "framework={} not supported in `ModelCatalog._get_v2_model_"
            "class`!".format(framework)
        )
    return VisionNet


def get_encoder_class(
        framework,
        # input_spec,
        # conv_filters,
        # conv_activations,
        # fcnet_hiddens,
        # fcnet_activations,
        # post_fcnet_hiddens,
        # post_fcnet_activations,
        #no_final_linear,
        # vf_share_layers
    ):
    # Uses get_vision_net and get_fc_net under the hood
    if framework in ["tf2", "tf"]:
        from ray.rllib.models.tf.complex_input_net import (
            ComplexInputNetwork as ComplexNet
        )
    elif framework == "torch":
        from ray.rllib.models.torch.complex_input_net import (
            ComplexInputNetwork as ComplexNet
        )
    else:
        raise ValueError(
            "framework={} not supported in `ModelCatalog._get_v2_model_"
            "class`!".format(framework)
        )
    return ComplexNet