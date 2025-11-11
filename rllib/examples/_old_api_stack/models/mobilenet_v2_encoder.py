# @OldAPIStack
"""
This file implements a MobileNet v2 Encoder.
It uses MobileNet v2 to encode images into a latent space of 1000 dimensions.

Depending on the experiment, the MobileNet v2 encoder layers can be frozen or
unfrozen. This is controlled by the `freeze` parameter in the config.

This is an example of how a pre-trained neural network can be used as an encoder
in RLlib. You can modify this example to accommodate your own encoder network or
other pre-trained networks.
"""

from ray.rllib.core.models.base import ENCODER_OUT, Encoder
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

MOBILENET_INPUT_SHAPE = (3, 224, 224)


class MobileNetV2EncoderConfig(ModelConfig):
    # MobileNet v2 has a flat output with a length of 1000.
    output_dims = (1000,)
    freeze = True

    def build(self, framework):
        assert framework == "torch", "Unsupported framework `{}`!".format(framework)
        return MobileNetV2Encoder(self)


class MobileNetV2Encoder(TorchModel, Encoder):
    """A MobileNet v2 encoder for RLlib."""

    def __init__(self, config):
        super().__init__(config)
        self.net = torch.hub.load(
            "pytorch/vision:v0.6.0", "mobilenet_v2", pretrained=True
        )
        if config.freeze:
            # We don't want to train this encoder, so freeze its parameters!
            for p in self.net.parameters():
                p.requires_grad = False

    def _forward(self, input_dict, **kwargs):
        return {ENCODER_OUT: (self.net(input_dict["obs"]))}
