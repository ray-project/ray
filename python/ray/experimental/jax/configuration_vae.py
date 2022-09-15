from transformers import PretrainedConfig


class VAEConfig(PretrainedConfig):
    def __init__(
        self,
        in_channels=3,
        out_channels=3,
        down_block_types=("DownEncoderBlock2D", "DownEncoderBlock2D", "DownEncoderBlock2D", "DownEncoderBlock2D"),
        up_block_types=("UpDecoderBlock2D", "UpDecoderBlock2D", "UpDecoderBlock2D", "UpDecoderBlock2D"),
        block_out_channels=(128, 256, 512, 512),
        layers_per_block=2,
        act_fn="silu",
        latent_channels=4,
        sample_size=512,
        double_z=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.down_block_types = down_block_types
        self.up_block_types = up_block_types
        self.block_out_channels = block_out_channels
        self.layers_per_block = layers_per_block
        self.act_fn = act_fn
        self.latent_channels = latent_channels
        self.sample_size = sample_size
        self.double_z = double_z
