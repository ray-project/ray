from transformers import PretrainedConfig


class UNet2DConfig(PretrainedConfig):
    def __init__(
        self,
        sample_size=32,
        in_channels=4,
        out_channels=4,
        down_block_types=("CrossAttnDownBlock2D", "CrossAttnDownBlock2D", "CrossAttnDownBlock2D", "DownBlock2D"),
        up_block_types=("UpBlock2D", "CrossAttnUpBlock2D", "CrossAttnUpBlock2D", "CrossAttnUpBlock2D"),
        block_out_channels=(224, 448, 672, 896),
        layers_per_block=2,
        attention_head_dim=8,
        cross_attention_dim=768,
        dropout=0.1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sample_size = sample_size
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.down_block_types = down_block_types
        self.up_block_types = up_block_types
        self.block_out_channels = block_out_channels
        self.layers_per_block = layers_per_block
        self.attention_head_dim = attention_head_dim
        self.cross_attention_dim = cross_attention_dim
        self.dropout = dropout
