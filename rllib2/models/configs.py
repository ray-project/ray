from typing import Union, Optional, Dict, Any, List, Tuple, Callable, cast

import enum
import torch.nn as nn
from dataclasses import dataclass

ModuleType = Union[nn.Module, "tf.Module"]
ActivationType = str

class RNNCellType(enum.Enum):
    LSTM = "lstm"
    GRU = "gru"

class ActivationType(enum.Enum):
    TANH = "tanh"
    RELU = "relu"
    SIGMOID = "sigmoid"
    GELU = "gelu"
    ELU = "elu"

@dataclass
class ConvLayerConfig:
    out_channels: int
    kernel_size: Union[int, Tuple[int, int]]
    stride: Union[int, Tuple[int, int]]


@dataclass
class ModelConfig:
    observation_space: Optional[Space] = None
    action_space: Optional[Space] = None
    input_encoder: Optional[ModuleType] = None
    disable_preprocessor_api: bool = False
    # fcnet
    fcnet_hiddens: List[int] = [256, 256]
    activation: ActivationType = ActivationType.TANH
	# image
    conv_filters: Optional[List[ConvLayerConfig]] = None 
    # rnn
    is_rnn: bool = False
    rnn_cell: RNNCellType = RNNCellType.LSTM
    rnn_cell_size: int = 256
    # transformer (GTrXL) → we will change this to be more general transformer architecture and have GTrXL to be an example
    is_attention: bool = False
    attention_num_transformer_units: int = 1
    attention_dim: int = 64
    attention_num_heads: int = 1
    attention_head_dim: int = 64
    attention_memory_inference: int = 50
    attention_memory_training: int = 50
    attention_position_wise_mlp_dim: int = 64
    attention_init_gru_gate_bias: float = 2.0
    attention_use_n_prev_actions: bool = False
    attention_use_n_prev_rewards: bool = False

    def from_dict(config: Dict[str, Any]) -> "ModelConfig":
        pass

    def to_dict(self) -> Dict[str, Any]:
        pass