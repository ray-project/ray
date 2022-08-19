import torch.nn as nn

from ..configs import ModelConfig

class Encoder(nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.config = config
    
    @abc.abstractmethod
    @property
    def out_dim(self) -> int:
        raise NotImplementedError

    def forward(self, input_dict: TensorDict) -> TensorDict:
        pass



class MLPEncoder(Encoder):
    pass


class VisionEncoder(Encoder):
    pass



class NestedEncoder(Encoder):
    pass



class RNNEncoder(Encoder):
    pass


class TransformerEncoder(Encoder):
    pass