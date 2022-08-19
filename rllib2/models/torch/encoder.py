import torch.nn as nn

class Encoder(nn.Module):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__()
        self.config = config

    def forward(self, input_dict: TensorDict) -> TensorDict:
        pass



class MLP(nn.Module):
    pass


class CNN(nn.Module):
    pass



class NestedEncoder(nn.Module):
    pass



class RNNEncoder(nn.Module):
    pass


class TransformerEncoder(nn.Module):
    pass