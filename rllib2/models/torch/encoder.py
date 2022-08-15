from typing import Optional

import torch.nn as nn


@dataclass
class EncoderOutput(NNOutput):
    state: Optional[TensorType] = None


class Encoder(nn.Module):
    def __init__(self, ecoder_config):
        super(Encoder, self).__init__()

    def forward(self, batch: SampleBatch) -> EncoderOutput:
        raise NotImplementedError

    def freeze(self):
        pass
