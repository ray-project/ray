import copy
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType

from rllib2.utils import NNOutput

from .encoder import Encoder, WithEncoderMixin

"""
Example:
    pi = Pi()
    vf = VFunction()

    # during training
        v_batch = vf({'obs'}).reduce('min') # min(V_1(s), V_2(s), ... , V_k(s))
        next_v_batch = vf({'next_obs'}).reduce('min') # min(V_1(s'), V_2(s'), ... , V_k(s'))
    
    # during inference
        I can't think of any case that vf would be used during inference. 
"""


@dataclass
class VFunctionOutput(NNOutput):
    values: Optional[Sequence[TensorType]] = None

    def reduce(self, mode: str = "min", dim=0):
        if mode == "min":
            return self.values.min(dim)
        raise NotImplementedError


class VFunctionBase(nn.Module):

    def __init__(self) -> None:
        super().__init__()
class QFunction(QFunctionBase, ModelWithEncoder):

    def __init__(self, config: QFConfig) -> None:
        # encode obs and append it to the input_action
        super().__init__(config)

        # no deep copy here
        encoder_config = copy.copy(config)
        self.encoder = ObsActionConcatEncoder(encoder_config)

        if config.action_space.is_discrete:
            self._out_layer = nn.Linear(self.encoder.output_size, config.action_space.n)
        else:
            self._out_layer = nn.Linear(self.encoder.output_size, 1)

    def forward(self, input_dict: SampleBatch, **kwargs) -> QFunctionOutput:
        encoder_output = self.encoder(input_dict)
        q_logits = self._out_layer(encoder_output)
        actions = input_dict["action"]
        q_values = q_logits[torch.arange(len(actions)), actions]
        return QFunctionOutput(value=[q_values], q_logit=q_logits)

class VFunction(WithEncoderMixin, ModelIO):
    """
    Design requirements:
    * Support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
    * Support distributional Q learning?
    * Support multiple ensembles and flexible reduction strategies across ensembles
    * Should be able to save/load very easily for serving (if needed)
    """

    def __init__(self, encoder: Optional[Encoder] = None) -> None:
        super().__init__()
        self.encoder = encoder

    def forward(self, batch: SampleBatch, **kwargs) -> VFunctionOutput:
        """Runs V(S), V({'obs': s}) -> V(s)"""
        pass

    def copy(self) -> "VFunction":
        return VFunction(self.encoder)


"""
Some examples of pre-defined RLlib standard Vfunctions
"""


#######################################################
########### Continuous action Q-network
#######################################################


class VNet(VFunction):
    def __init__(self, encoder: Optional[Encoder] = None) -> None:
        super().__init__(encoder)
        self.net = nn.Linear(self.encoder.output_dim, 1)

    def forward(self, batch: SampleBatch, **kwargs) -> VFunctionOutput:
        state = self.encoder(batch).state
        q_values = self.net(state)
        return VFunctionOutput(values=[q_values])
