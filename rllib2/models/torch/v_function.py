import copy
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType

from rllib2.utils import NNOutput


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

class VFunction(
    TorchRecurrentModel,
    ModelWithEncoder
):
    """
    Design requirements:
    * Support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
    * Support multiple ensembles and flexible reduction strategies across ensembles
    * Should be able to save/load very easily for serving (if needed)
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        self.vnet = MLP(..., 1)


    def input_spec(self) -> types.SpecDict:
        return self.encoder.input_spec()
    
    def output_spec(self) -> types.SpecDict:
        return types.SpecDict({
            "value": types.Spec(shape='b'),
        })

    def prev_state_spec(self) -> types.SpecDict:
        return self.encoder.prev_state_spec()

    def next_state_spec(self) -> types.SpecDict:
        return self.encoder.next_state_spec()


    def _unroll(self, 
        inputs: types.TensorDict, 
        prev_state: types.TensorDict,
        return_encoded_output: bool = False) -> UnrollOutputType:

        encoder_out, next_state = self.encoder(inputs, prev_state)
        v_value = self.vnet(encoder_out["encoder_out"])
        output = NestedDict({
            "value": v_value,
            "encoder_output": encoder_out["encoder_out"],
        })

        return output, next_state
