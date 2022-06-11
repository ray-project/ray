from typing import Optional

import torch.nn as nn
from torch.distributions import Distribution
from torch import TensorType
from dataclasses import dataclass

from rllib2.utils import NNOutput

@dataclass
class QFunctionOutput(NNOutput):
    pass



class QFunction(nn.Module):
    """
    Design requirements:
    * Support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
    * Support both Continuous and Discrete actions
    * Support distributional Q learning?
    * Support multiple ensembles and flexible reduction strategies across ensembles
        * Should be able to get the pessimistic estimate as well as the individual estimates q_max = max(q_list) and also q_list?
    * Support arbitrary target value estimations
    * Should be able to save/load very easily for serving (if needed)
    * Should be able to create copies efficiently and perform arbitrary parameter updates in target_updates
    """


    def forward(self, batch: SampleBatch) -> QFunctionOutput:
        pass