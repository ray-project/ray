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
    qf = QFunction()

    # during training
        q_batch = qf({'obs', 'action'}).reduce('min') # min(Q_1(s, a), Q_2(s, a), ... , Q_k(s, a))
        action = pi({'next_obs'}).target_sample # a ~ pi(s')
        next_q_batch = qf({'next_obs', 'action'}).reduce('min') # min(Q_1(s', a'), Q_2(s', a'), ... , Q_k(s', a'))
        q_batch_list = qf({'obs', 'action'}).values # torch.Tensor([Q_1(s, a), ..., Q_k(s, a)])
        
        # in discrete case
        q_logits = qf({'obs'}).q_logits # [[Q_1(s, a_1), ..., Q_1(s, a_m)], ..., [Q_k(s, a_1), ..., Q_k(s, a_m)]]
    
    # during inference
        # As a policy over discrete actions
        action_batch = qf({'obs': s}).q_logits.argmin(0).argmax(-1)
        
        # As a q_network
        q_ = qf({'obs': obs_tens[None], 'action': act_tens[None]}).reduce('min')
        actual_rtg = compute_rtg(sampleBatch)
        # compare q_.mean() and actual_rtg.mean() as a metric for how accurate q_est is 
        during evaluation? 
"""


@dataclass
class QFunctionOutput(NNOutput):
    value: Optional[Sequence[TensorType]] = None
    q_logit: Optional[Sequence[TensorType]] = None


@dataclass
class EnsembleQFunctionOutput(NNOutput):
    q_outputs: Optional[Sequence[QFunctionOutput]] = None

    @property
    def values(self):
        return [q.value for q in self.q_outputs]

    @property
    def q_logits(self):
        return [q.q_logit for q in self.q_outputs]

    def reduce(self, mode: str = "min", dim=0):
        if mode == "min":
            return self.values.min(dim)
        raise NotImplementedError


class QFunction(WithEncoderMixin):
    """
    Design requirements:
    * Support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
    * Support both Continuous and Discrete actions
    * Support distributional Q learning?
    * Support multiple ensembles and flexible reduction strategies across ensembles
        * Should be able to get the pessimistic estimate as well as the individual
        estimates q_max = max(q_list) and also q_list?
    * Support arbitrary target value estimations --- SEE BELOW about bootstrapping
    * Should be able to save/load very easily for serving (if needed)
    * Should be able to create copies efficiently and perform arbitrary parameter
    updates in target_updates
    * Bootstrapping utilities like TD-gamma target estimate should live outside of this
    module as they need to have access to pi, discount, reward, .etc
    *

    """

    def __init__(self, encoder: Optional[Encoder] = None) -> None:
        super().__init__()
        self.encoder = encoder

    def forward(self, batch: SampleBatch, **kwargs) -> QFunctionOutput:
        """Runs Q(S,A), Q({'obs': s, 'action': a}) -> Q(s, a)"""
        pass

    def copy(self) -> "QFunction":
        # TODO: during deep copying we should figure out what we should do with
        #  the encoder
        return QFunction(self.encoder)

    def update_polyak(self, other: "QFunction", polyak_coef: float, **kwargs):
        other_params = other.named_parameters()
        for name, param in self.named_parameters():
            if name not in other_params:
                raise ValueError("Cannot copy because of parameter name mis-match")
            other_param = other_params[name]
            param.data = polyak_coef * param.data + (1 - polyak_coef) * other_param.data


"""
Some examples of pre-defined RLlib standard Qfunctions
"""


#######################################################
########### Continuous action Q-network
#######################################################


class ContinuousQFunction(QFunction):
    def __init__(self, encoder: Optional[Encoder] = None) -> None:
        super().__init__(encoder)
        self.net = nn.Linear(self.encoder.output_dim, 1)

    def forward(self, batch: SampleBatch, **kwargs) -> QFunctionOutput:
        state = self.encoder(batch).state
        q_values = self.net(state)
        return QFunctionOutput(values=[q_values])


#######################################################
########### Discrete action Q-network (DQN on atari)
#######################################################


class DiscreteQFunction(QFunction):
    def __init__(self, encoder: Optional[Encoder] = None) -> None:
        super().__init__(encoder)
        self.net = nn.Linear(self.encoder.output_dim, action_dim)

    def forward(self, batch: SampleBatch, **kwargs) -> QFunctionOutput:
        state = self.encoder(batch).state
        q_logits = self.net(state)
        actions = batch["action"]
        q_values = q_logits[torch.arange(len(actions)), actions]
        return QFunctionOutput(values=[q_values], q_logits=[q_logits])


################################################################
########### Ensemble of Q function networks (e.g. used in TD3)
################################################################


class EnsembleQFunction(QFunction):
    def __init__(
        self, encoder: Optional[Encoder] = None, q_list: List[QFunction] = ()
    ) -> None:
        super().__init__(encoder)
        self.qs = nn.ModuleList(q_list)

    def forward(self, batch: SampleBatch, **kwargs) -> QFunctionOutput:
        state = self.encoder(batch)
        q_values, q_logits = [], []
        for q in self.qs:
            q_out = q(state)
            # check if each q_out is a single q
            q_values.append(q_out.values[0])
            q_logits.append(q_out.q_logits[0])

        return QFunctionOutput(values=q_values, q_logits=q_logits)
