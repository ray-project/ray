import copy
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType

from ..configs import ModelConfig
from .model_base import ModelWithEncoder

from rllib2.utils import NNOutput


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

@dataclass
class QFConfig(ModelConfig):
    num_ensemble: int = 1

class QFunctionBase(nn.Module):
    """Design requirements:
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
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.config = config

    @abc.abstractmethod
    def forward(
        self, 
        input_dict: SampleBatch, 
        return_encoder_output: bool = False,
        **kwargs
    ) -> QFunctionOutput:
        raise NotImplementedError

    def update_polyak(self, other: "QFunctionBase", polyak_coef: float, **kwargs):
        # if the encoder is shared the parameters are gonna be the same
        other_params = other.named_parameters()
        for name, param in self.named_parameters():
            if name not in other_params:
                raise ValueError("Cannot copy because of parameter name mis-match")
            other_param = other_params[name]
            param.data = polyak_coef * param.data + (1 - polyak_coef) * other_param.data

class ObsActionConcatEncoder(Encoder):

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        if self.config.encoder:
            self.obs_encoder = self.config.encoder
        else:
            self.obs_encoder = model_catalog.get_encoder(config)
    
    @property
    def out_dim(self) -> int:
        return self.obs_encoder.out_dim + self.config.action_dim
    
    def forward(self, input_dict: SampleBatch) -> torch.Tensor:
        obs_h = self.obs_encoder(input_dict)
        action_h = input_dict['action']
        return torch.cat([obs_h, action_h], dim=-1)

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


################################################################
########### Ensemble of Q function networks (e.g. used in TD3)
################################################################

class EnsembleQFunction(QFunctionBase, ModelWithEncoder):
    def __init__(
        self, config: QFConfig,
    ) -> None:
        super().__init__(config)
        self.qs = nn.ModuleList([QFunction(config) for _ in range(config.num_ensemble)])

    def forward(self, input_dict: SampleBatch, **kwargs) -> EnsembleQFunctionOutput:
        q_values, q_logits = [], []
        for q in self.qs:
            q_out = q(input_dict)
            # check if each q_out is a single q
            q_values.append(q_out.values[0])
            q_logits.append(q_out.q_logits[0])

        return QFunctionOutput(values=q_values, q_logits=q_logits)
