import copy
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType

from ..types import NestedDict

from ..configs import ModelConfig
from .model_base import ModelWithEncoder, TorchRecurrentModel

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

class QFunction(
    TorchRecurrentModel,
    ModelWithEncoder
):
    """Design requirements:
        * [x] Support arbitrary encoders (encode observations / history to s_t)
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
        super().__init__(config)
        self.q_logits = MLP(..., self.action_dim)

    @property
    def action_dim(self) -> int:
        if self.config.action_space.is_discrete:
            return self.config.action_space.n
        return self.config.action_space.shape[0]

    def input_spec(self) -> types.SpecDict:
        specs = self.encoder.input_spec()
        specs.update("action": types.Spec(shape='b a', a=self.action_dim))
        return specs
    
    def output_spec(self) -> types.SpecDict:
        spec = types.SpecDict({
                "value": types.Spec(shape='b'),
        })
        if self.config.action_space.is_discrete:
            spec = spec.update(
                q_logits=types.Spec(shape='b a', a=self.action_logit_dim)
            )
        return spec

    def prev_state_spec(self) -> types.SpecDict:
        return self.encoder.prev_state_spec()

    def next_state_spec(self) -> types.SpecDict:
        return super().next_state_spec()

    def update_polyak(self, other: "QFunction", polyak_coef: float, **kwargs):
        # if the encoder is shared the parameters are gonna be the same
        other_params = other.named_parameters()
        for name, param in self.named_parameters():
            if name not in other_params:
                raise ValueError("Cannot copy because of parameter name mis-match")
            other_param = other_params[name]
            param.data = polyak_coef * param.data + (1 - polyak_coef) * other_param.data

    def _unroll(self, inputs: types.TensorDict, prev_state: types.TensorDict, **kwargs) -> UnrollOutputType:

        encoder_out, next_state = self.encoder(inputs, prev_state)
        if self.config.action_space.is_discrete:
            q_logits = self.q_logits(encoder_out["encoder_out"])
            actions = inputs["action"]
            q_values = q_logits[torch.arange(len(actions)), actions]
        else:
            obs_action = torch.cat(
                [encoder_out['encoder_out'], inputs["action"]], dim=-1
            )
            q_logits = q_values = self.q_logits(obs_action)

        output = NestedDict({
            "value": q_values,
            "q_logits": q_logits,
        })

        return output, next_state

################################################################
########### Ensemble of Q function networks (e.g. used in TD3)
################################################################

class EnsembleQFunction(QFunction):
    def __init__(
        self, config: QFConfig,
    ) -> None:
        super().__init__(config)
        self.qs = nn.ModuleList(
            [QFunction(config) for _ in range(self.config.num_ensemble)]
        )

    def input_spec(self) -> types.SpecDict:
        example_spec = next(iter(self.qs)).input_spec()
        return example_spec
    
    def output_spec(self) -> types.SpecDict:
        spec = {f'q_{i}': q.output_spec() for i, q in enumerate(self.qs)}
        return NestedDict(spec)

    def prev_state_spec(self) -> types.SpecDict:
        return NestedDict({f'q_{i}': q.prev_state_spec() for i, q in enumerate(self.qs)})
    
    def next_state_spec(self) -> types.SpecDict:
        return NestedDict({f'q_{i}': q.next_state_spec() for i, q in enumerate(self.qs)})
    
    def _unroll(self, inputs: types.TensorDict, prev_state: types.TensorDict, **kwargs) -> UnrollOutputType:
        outputs_and_states = {f'q_{i}': q(inputs, prev_state) for i, q in enumerate(self.qs)}

        q_outputs = {k: v[0] for k, v in outputs_and_states.items()}
        next_states = {k: v[1] for k, v in outputs_and_states.items()}

        return q_outputs, next_states
