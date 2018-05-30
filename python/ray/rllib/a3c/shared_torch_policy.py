from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn.functional as F

from ray.rllib.a3c.torchpolicy import TorchPolicy
from ray.rllib.models.pytorch.misc import var_to_np, convert_batch
from ray.rllib.models.catalog import ModelCatalog


class SharedTorchPolicy(TorchPolicy):
    """Assumes nonrecurrent."""

    other_output = ["vf_preds"]
    is_recurrent = False

    def __init__(self, registry, ob_space, ac_space, config, **kwargs):
        super(SharedTorchPolicy, self).__init__(registry, ob_space, ac_space,
                                                config, **kwargs)

    def _setup_graph(self, ob_space, ac_space):
        _, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = ModelCatalog.get_torch_model(
            self.registry, ob_space, self.logit_dim, self.config["model"])
        self.optimizer = torch.optim.Adam(
            self._model.parameters(), lr=self.config["lr"])

    def compute(self, ob, *args):
        """Should take in a SINGLE ob"""
        with self.lock:
            ob = torch.from_numpy(ob).float().unsqueeze(0)
            logits, values = self._model(ob)
            # TODO(alok): Support non-categorical distributions. Multinomial
            # is only for categorical.
            sampled_actions = F.softmax(logits, dim=1).multinomial(1).squeeze()
            values = values.squeeze()
            return var_to_np(sampled_actions), {"vf_preds": var_to_np(values)}

    def compute_logits(self, ob, *args):
        with self.lock:
            ob = torch.from_numpy(ob).float().unsqueeze(0)
            res = self._model.hidden_layers(ob)
            return var_to_np(self._model.logits(res))

    def value(self, ob, *args):
        with self.lock:
            ob = torch.from_numpy(ob).float().unsqueeze(0)
            res = self._model.hidden_layers(ob)
            res = self._model.value_branch(res)
            res = res.squeeze()
            return var_to_np(res)

    def _evaluate(self, obs, actions):
        """Passes in multiple obs."""
        logits, values = self._model(obs)
        log_probs = F.log_softmax(logits, dim=1)
        probs = F.softmax(logits, dim=1)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        # TODO(alok): set distribution based on action space and use its
        # `.entropy()` method to calculate automatically
        entropy = -(log_probs * probs).sum(-1).sum()
        return values, action_log_probs, entropy

    def _backward(self, batch):
        """Loss is encoded in here. Defining a new loss function
        would start by rewriting this function"""

        states, actions, advs, rs, _ = convert_batch(batch)
        values, action_log_probs, entropy = self._evaluate(states, actions)
        pi_err = -advs.dot(action_log_probs.reshape(-1))
        value_err = F.mse_loss(values.reshape(-1), rs)

        self.optimizer.zero_grad()

        overall_err = sum([
            pi_err,
            self.config["vf_loss_coeff"] * value_err,
            self.config["entropy_coeff"] * entropy,
        ])

        overall_err.backward()
        torch.nn.utils.clip_grad_norm_(self._model.parameters(),
                                       self.config["grad_clip"])
