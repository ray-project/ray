from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from threading import Lock

import torch
import torch.nn.functional as F

import ray
from ray.rllib.models.pytorch.misc import var_to_np, convert_batch


class TorchPolicyGraph(PolicyGraph):
    def __init__(
            self, observation_space, action_space, model, loss,
            model_inputs, model_outputs, loss_inputs):
        self.observation_space = observation_space
        self.action_space = action_space
        self.model = model
        self.loss = loss
        self.lock = Lock()
        self._model_inputs = model_inputs
        self._model_outputs = model_outputs
        self._loss_inputs = loss_inputs
        self._optimizer = self.optimizer()
        # TODO(ekl) handle action dists generically in pytorch
        assert "logits" in model_outputs, "Model must output logits"

    def compute_actions(
            self, obs_batch, state_batches=None, is_training=False):
        if state_batches:
            raise NotImplementedError("Torch RNN support")
        with self.torch_lock:
            ob = torch.from_numpy(np.array(obs_batch)).float()
            model_out = self.torch_model(ob)
            assert len(model_out) == len(self._model_outputs), \
                "Model returned more outputs than expected"
            logits = model_out[self._model_outputs.index("logits")]
            actions = F.softmax(logits, dim=1).multinomial(1).squeeze(0)
            return var_to_np(actions), [], self.extra_action_out(model_out)

    def compute_gradients(self, samples):
        with self._lock:
            self._backward(samples)
            # Note that return values are just references;
            # calling zero_grad will modify the values
            return [p.grad.data.numpy() for p in self._model.parameters()], {}

    def apply_gradients(self, grads):
        self._optimizer.zero_grad()
        for g, p in zip(grads, self._model.parameters()):
            p.grad = torch.from_numpy(g)
        self._optimizer.step()

        return {}

    def get_weights(self):
        return self._model.state_dict()

    def set_weights(self, weights):
        with self.lock:
            self._model.load_state_dict(weights)

    def extra_action_out(self, model_out):
        return {}

    def optimizer(self):
        return torch.optim.Adam(self.model.parameters())

    def _backward(self, samples):
        loss_t = self._forward(samples)
        self._optimizer.zero_grad()
        loss_t.backward()

    def _forward(self, samples):
        model_in = []
        for key in self._model_inputs:
            model_in.append(torch.from_numpy(samples[key]))
        model_out = self._model(*model_in)
        loss_in = []
        for key in self._loss_inputs:
            if key in samples:
                loss_in.append(torch.from_numpy(samples[key]))
            else:
                loss_in.append(model_out[self._model_out.index(key)])
        return self._loss(*loss_in)
