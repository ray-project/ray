from __future__ import absolute_import, division, print_function

from copy import deepcopy

import numpy as np
import ray
import torch
import torch.nn.functional as F
import torchvision.datasets as dset
import torchvision.transforms as transforms
from ray.rllib.a3c.shared_torch_policy import SharedTorchPolicy
from ray.rllib.a3c.torchpolicy import TorchPolicy
from ray.rllib.models.pytorch.misc import convert_batch, var_to_np
from ray.rllib.utils.process_rollout import discount, process_rollout
from torch import Tensor, distributions, nn
from torch.autograd import Variable
from torch.nn import BatchNorm1d, Dropout, Linear, ReLU, Sequential, Softmax
from torch.nn.utils.convert_parameters import (_check_param_device,
                                               parameters_to_vector,
                                               vector_to_parameters,)
from torch.optim import LBFGS, Adam
from torch.utils.data import DataLoader, Dataset, TensorDataset

# TODO rm unused imports

# TODO support tensorflow


def explained_variance_1d(ypred, y):
    """
    Var[ypred - y] / var[y].
    https://www.quora.com/What-is-the-meaning-proportion-of-variance-explained-in-linear-regression
    """
    assert y.ndim == 1 and ypred.ndim == 1
    vary = np.var(y)
    return np.nan if vary == 0 else 1 - np.var(y - ypred) / vary


def vector_to_gradient(v, parameters):
    r"""Convert one vector representing the
    gradient to the .grad of the parameters.

    Arguments:
        v (Tensor): a single vector represents the parameters of a model.
        parameters (Iterable[Tensor]): an iterator of Tensors that are the
            parameters of a model.
    """
    # Ensure v of type Tensor
    if not isinstance(v, torch.Tensor):
        raise TypeError('expected torch.Tensor, but got: {}'.format(
            torch.typename(v)))
    # Flag for the device where the parameter is located
    param_device = None

    # Pointer for slicing the vector for each parameter
    pointer = 0
    for param in parameters:
        # Ensure the parameters are located in the same device
        param_device = _check_param_device(param, param_device)

        # The length of the parameter
        num_param = torch.prod(torch.LongTensor(list(param.grad.size())))
        # Slice the vector, reshape it, and replace the old data of the parameter
        param.grad.data = v[pointer:pointer + num_param].view(
            param.size()).data

        # Increment the pointer
        pointer += num_param


class _ValueFunctionWrapper(nn.Module):
    """Wrapper around any value function model to add fit and predict
    functions."""

    def __init__(self, model, lr):
        super().__init__()
        self.model = model
        self.loss_fn = nn.MSELoss()
        self.lr = lr

    def forward(self, data):
        return self.model.forward(data)

    def fit(self, observations, labels):
        def closure():
            predicted = self.predict(observations)
            loss = self.loss_fn(predicted, labels)
            self.optimizer.zero_grad()
            loss.backward()
            return loss

        old_params = parameters_to_vector(self.model.parameters())
        for lr in self.lr * .5**np.arange(10):
            self.optimizer = LBFGS(self.model.parameters(), lr=lr)
            self.optimizer.step(closure)
            current_params = parameters_to_vector(self.model.parameters())
            if any(np.isnan(current_params.data.cpu().numpy())):
                # LBFGS optimization diverged. Roll back update.
                vector_to_parameters(old_params, self.model.parameters())
        else:
            return

    def predict(self, observations):
        return self.forward(
            torch.cat([
                Variable(Tensor(observation)).unsqueeze(0)
                for observation in observations
            ]))


class TRPOPolicy(SharedTorchPolicy):
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

    def mean_kl(self, states, policy):
        """Returns an estimate of the average KL divergence between a given
        policy and self.policy."""
        new_p = policy(states).detach() + 1e-8
        old_p = self.policy(states)

        return (old_p.dot((old_p / new_p).log())).mean()

    def HVP(self, v):
        """Returns the product of the Hessian of the KL divergence and the
        given vector."""

        self.policy.zero_grad()
        mean_kl = self.mean_kl(self.policy)

        g_kl = torch.autograd.grad(
            mean_kl, self.policy.parameters(), create_graph=True)
        gvp = torch.cat([grad.view(-1) for grad in g_kl]).dot(v)

        H = torch.autograd.grad(gvp, self.policy.parameters())
        fisher_vector_product = torch.cat(
            [grad.contiguous().view(-1) for grad in H]).data


        return fisher_vector_product + (self.config['cg_damping'] * v.data)

    def conjugate_gradient(self, b, cg_iters=10):
        """Returns F^(-1)b where F is the Hessian of the KL divergence."""
        p = b.clone().data
        r = b.clone().data
        x = np.zeros_like(b.data.numpy())

        rdotr = r.double().dot(r.double())

        for _ in range(cg_iters):
            z = self.HVP(Variable(p)).squeeze(0)
            v = rdotr / p.double().dot(z.double())
            x += v * p.numpy()
            r -= v * z
            newrdotr = r.double().dot(r.double())
            mu = newrdotr / rdotr
            p = r + mu * p
            rdotr = newrdotr

            if rdotr < self.config['residual_tol']:
                break
        return x

    def surrogate_loss(self, params, states, actions, adv):
        """Returns the surrogate loss wrt the given parameter vector params."""

        new_policy = deepcopy(self.policy)
        vector_to_parameters(params, new_policy.parameters())

        EPSILON = 1e-8

        prob_new = new_policy(states).gather(1, torch.cat(actions)).data
        prob_old = self.policy(states).gather(
            1, torch.cat(actions)).data + EPSILON

        return -torch.mean((prob_new / prob_old) * adv)

    def linesearch(self, x, fullstep, expected_improve_rate):
        """Returns the parameter vector given by a linesearch."""

        accept_ratio = 0.1
        max_backtracks = 10

        fval = self.surrogate_loss(x)

        for stepfrac in .5**np.arange(max_backtracks):

            g = stepfrac * fullstep

            xnew = x.data.numpy() + g
            newfval = self.surrogate_loss(Variable(torch.from_numpy(xnew)))
            actual_improve = fval - newfval
            expected_improve = expected_improve_rate * stepfrac

            if actual_improve / expected_improve > accept_ratio and actual_improve > 0:
                # return Variable(torch.from_numpy(xnew))
                return g
        else:
            return 0

    def _backward(self, batch):
        """Fills gradient buffers up."""

        # TODO what's in a batch by default? calculate batches
        #
        # TODO does the policy used return logits as well as an action or does
        # it need to be modified to do so?
        #
        # TODO how to just get grad rather than setting weights directly?
        # Maybe just this the `set_weights` or `model_update` method?

        # TODO update critic

        states, actions, advs, rewards, _ = convert_batch(batch)
        discounted_rewards = discount(rewards, self.config['discount_rate'])
        values, log_probs, entropy = self._evaluate(states, actions)
        policy_error = -torch.dot(advs, log_probs)

        value_err = F.mse(values, rewards)

        self.env = env
        self.policy = policy
        # TODO rm wrapper?
        self.vf = _ValueFunctionWrapper(
            self.vf,
            self.config['vf_lr'],
        )

        bs = self.config['batch_size']

        # TODO add cuda

        # XXX compute_action: S -> a, prob dist

        # TODO implement in separate file or use existing implementation.
        # TODO accumulate until data is one batch big
        num_batches = len(actions) // bs if len(
            actions) % bs == 0 else (len(actions) // bs) + 1

        for batch_n in range(num_batches):
            _states = states[batch_n * bs:(batch_n + 1) * bs]

            _disc_rews = discounted_rewards[batch_n * bs:bs * (batch_n + 1)]

            _actions = actions[batch_n * bs:(batch_n + 1) * bs]

            _action_dists = action_dists[batch_n * bs:(batch_n + 1) * bs]

            baseline = self.vf.predict(_states).data
            advantage = Tensor(_disc_rews).unsqueeze(1) - baseline

            # Normalize the advantage
            self.advantage = (advantage - advantage.mean()) / (
                advantage.std() + 1e-8)

            # Calculate the surrogate loss as the element-wise product of the
            # advantage and the probability ratio of actions taken.
            new_p = torch.cat(_action_dists).gather(1, torch.cat(_actions))
            old_p = new_p.detach() + 1e-8

            prob_ratio = new_p / old_p
            surrogate_loss = -torch.mean(prob_ratio * Variable(advantage)) - (
                self.config['ent_coeff'] * entropy)

            # Calculate the gradient of the surrogate loss
            self.policy.zero_grad()

            surrogate_loss.backward(retain_graph=True)

            g = parameters_to_vector(
                [v.grad for v in self.policy.parameters()]).squeeze(0)

            if g.nonzero().size()[0]:
                # Use conjugate gradient algorithm to determine the step direction in params space
                step_direction = self.conjugate_gradient(-g)
                step_direction_variable = Variable(
                    torch.from_numpy(step_direction))

                # Do line search to determine the stepsize of params in the direction of step_direction
                shs = step_direction.dot(
                    self.HVP(step_direction_variable).numpy().T) / 2
                lm = np.sqrt(shs / self.max_kl)
                fullstep = step_direction / lm
                gdotstepdir = -g.dot(step_direction_variable).data[0]

                params = self.linesearch(
                    x=parameters_to_vector(self.policy.parameters()),
                    fullstep=fullstep,
                    expected_improve_rate=gdotstepdir / lm,
                )

                # TODO use explained variance to fit vf?
                # Fit the estimated value function to the actual observed discounted rewards
                ev_before = explained_variance_1d(
                    baseline.squeeze(1).numpy(),
                    _disc_rews,
                )

                self.vf.zero_grad()
                vf_params = parameters_to_vector(self.vf.parameters())

                self.vf.fit(
                    _states,
                    Variable(Tensor(_disc_rews)),
                )

                ev_after = explained_variance_1d(
                    self.vf.predict(_states).data.squeeze(1).numpy(),
                    _disc_rews,
                )
                if ev_after < ev_before or np.abs(ev_after) < 1e-4:
                    vector_to_parameters(
                        vf_params,
                        self.vf.parameters(),
                    )

                # Update parameters of policy model
                old_model = deepcopy(self.policy)
                old_model.load_state_dict(self.policy.state_dict())

                # XXX fills gradient buffers here
                # TODO fill gradient buffers rather than changing the parameters
                # TODO set grad buffers to params

                # if not any(np.isnan(params.data.numpy())):
                # vector_to_parameters(params, self.policy.parameters())
