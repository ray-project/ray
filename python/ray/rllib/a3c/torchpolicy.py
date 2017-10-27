import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F
# Code adapted from ELF and https://github.com/ikostrikov/pytorch-a3c


def convert_batch(batch):
    states = Variable(torch.from_numpy(batch.si).float())
    acs = Variable(torch.from_numpy(batch.a))
    advs = Variable(torch.from_numpy(batch.adv.copy()).float())
    advs = advs.view(-1, 1)
    rs = Variable(torch.from_numpy(batch.r.copy()).float())
    rs = rs.view(-1, 1)
    # import ipdb; ipdb.set_trace()
    features = [Variable(torch.from_numpy(f)) for f in batch.features]
    return states, acs, advs, rs, features


class Model(nn.Module):
    def __init__(self, obs_space, ac_space):
        super(Model, self).__init__()
        self.volatile = False
        self.dtype = torch.cuda.FloatTensor if torch.cuda.is_available() else torch.FloatTensor
        self._init(obs_space, ac_space.n, {})

    def set_volatile(self, volatile):
        ''' Set model to ``volatile``.

        Args:
            volatile(bool): indicating that the Variable should be used in inference mode, i.e. don't save the history.'''
        self.volatile = volatile

    def set_gpu(self, id):
        pass

    def var_to_np(self, var):
        # Assumes single input
        return var.data.numpy()[0]


class Policy(Model):

    def _init(self, inputs, num_outputs, options):
        raise NotImplementedError

    def setup_loss(self):
        self.optimizer = torch.optim.SGD(self.parameters(), lr=0.001)

    def _policy_entropy_loss(self, ac_logprobs, advs):
        return -(advs * ac_logprobs).mean()

    def _backward(self, batch):
        # not sure if this takes tensors ...........

        # reinsert into graphs
        states, acs, advs, rs, _ = convert_batch(batch)
        values, ac_logprobs, entropy = self._evaluate(states, acs)
        pi_err = self._policy_entropy_loss(ac_logprobs, advs)
        value_err = (values - rs).pow(2).mean()

        self.optimizer.zero_grad()
        overall_err = value_err + pi_err - entropy * 0.1
        overall_err.backward()

    def _evaluate(self, x, actions):
        logits, values, features = self(x)
        log_probs = F.log_softmax(logits)
        probs = self.probs(logits)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).mean()
        return values, action_log_probs, entropy

    def forward(self, x):
        res = self.hidden_layers(x)
        logits = self.logits(res)
        value = self.value_branch(res)
        return logits, value, []

    ########### EXTERNAL API ##################
    def model_update(self, batch):
        """ Implements compute + apply """
        # TODO(rliaw): Pytorch has nice
        # caching property that doesn't require
        # full batch to be passed in - can exploit that
        self._backward(batch)
        self.optimizer.step()

    def compute_gradients(self, batch):
        self._backward(batch)
        # Note that return values are just references;
        # calling zero_grad will modify the values
        return [p.grad.data.numpy() for p in self.parameters()], {}

    def apply_gradients(self, grads):
        for g, p in zip(grads, self.parameters()):
            p.grad = Variable(torch.from_numpy(g))
        self.optimizer.step()

    def compute(self, x, *args):
        raise NotImplementedError

    def compute_logits(self, x, *args):
        raise NotImplementedError

    def value(self, x, *args):
        raise NotImplementedError

    def get_weights(self):
        ## !! This only returns references to the data.
        return self.state_dict()

    def set_weights(self, weights):
        self.load_state_dict(weights)

    def get_initial_features(self):
        return [None]
