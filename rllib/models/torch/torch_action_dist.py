import functools
from math import log
import numpy as np
import tree

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import SMALL_NUMBER, MIN_LOG_NN_OUTPUT, \
    MAX_LOG_NN_OUTPUT
from ray.rllib.utils.torch_ops import atanh

torch, nn = try_import_torch()


class TorchDistributionWrapper(ActionDistribution):
    """Wrapper class for torch.distributions."""

    @override(ActionDistribution)
    def __init__(self, inputs, model):
        if not isinstance(inputs, torch.Tensor):
            inputs = torch.Tensor(inputs)
        super().__init__(inputs, model)
        # Store the last sample here.
        self.last_sample = None

    @override(ActionDistribution)
    def logp(self, actions):
        return self.dist.log_prob(actions)

    @override(ActionDistribution)
    def entropy(self):
        return self.dist.entropy()

    @override(ActionDistribution)
    def kl(self, other):
        return torch.distributions.kl.kl_divergence(self.dist, other.dist)

    @override(ActionDistribution)
    def sample(self):
        self.last_sample = self.dist.sample()
        return self.last_sample

    @override(ActionDistribution)
    def sampled_action_logp(self):
        assert self.last_sample is not None
        return self.logp(self.last_sample)


class TorchCategorical(TorchDistributionWrapper):
    """Wrapper class for PyTorch Categorical distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs, model=None, temperature=1.0):
        if temperature != 1.0:
            assert temperature > 0.0, \
                "Categorical `temperature` must be > 0.0!"
            inputs /= temperature
        super().__init__(inputs, model)
        self.dist = torch.distributions.categorical.Categorical(
            logits=self.inputs)

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self.dist.probs.argmax(dim=1)
        return self.last_sample

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return action_space.n


class TorchMultiCategorical(TorchDistributionWrapper):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    @override(TorchDistributionWrapper)
    def __init__(self, inputs, model, input_lens):
        super().__init__(inputs, model)
        # If input_lens is np.ndarray or list, force-make it a tuple.
        inputs_split = self.inputs.split(tuple(input_lens), dim=1)
        self.cats = [
            torch.distributions.categorical.Categorical(logits=input_)
            for input_ in inputs_split
        ]

    @override(TorchDistributionWrapper)
    def sample(self):
        arr = [cat.sample() for cat in self.cats]
        self.last_sample = torch.stack(arr, dim=1)
        return self.last_sample

    @override(ActionDistribution)
    def deterministic_sample(self):
        arr = [torch.argmax(cat.probs, -1) for cat in self.cats]
        self.last_sample = torch.stack(arr, dim=1)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def logp(self, actions):
        # # If tensor is provided, unstack it into list.
        if isinstance(actions, torch.Tensor):
            actions = torch.unbind(actions, dim=1)
        logps = torch.stack(
            [cat.log_prob(act) for cat, act in zip(self.cats, actions)])
        return torch.sum(logps, dim=0)

    @override(ActionDistribution)
    def multi_entropy(self):
        return torch.stack([cat.entropy() for cat in self.cats], dim=1)

    @override(TorchDistributionWrapper)
    def entropy(self):
        return torch.sum(self.multi_entropy(), dim=1)

    @override(ActionDistribution)
    def multi_kl(self, other):
        return torch.stack(
            [
                torch.distributions.kl.kl_divergence(cat, oth_cat)
                for cat, oth_cat in zip(self.cats, other.cats)
            ],
            dim=1,
        )

    @override(TorchDistributionWrapper)
    def kl(self, other):
        return torch.sum(self.multi_kl(other), dim=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.sum(action_space.nvec)


class TorchDiagGaussian(TorchDistributionWrapper):
    """Wrapper class for PyTorch Normal distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs, model):
        super().__init__(inputs, model)
        mean, log_std = torch.chunk(inputs, 2, dim=1)
        self.dist = torch.distributions.normal.Normal(mean, torch.exp(log_std))

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self.dist.mean
        return self.last_sample

    @override(TorchDistributionWrapper)
    def logp(self, actions):
        return super().logp(actions).sum(-1)

    @override(TorchDistributionWrapper)
    def entropy(self):
        return super().entropy().sum(-1)

    @override(TorchDistributionWrapper)
    def kl(self, other):
        return super().kl(other).sum(-1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape) * 2


class TorchSquashedGaussian(TorchDistributionWrapper):
    """A tanh-squashed Gaussian distribution defined by: mean, std, low, high.

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """

    def __init__(self, inputs, model, low=-1.0, high=1.0):
        """Parameterizes the distribution via `inputs`.

        Args:
            low (float): The lowest possible sampling value
                (excluding this value).
            high (float): The highest possible sampling value
                (excluding this value).
        """
        super().__init__(inputs, model)
        # Split inputs into mean and log(std).
        mean, log_std = torch.chunk(self.inputs, 2, dim=-1)
        # Clip `scale` values (coming from NN) to reasonable values.
        log_std = torch.clamp(log_std, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT)
        std = torch.exp(log_std)
        self.dist = torch.distributions.normal.Normal(mean, std)
        assert np.all(np.less(low, high))
        self.low = low
        self.high = high

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self._squash(self.dist.mean)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def sample(self):
        # Use the reparameterization version of `dist.sample` to allow for
        # the results to be backprop'able e.g. in a loss term.
        normal_sample = self.dist.rsample()
        self.last_sample = self._squash(normal_sample)
        return self.last_sample

    @override(ActionDistribution)
    def logp(self, x):
        # Unsquash values (from [low,high] to ]-inf,inf[)
        unsquashed_values = self._unsquash(x)
        # Get log prob of unsquashed values from our Normal.
        log_prob_gaussian = self.dist.log_prob(unsquashed_values)
        # For safety reasons, clamp somehow, only then sum up.
        log_prob_gaussian = torch.clamp(log_prob_gaussian, -100, 100)
        log_prob_gaussian = torch.sum(log_prob_gaussian, dim=-1)
        # Get log-prob for squashed Gaussian.
        unsquashed_values_tanhd = torch.tanh(unsquashed_values)
        log_prob = log_prob_gaussian - torch.sum(
            torch.log(1 - unsquashed_values_tanhd**2 + SMALL_NUMBER), dim=-1)
        return log_prob

    def _squash(self, raw_values):
        # Returned values are within [low, high] (including `low` and `high`).
        squashed = ((torch.tanh(raw_values) + 1.0) / 2.0) * \
            (self.high - self.low) + self.low
        return torch.clamp(squashed, self.low, self.high)

    def _unsquash(self, values):
        normed_values = (values - self.low) / (self.high - self.low) * 2.0 - \
                        1.0
        # Stabilize input to atanh.
        save_normed_values = torch.clamp(normed_values, -1.0 + SMALL_NUMBER,
                                         1.0 - SMALL_NUMBER)
        unsquashed = atanh(save_normed_values)
        return unsquashed


class TorchBeta(TorchDistributionWrapper):
    """
    A Beta distribution is defined on the interval [0, 1] and parameterized by
    shape parameters alpha and beta (also called concentration parameters).

    PDF(x; alpha, beta) = x**(alpha - 1) (1 - x)**(beta - 1) / Z
        with Z = Gamma(alpha) Gamma(beta) / Gamma(alpha + beta)
        and Gamma(n) = (n - 1)!
    """

    def __init__(self, inputs, model, low=0.0, high=1.0):
        super().__init__(inputs, model)
        # Stabilize input parameters (possibly coming from a linear layer).
        self.inputs = torch.clamp(self.inputs, log(SMALL_NUMBER),
                                  -log(SMALL_NUMBER))
        self.inputs = torch.log(torch.exp(self.inputs) + 1.0) + 1.0
        self.low = low
        self.high = high
        alpha, beta = torch.chunk(self.inputs, 2, dim=-1)
        # Note: concentration0==beta, concentration1=alpha (!)
        self.dist = torch.distributions.Beta(
            concentration1=alpha, concentration0=beta)

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self._squash(self.dist.mean)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def sample(self):
        # Use the reparameterization version of `dist.sample` to allow for
        # the results to be backprop'able e.g. in a loss term.
        normal_sample = self.dist.rsample()
        self.last_sample = self._squash(normal_sample)
        return self.last_sample

    @override(ActionDistribution)
    def logp(self, x):
        unsquashed_values = self._unsquash(x)
        return torch.sum(self.dist.log_prob(unsquashed_values), dim=-1)

    def _squash(self, raw_values):
        return raw_values * (self.high - self.low) + self.low

    def _unsquash(self, values):
        return (values - self.low) / (self.high - self.low)


class TorchDeterministic(TorchDistributionWrapper):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).
    """

    @override(ActionDistribution)
    def deterministic_sample(self):
        return self.inputs

    @override(TorchDistributionWrapper)
    def sampled_action_logp(self):
        return 0.0

    @override(TorchDistributionWrapper)
    def sample(self):
        return self.deterministic_sample()

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape)


class TorchMultiActionDistribution(TorchDistributionWrapper):
    """Action distribution that operates for list of (flattened) actions.

    Args:
        inputs (Tensor list): A list of tensors from which to compute samples.
    """

    def __init__(self, inputs, model, action_space, child_distributions,
                 input_lens):
        inputs = tree.map_structure(
            lambda i: torch.Tensor(i) if not isinstance(i, torch.Tensor)
            else i, inputs)
        #for i, input_ in enumerate(inputs):
        #    if not isinstance(input_, torch.Tensor):
        #        inputs[i] = torch.Tensor(input_)
        super().__init__(inputs, model)
        self.input_lens = input_lens
        split_inputs = tree.unflatten_as(
            child_distributions, torch.split(inputs, self.input_lens, dim=1))
        self.child_distributions = tree.map_structure(
            lambda dist, input_: dist(input_, model),
            child_distributions, split_inputs)
        self.flat_child_distributions = tree.flatten(self.child_distributions)



        #self.input_lens = input_lens
        #split_inputs = torch.split(self.inputs, self.input_lens, dim=1)
        #child_list = []
        #for i, distribution in enumerate(child_distributions):
        #    child_list.append(distribution(split_inputs[i], model))
        #self.child_distributions = child_list

    @override(ActionDistribution)
    def logp(self, x):

        def map_(val, dist):
            # Remove extra categorical dimension.
            if isinstance(dist, TorchCategorical):
                val = torch.squeeze(val, dim=-1).int()
            return dist.logp(val)

        # Remove extra categorical dimension
        logps = tree.map_structure(map_, x, self.child_distributions)
        flat_logps = tree.flatten(logps)

        #log_list = [
        #    distribution.logp(split_x) for distribution, split_x in zip(
        #        self.child_distributions, split_list)
        #]
        return functools.reduce(lambda a, b: a + b, flat_logps)

        #split_indices = []
        #for dist in self.child_distributions:
        #    if isinstance(dist, TorchCategorical):
        #        split_indices.append(1)
        #    else:
        #        split_indices.append(dist.sample().size()[1])
        #split_list = list(torch.split(x, split_indices, dim=1))
        #for i, distribution in enumerate(self.child_distributions):
        #    # Remove extra categorical dimension
        #    if isinstance(distribution, TorchCategorical):
        #        split_list[i] = torch.squeeze(split_list[i], dim=-1).int()
        #log_list = [
        #    distribution.logp(split_x) for distribution, split_x in zip(
        #        self.child_distributions, split_list)
        #]
        return functools.reduce(lambda a, b: a + b, log_list)

    @override(ActionDistribution)
    def kl(self, other):
        kl_list = [d.kl(o) for d, o in zip(
            self.flat_child_distributions, other.flat_child_distributions)]
        return functools.reduce(lambda a, b: a + b, kl_list)
        #kl_list = [
        #    distribution.kl(other_distribution)
        #    for distribution, other_distribution in zip(
        #        self.child_distributions, other.child_distributions)
        #]
        #return functools.reduce(lambda a, b: a + b, kl_list)

    @override(ActionDistribution)
    def entropy(self):
        entropy_list = [d.entropy() for d in self.flat_child_distributions]
        return functools.reduce(lambda a, b: a + b, entropies)
        #entropy_list = [s.entropy() for s in self.child_distributions]
        #return functools.reduce(lambda a, b: a + b, entropy_list)

    @override(ActionDistribution)
    def sample(self):
        return tree.map_structure(
            lambda s: s.sample(), self.child_distributions)
        ##TupleActions
        #return tuple([s.sample() for s in self.child_distributions])

    @override(ActionDistribution)
    def deterministic_sample(self):
        return tree.map_structure(
            lambda s: s.deterministic_sample(), self.child_distributions)
        ##TupleActions
        #return tuple(
        #    [s.deterministic_sample() for s in self.child_distributions])

    @override(TorchDistributionWrapper)
    def sampled_action_logp(self):
        p = self.flat_child_distributions[0].sampled_action_logp()
        for c in self.flat_child_distributions[1:]:
            p += c.sampled_action_logp()
        return p

        #p = self.child_distributions[0].sampled_action_logp()
        #for c in self.child_distributions[1:]:
        #    p += c.sampled_action_logp()
        #return p
