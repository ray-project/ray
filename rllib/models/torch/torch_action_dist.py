import numpy as np

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_torch

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
        assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
        inputs /= temperature
        super().__init__(inputs, model)
        self.dist = torch.distributions.categorical.Categorical(
            logits=self.inputs)

    @override(ActionDistribution)
    def deterministic_sample(self):
        return self.dist.probs.argmax(dim=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return action_space.n


class TorchMultiCategorical(TorchDistributionWrapper):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    @override(TorchDistributionWrapper)
    def __init__(self, inputs, model, input_lens):
        super().__init__(inputs, model)
        inputs_split = self.inputs.split(input_lens, dim=1)
        self.cats = [
            torch.distributions.categorical.Categorical(logits=input_)
            for input_ in inputs_split
        ]

    @override(TorchDistributionWrapper)
    def sample(self):
        arr = [cat.sample() for cat in self.cats]
        ret = torch.stack(arr, dim=1)
        return ret

    @override(ActionDistribution)
    def deterministic_sample(self):
        arr = [torch.argmax(cat.probs, -1) for cat in self.cats]
        ret = torch.stack(arr, dim=1)
        return ret

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
        return self.dist.mean

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
