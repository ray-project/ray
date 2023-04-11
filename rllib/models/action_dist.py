import functools
import numpy as np
import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.spaces.space_utils import flatten_space
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.typing import TensorType, List, Tuple, Union, ModelConfigDict


@DeveloperAPI
class ActionDistribution:
    """The policy action distribution of an agent.

    Attributes:
        inputs: input vector to compute samples from.
        model (ModelV2): reference to model producing the inputs.
    """

    framework: str

    @DeveloperAPI
    def __init__(self, inputs: List[TensorType], model: ModelV2):
        """Initializes an ActionDist object.

        Args:
            inputs: input vector to compute samples from.
            model (ModelV2): reference to model producing the inputs. This
                is mainly useful if you want to use model variables to compute
                action outputs (i.e., for auto-regressive action distributions,
                see examples/autoregressive_action_dist.py).
        """
        self.inputs = inputs
        self.model = model

    @DeveloperAPI
    def sample(self) -> TensorType:
        """Draw a sample from the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def deterministic_sample(self) -> TensorType:
        """
        Get the deterministic "sampling" output from the distribution.
        This is usually the max likelihood output, i.e. mean for Normal, argmax
        for Categorical, etc..
        """
        raise NotImplementedError

    @DeveloperAPI
    def sampled_action_logp(self) -> TensorType:
        """Returns the log probability of the last sampled action."""
        raise NotImplementedError

    @DeveloperAPI
    def logp(self, x: TensorType) -> TensorType:
        """The log-likelihood of the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def kl(self, other: "ActionDistribution") -> TensorType:
        """The KL-divergence between two action distributions."""
        raise NotImplementedError

    @DeveloperAPI
    def entropy(self) -> TensorType:
        """The entropy of the action distribution."""
        raise NotImplementedError

    def multi_kl(self, other: "ActionDistribution") -> TensorType:
        """The KL-divergence between two action distributions.

        This differs from kl() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.kl(other)

    def multi_entropy(self) -> TensorType:
        """The entropy of the action distribution.

        This differs from entropy() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.entropy()

    @classmethod
    @DeveloperAPI
    def required_model_output_shape(
        cls, action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        """Returns the required shape of an input parameter tensor for a
        particular action space and an optional dict of distribution-specific
        options.

        Args:
            action_space (gym.Space): The action space this distribution will
                be used for, whose shape attributes will be used to determine
                the required shape of the input parameter tensor.
            model_config: Model's config dict (as defined in catalog.py)

        Returns:
            model_output_shape (int or np.ndarray of ints): size of the
                required input vector (minus leading batch dimension).
        """
        raise NotImplementedError


@DeveloperAPI
class MultiActionDistributionMixIn(ActionDistribution):
    """Action distribution that operates on multiple, possibly nested actions."""

    @override(ActionDistribution)
    def kl(self, other: "MultiActionDistributionMixIn") -> TensorType:
        kl_list = [
            d.kl(o)
            for d, o in zip(
                self.flat_child_distributions, other.flat_child_distributions
            )
        ]
        return functools.reduce(lambda a, b: a + b, kl_list)

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        entropy_list = [d.entropy() for d in self.flat_child_distributions]
        return functools.reduce(lambda a, b: a + b, entropy_list)

    @override(ActionDistribution)
    def sample(self) -> TensorType:
        child_distributions = tree.unflatten_as(
            self.action_space_struct, self.flat_child_distributions
        )
        return tree.map_structure(lambda s: s.sample(), child_distributions)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        child_distributions = tree.unflatten_as(
            self.action_space_struct, self.flat_child_distributions
        )
        return tree.map_structure(
            lambda s: s.deterministic_sample(), child_distributions
        )

    @override(ActionDistribution)
    def sampled_action_logp(self) -> TensorType:
        p = self.flat_child_distributions[0].sampled_action_logp()
        for c in self.flat_child_distributions[1:]:
            p += c.sampled_action_logp()
        return p

    @classmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        cls, action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        _, input_lens = cls.get_child_dists_and_input_lens(
            action_space, model_config, framework=cls.framework
        )

        return np.sum(input_lens, dtype=np.int32)

    @staticmethod
    def get_child_dists_and_input_lens(
        action_space: gym.Space, model_config: ModelConfigDict, framework: str
    ) -> Tuple[List[ActionDistribution], List[int]]:
        from ray.rllib.models.catalog import ModelCatalog

        flat_action_space = flatten_space(action_space)
        child_dists_and_input_lens = tree.map_structure(
            lambda s: ModelCatalog.get_action_dist(
                s, model_config, framework=framework
            ),
            flat_action_space,
        )
        child_dists = [e[0] for e in child_dists_and_input_lens]
        input_lens = [int(e[1]) for e in child_dists_and_input_lens]

        return child_dists, input_lens
