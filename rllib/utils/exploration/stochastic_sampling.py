from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils import try_import_tree
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    TensorType

tf = try_import_tf()
torch, _ = try_import_torch()
tree = try_import_tree()


class StochasticSampling(Exploration):
    """An exploration that simply samples from a distribution.

    The sampling can be made deterministic by passing explore=False into
    the call to `get_exploration_action`.
    Also allows for scheduled parameters for the distributions, such as
    lowering stddev, temperature, etc.. over time.
    """

    def __init__(self, action_space, *, framework: str, model: ModelV2,
                 **kwargs):
        """Initializes a StochasticSampling Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (str): One of None, "tf", "torch".
        """
        assert framework is not None
        super().__init__(
            action_space, model=model, framework=framework, **kwargs)

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        if self.framework == "torch":
            return self._get_torch_exploration_action(action_distribution,
                                                      explore)
        else:
            return self._get_tf_exploration_action_op(action_distribution,
                                                      explore)

    def _get_tf_exploration_action_op(self, action_dist, explore):
        sample = action_dist.sample()
        deterministic_sample = action_dist.deterministic_sample()
        action = tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=lambda: sample,
            false_fn=lambda: deterministic_sample)

        def logp_false_fn():
            batch_size = tf.shape(tree.flatten(action)[0])[0]
            return tf.zeros(shape=(batch_size, ), dtype=tf.float32)

        logp = tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=lambda: action_dist.sampled_action_logp(),
            false_fn=logp_false_fn)

        return action, logp

    @staticmethod
    def _get_torch_exploration_action(action_dist, explore):
        if explore:
            action = action_dist.sample()
            logp = action_dist.sampled_action_logp()
        else:
            action = action_dist.deterministic_sample()
            logp = torch.zeros((action.size()[0], ), dtype=torch.float32)
        return action, logp
