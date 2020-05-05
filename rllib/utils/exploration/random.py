from gym.spaces import Discrete, MultiDiscrete, Tuple
import numpy as np
import tree
from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils import force_tuple
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    TensorType

tf = try_import_tf()
torch, _ = try_import_torch()


class Random(Exploration):
    """A random action selector (deterministic/greedy for explore=False).

    If explore=True, returns actions randomly from `self.action_space` (via
    Space.sample()).
    If explore=False, returns the greedy/max-likelihood action.
    """

    def __init__(self, action_space, *, model, framework, **kwargs):
        """Initialize a Random Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        super().__init__(
            action_space=action_space,
            model=model,
            framework=framework,
            **kwargs)

        # Determine py_func types, depending on our action-space.
        if isinstance(self.action_space, (Discrete, MultiDiscrete)) or \
                (isinstance(self.action_space, Tuple) and
                 isinstance(self.action_space[0], (Discrete, MultiDiscrete))):
            self.dtype_sample, self.dtype = (tf.int64, tf.int32)
        else:
            self.dtype_sample, self.dtype = (tf.float64, tf.float32)

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        # Instantiate the distribution object.
        if self.framework == "tf":
            return self.get_tf_exploration_action_op(action_distribution,
                                                     explore)
        else:
            return self.get_torch_exploration_action(action_distribution,
                                                     explore)

    def get_tf_exploration_action_op(self, action_dist, explore):
        def scan_fn(accum, iter):
            r = tf.cast(tf.py_func(
                self.action_space.sample, [], self.dtype_sample),
                dtype=self.dtype)
            r.set_shape(self.action_space.shape)
            return r

        def true_fn():
            batch_size = 1
            req = force_tuple(
                action_dist.required_model_output_shape(
                    self.action_space, self.model.model_config))
            # Add a batch dimension?
            if len(action_dist.inputs.shape) == len(req) + 1:
                batch_size = tf.shape(action_dist.inputs)[0]
            actions = tf.scan(scan_fn, tf.range(batch_size))
            return actions

        def false_fn():
            return tf.cast(
                action_dist.deterministic_sample(), dtype=self.dtype)

        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=true_fn,
            false_fn=false_fn)

        # TODO(sven): Move into (deterministic_)sample(logp=True|False)
        batch_size = tf.shape(tree.flatten(action)[0])[0]
        logp = tf.zeros(shape=(batch_size, ), dtype=tf.float32)
        return action, logp

    def get_torch_exploration_action(self, action_dist, explore):
        if explore:
            req = force_tuple(
                action_dist.required_model_output_shape(
                    self.action_space, self.model.model_config))
            # Add a batch dimension?
            if len(action_dist.inputs.shape) == len(req) + 1:
                batch_size = action_dist.inputs.shape[0]
                a = np.stack([self.action_space.sample() for _ in range(batch_size)])
            else:
                a = self.action_space.sample()
            # Convert action to torch tensor.
            action = torch.from_numpy(a).to(self.device)
        else:
            action = action_dist.deterministic_sample()
        logp = torch.zeros(
            (action.size()[0], ), dtype=torch.float32, device=self.device)
        return action, logp
