from gym.spaces import Discrete, MultiDiscrete, Tuple
from typing import Union

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    TensorType
from ray.rllib.utils.tuple_actions import TupleActions
from ray.rllib.models.modelv2 import ModelV2

tf = try_import_tf()
torch, _ = try_import_torch()


class Random(Exploration):
    """A random action selector (deterministic/greedy for explore=False).

    If explore=True, returns actions randomly from `self.action_space` (via
    Space.sample()).
    If explore=False, returns the greedy/max-likelihood action.
    """

    def __init__(self, action_space, *, framework="tf", **kwargs):
        """Initialize a Random Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        super().__init__(
            action_space=action_space, framework=framework, **kwargs)

        # Determine py_func types, depending on our action-space.
        if isinstance(self.action_space, (Discrete, MultiDiscrete)) or \
                (isinstance(self.action_space, Tuple) and
                 isinstance(self.action_space[0], (Discrete, MultiDiscrete))):
            self.dtype_sample, self.dtype = (tf.int64, tf.int32)
        else:
            self.dtype_sample, self.dtype = (tf.float64, tf.float32)

    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        # Instantiate the distribution object.
        action_dist = action_dist_class(distribution_inputs, model)
        if self.framework == "tf":
            return self.get_tf_exploration_action_op(action_dist, explore)
        else:
            return self.get_torch_exploration_action(action_dist, explore)

    def get_tf_exploration_action_op(self, action_dist, explore):
        def true_fn():
            action = tf.py_function(self.action_space.sample, [],
                                    self.dtype_sample)
            # Will be unnecessary, once we support batch/time-aware Spaces.
            return tf.expand_dims(tf.cast(action, dtype=self.dtype), 0)

        def false_fn():
            return tf.cast(
                action_dist.deterministic_sample(), dtype=self.dtype)

        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=true_fn,
            false_fn=false_fn)

        # TODO(sven): Move into (deterministic_)sample(logp=True|False)
        if isinstance(action, TupleActions):
            batch_size = tf.shape(action[0][0])[0]
        else:
            batch_size = tf.shape(action)[0]
        logp = tf.zeros(shape=(batch_size, ), dtype=tf.float32)
        return action, logp

    def get_torch_exploration_action(self, action_dist, explore):
        tensor_fn = torch.LongTensor if \
            type(self.action_space) in [Discrete, MultiDiscrete] else \
            torch.FloatTensor
        if explore:
            # Unsqueeze will be unnecessary, once we support batch/time-aware
            # Spaces.
            action = tensor_fn(self.action_space.sample()).unsqueeze(0)
        else:
            action = tensor_fn(action_dist.deterministic_sample())
        logp = torch.zeros((action.size()[0], ), dtype=torch.float32)
        return action, logp
