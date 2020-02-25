from gym.spaces import Discrete, MultiDiscrete, Tuple

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    tf_function
from ray.rllib.utils.tuple_actions import TupleActions

tf = try_import_tf()
torch, _ = try_import_torch()


class Random(Exploration):
    """A random action selector (deterministic/greedy for explore=False).

    If explore=True, returns actions randomly from `self.action_space` (via
    Space.sample()).
    If explore=False, returns the greedy/max-likelihood action.
    """

    def __init__(self, action_space, framework="tf", **kwargs):
        """Initialize a Random Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        super().__init__(
            action_space=action_space, framework=framework, **kwargs)

    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs,
                               action_dist_class,
                               model=None,
                               explore=True,
                               timestep=None):
        # Instantiate the distribution object.
        action_dist = action_dist_class(distribution_inputs, model)
        if self.framework == "tf":
            return self.get_tf_exploration_action_op(action_dist, explore)
        else:
            return self.get_torch_exploration_action(action_dist, explore)

    @tf_function(tf)
    def get_tf_exploration_action_op(self, action_dist, explore):
        # Determine py_func types, depending on our action-space.
        if isinstance(self.action_space, (Discrete, MultiDiscrete)) or \
                (isinstance(self.action_space, Tuple) and
                 isinstance(self.action_space[0], (Discrete, MultiDiscrete))):
            dtype_sample, dtype = (tf.int64, tf.int32)
        else:
            dtype_sample, dtype = (tf.float64, tf.float32)

        if explore:
            action = tf.py_function(self.action_space.sample, [], dtype_sample)
            # Will be unnecessary, once we support batch/time-aware Spaces.
            action = tf.expand_dims(tf.cast(action, dtype=dtype), 0)
        else:
            action = tf.cast(
                action_dist.deterministic_sample(), dtype=dtype)

        # TODO(sven): Move into (deterministic_)sample(logp=True|False)
        if isinstance(action, TupleActions):
            batch_size = tf.shape(action[0][0])[0]
        else:
            batch_size = tf.shape(action)[0]
        logp = tf.zeros(shape=(batch_size, ), dtype=tf.float32)
        return action, logp

    def get_torch_exploration_action(self, action_dist, explore):
        if explore:
            # Unsqueeze will be unnecessary, once we support batch/time-aware
            # Spaces.
            action = torch.LongTensor(self.action_space.sample()).unsqueeze(0)
        else:
            action = torch.LongTensor(action_dist.deterministic_sample())
        logp = torch.zeros((action.size()[0], ), dtype=torch.float32)
        return action, logp
