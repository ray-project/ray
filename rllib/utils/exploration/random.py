from gym.spaces import Discrete

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf = try_import_tf()
torch, _ = try_import_torch()


class Random(Exploration):
    """A random action selector (deterministic/greedy for explore=False).
    """

    def __init__(self, action_space, framework="tf", **kwargs):
        """

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert isinstance(action_space, Discrete)
        super().__init__(
            action_space=action_space, framework=framework, **kwargs)

    @override(Exploration)
    def get_exploration_action(self,
                               model_output,
                               model,
                               action_dist_class=None,
                               explore=True,
                               timestep=None):
        # Instantiate the distribution object.
        action_dist = action_dist_class(model_output, model)

        if self.framework == "tf":
            return self._get_tf_exploration_action_op(action_dist, explore,
                                                      timestep)
        else:
            pass  # TODO

    @tf.function
    def _get_tf_exploration_action_op(self, action_dist, explore, timestep):
        if explore:
            action = self.action_space.sample()
            # Will be unnecessary, once we support batch/time-aware Spaces.
            action = tf.expand_dims(tf.cast(action, dtype=tf.int32), 0)
        else:
            action = tf.cast(
                action_dist.deterministic_sample(), dtype=tf.int32)
        logp = tf.zeros_like(action, dtype=tf.float32)
        return action, logp
