from typing import Union

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    TensorType
from ray.rllib.utils.tuple_actions import TupleActions
from ray.rllib.models.modelv2 import ModelV2

tf = try_import_tf()
torch, _ = try_import_torch()


class StochasticSampling(Exploration):
    """An exploration that simply samples from a distribution.

    The sampling can be made deterministic by passing explore=False into
    the call to `get_exploration_action`.
    Also allows for scheduled parameters for the distributions, such as
    lowering stddev, temperature, etc.. over time.
    """

    def __init__(self,
                 action_space,
                 *,
                 static_params=None,
                 time_dependent_params=None,
                 framework="tf",
                 **kwargs):
        """Initializes a StochasticSampling Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            static_params (Optional[dict]): Parameters to be passed as-is into
                the action distribution class' constructor.
            time_dependent_params (dict): Parameters to be evaluated based on
                `timestep` and then passed into the action distribution
                class' constructor.
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert framework is not None
        super().__init__(action_space, framework=framework, **kwargs)

        self.static_params = static_params or {}

        # TODO(sven): Support scheduled params whose values depend on timestep
        #  and that will be passed into the distribution's c'tor.
        self.time_dependent_params = time_dependent_params or {}

    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        kwargs = self.static_params.copy()

        # TODO(sven): create schedules for these via easy-config patterns
        #  These can be used anywhere in configs, where schedules are wanted:
        #  e.g. lr=[0.003, 0.00001, 100k] <- linear anneal from 0.003, to
        #  0.00001 over 100k ts.
        # if self.time_dependent_params:
        #    for k, v in self.time_dependent_params:
        #        kwargs[k] = v(timestep)
        action_dist = action_dist_class(distribution_inputs, model, **kwargs)

        if self.framework == "torch":
            return self._get_torch_exploration_action(action_dist, explore)
        else:
            return self._get_tf_exploration_action_op(action_dist, explore)

    def _get_tf_exploration_action_op(self, action_dist, explore):
        sample = action_dist.sample()
        deterministic_sample = action_dist.deterministic_sample()
        action = tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=lambda: sample,
            false_fn=lambda: deterministic_sample)

        def logp_false_fn():
            # TODO(sven): Move into (deterministic_)sample(logp=True|False)
            if isinstance(sample, TupleActions):
                batch_size = tf.shape(action[0])[0]
            else:
                batch_size = tf.shape(action)[0]
            return tf.zeros(shape=(batch_size, ), dtype=tf.float32)

        logp = tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=lambda: action_dist.sampled_action_logp(),
            false_fn=logp_false_fn)

        return TupleActions(action) if isinstance(sample, TupleActions) \
            else action, logp

    @staticmethod
    def _get_torch_exploration_action(action_dist, explore):
        if explore:
            action = action_dist.sample()
            logp = action_dist.sampled_action_logp()
        else:
            action = action_dist.deterministic_sample()
            logp = torch.zeros((action.size()[0], ), dtype=torch.float32)
        return action, logp
