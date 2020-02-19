from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    tf_function
from ray.rllib.utils.tuple_actions import TupleActions

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
                 framework="tf",
                 static_params=None,
                 time_dependent_params=None,
                 **kwargs):
        """Initializes a StochasticSampling Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            framework (Optional[str]): One of None, "tf", "torch".
            static_params (Optional[dict]): Parameters to be passed as-is into
                the action distribution class' constructor.
            time_dependent_params (dict): Parameters to be evaluated based on
                `timestep` and then passed into the action distribution
                class' constructor.
        """
        assert framework is not None
        super().__init__(
            action_space=action_space, framework=framework, **kwargs)

        self.static_params = static_params or {}

        # TODO(sven): Support scheduled params whose values depend on timestep
        #  and that will be passed into the distribution's c'tor.
        self.time_dependent_params = time_dependent_params or {}

    @override(Exploration)
    def get_exploration_action(self,
                               model_output,
                               model,
                               action_dist_class,
                               explore=True,
                               timestep=None):
        kwargs = self.static_params.copy()

        # TODO(sven): create schedules for these via easy-config patterns
        #  These can be used anywhere in configs, where schedules are wanted:
        #  e.g. lr=[0.003, 0.00001, 100k] <- linear anneal from 0.003, to
        #  0.00001 over 100k ts.
        # if self.time_dependent_params:
        #    for k, v in self.time_dependent_params:
        #        kwargs[k] = v(timestep)
        constructor, _ = ModelCatalog.get_action_dist(
            self.action_space,
            None,
            action_dist_class,
            framework=self.framework)
        action_dist = constructor(model_output, model, **kwargs)

        if self.framework == "torch":
            return self._get_torch_exploration_action(action_dist, explore)
        else:
            return self._get_tf_exploration_action_op(action_dist, explore)

    @staticmethod
    @tf_function(tf)
    def _get_tf_exploration_action_op(action_dist, explore):
        if explore:
            action = action_dist.sample()
            # TODO(sven): Change `sample` to accept `sample(logp=True|False)`
            logp = action_dist.sampled_action_logp()
        else:
            action = action_dist.deterministic_sample()
            # TODO(sven): Move into (deterministic_)sample(logp=True|False)
            if isinstance(action, TupleActions):
                batch_size = tf.shape(action[0][0])[0]
            else:
                batch_size = tf.shape(action)[0]
            logp = tf.zeros(shape=(batch_size, ), dtype=tf.float32)
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
