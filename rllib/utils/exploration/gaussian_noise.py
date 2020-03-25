from typing import Union

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.exploration.random import Random
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable, TensorType
from ray.rllib.utils.schedules.piecewise_schedule import PiecewiseSchedule
from ray.rllib.models.modelv2 import ModelV2

tf = try_import_tf()
torch, _ = try_import_torch()


class GaussianNoise(Exploration):
    """An exploration that adds white noise to continuous actions.

    If explore=True, returns actions plus scale (<-annealed over time) x
        Gaussian noise. Also, some completely random period is possible at the
        beginning.
    If explore=False, returns the deterministic action.
    """

    def __init__(self,
                 action_space,
                 *,
                 random_timesteps=1000,
                 stddev=0.1,
                 initial_scale=1.0,
                 final_scale=0.02,
                 scale_timesteps=10000,
                 scale_schedule=None,
                 framework="tf",
                 **kwargs):
        """Initializes a GaussianNoise Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            random_timesteps (int): The number of timesteps for which to act
                completely randomly. Only after this number of timesteps, the
                `self.scale` annealing process will start (see below).
            stddev (float): The stddev (sigma) to use for the
                Gaussian noise to be added to the actions.
            initial_scale (float): The initial scaling weight to multiply
                the noise with.
            final_scale (float): The final scaling weight to multiply
                the noise with.
            scale_timesteps (int): The timesteps over which to linearly anneal
                the scaling factor (after(!) having used random actions for
                `random_timesteps` steps.
            scale_schedule (Optional[Schedule]): An optional Schedule object
                to use (instead of constructing one from the given parameters).
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert framework is not None
        super().__init__(action_space, framework=framework, **kwargs)

        self.random_timesteps = random_timesteps
        self.random_exploration = Random(
            action_space, framework=self.framework, **kwargs)
        self.stddev = stddev
        # The `scale` annealing schedule.
        self.scale_schedule = scale_schedule or PiecewiseSchedule(
            endpoints=[(random_timesteps, initial_scale),
                       (random_timesteps + scale_timesteps, final_scale)],
            outside_value=final_scale,
            framework=self.framework)

        # The current timestep value (tf-var or python int).
        self.last_timestep = get_variable(
            0, framework=self.framework, tf_name="timestep")

    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        # Adds IID Gaussian noise for exploration, TD3-style.
        action_dist = action_dist_class(distribution_inputs, model)

        if self.framework == "torch":
            return self._get_torch_exploration_action(action_dist, explore,
                                                      timestep)
        else:
            return self._get_tf_exploration_action_op(action_dist, explore,
                                                      timestep)

    def _get_tf_exploration_action_op(self, action_dist, explore, timestep):
        ts = timestep if timestep is not None else self.last_timestep

        # The deterministic actions (if explore=False).
        deterministic_actions = action_dist.deterministic_sample()

        # Take a Gaussian sample with our stddev (mean=0.0) and scale it.
        gaussian_sample = self.scale_schedule(ts) * tf.random_normal(
            tf.shape(deterministic_actions), stddev=self.stddev)

        # Stochastic actions could either be: random OR action + noise.
        random_actions, _ = \
            self.random_exploration.get_tf_exploration_action_op(
                action_dist, explore)
        stochastic_actions = tf.cond(
            pred=ts <= self.random_timesteps,
            true_fn=lambda: random_actions,
            false_fn=lambda: tf.clip_by_value(
                deterministic_actions + gaussian_sample,
                self.action_space.low * tf.ones_like(deterministic_actions),
                self.action_space.high * tf.ones_like(deterministic_actions))
        )

        # Chose by `explore` (main exploration switch).
        batch_size = tf.shape(deterministic_actions)[0]
        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=lambda: stochastic_actions,
            false_fn=lambda: deterministic_actions)
        # Logp=always zero.
        logp = tf.zeros(shape=(batch_size, ), dtype=tf.float32)

        # Increment `last_timestep` by 1 (or set to `timestep`).
        assign_op = \
            tf.assign_add(self.last_timestep, 1) if timestep is None else \
            tf.assign(self.last_timestep, timestep)
        with tf.control_dependencies([assign_op]):
            return action, logp

    def _get_torch_exploration_action(self, action_dist, explore, timestep):
        # Set last timestep or (if not given) increase by one.
        self.last_timestep = timestep if timestep is not None else \
            self.last_timestep + 1

        # Apply exploration.
        if explore:
            # Random exploration phase.
            if self.last_timestep <= self.random_timesteps:
                action, _ = \
                    self.random_exploration.get_torch_exploration_action(
                        action_dist, True)
            # Take a Gaussian sample with our stddev (mean=0.0) and scale it.
            else:
                det_actions = action_dist.deterministic_sample()
                scale = self.scale_schedule(self.last_timestep)
                gaussian_sample = scale * torch.normal(
                    mean=0.0, stddev=self.stddev, size=det_actions.size())
                action = torch.clamp(
                    det_actions + gaussian_sample,
                    self.action_space.low * torch.ones_like(det_actions),
                    self.action_space.high * torch.ones_like(det_actions))
        # No exploration -> Return deterministic actions.
        else:
            action = action_dist.deterministic_sample()

        # Logp=always zero.
        logp = torch.zeros(shape=(action.size()[0], ), dtype=torch.float32)

        return action, logp

    @override(Exploration)
    def get_info(self):
        """Returns the current scale value.

        Returns:
            Union[float,tf.Tensor[float]]: The current scale value.
        """
        return self.scale_schedule(self.last_timestep)
