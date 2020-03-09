from gym.spaces import Discrete
import numpy as np
from scipy.stats import entropy
from typing import Union

from ray.rllib.env.base_env import BaseEnv
from ray.rllib.policy.policy import ACTION_PROB, ACTION_LOGP
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.epsilon_greedy import EpsilonGreedy
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    TensorType
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.framework import get_variable
from ray.rllib.utils.numpy import softmax

tf = try_import_tf()
torch, _ = try_import_torch()


class ParameterNoise(Exploration):
    """An exploration that changes a Model's parameters.

    Implemented based on:
    [1] https://blog.openai.com/better-exploration-with-parameter-noise/

    At the beginning of an episode, Gaussian noise is added to all weights
    of the model. At the end of the episode, the noise is undone and an action
    diff (pi-delta) is calculated, from which we determine the changes in the
    noise's stddev for the next episode.
    """

    def __init__(self,
                 action_space,
                 *,
                 initial_stddev=1.0,
                 random_timesteps=10000,
                 framework="tf",
                 **kwargs):
        """Initializes a ParameterNoise Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            initial_stddev (float): The initial stddev to use for the noise.
            random_timesteps (int): The number of timesteps to act completely
                randomly (see [1]).
            framework (Optional[str]): One of None, "tf", "torch".
        """
        assert framework is not None
        super().__init__(action_space, framework=framework, **kwargs)

        self.stddev = get_variable(initial_stddev, framework=self.framework,
                                   tf_name="stddev")
        # Our noise to be added to the weights. Each item in `self.noise`
        # corresponds to one Model variable and holding the Gaussian noise to
        # be added to that variable (weight).
        self.noise = None
        # The weight variables of the Model where noise should be applied to.
        # This excludes any variable, whose name contains "LayerNorm" (those
        # are BatchNormalization layers, which should not be perturbed).
        self.model_variables = None

        self.sub_exploration = None
        # For discrete action spaces, use an underlying EpsilonGreedy with a
        # special schedule.
        if isinstance(self.action_space, Discrete):
            self.sub_exploration = EpsilonGreedy(
                self.action_space, epsilon_schedule={
                    "type": "PiecewiseSchedule",
                    "endpoints": [(0, 1.0), (random_timesteps, 0.01)],
                    "outside_value": 0.01})
        # TODO(sven): Implement for any action space.
        else:
            raise NotImplementedError

    @override(Exploration)
    def forward(self, model, input_dict, states, seq_lens, explore=True):
        # Initialize all noise_vars.
        if self.noise is None:
            self.model_variables = [v for v in model.variables()
                                    if "LayerNorm" not in v.name]
            self.noise = []
            for var in self.model_variables:
                self.noise.append(get_variable(
                    np.zeros(var.shape, dtype=np.float32),
                    framework=self.framework,
                    tf_name=var.name.split(":")[0] + "_noisy"))

        if self.framework == "tf" and not tf.executing_eagerly():
            return self._tf_forward_op(
                explore, model, input_dict, states, seq_lens)
        else:
            # Exploration: Normal behavior.
            if explore:
                return model(input_dict, states, seq_lens)
    
            # No exploration: Revert noise, forward-pass, then re-apply noise.
            else:
                remove_op = self._remove_noise(model)
                out = model(input_dict, states, seq_lens)
                self._apply_noise(model)
                return out

    @override(Exploration)
    def get_exploration_action(self, distribution_inputs, action_dist_class,
                               model, timestep, explore=True):
        return self.sub_exploration.get_exploration_action(
            distribution_inputs, action_dist_class, model, timestep,
            explore=explore)
        #action_dist = action_dist_class(distribution_inputs, model)
        #_ = action_dist.sample()
        #deterministic_sample = action_dist.deterministic_sample()
        #return deterministic_sample, action_dist.sampled_action_logp()

    @override(Exploration)
    def on_episode_start(self, environment: BaseEnv, episode, model):
        # Build the add-noise-op.
        if self.framework == "tf":
            added_noises = []
            for noise in self.noise:
                added_noises.append(tf.assign(noise, tf.random_normal(
                    shape=noise.shape, stddev=self.stddev, dtype=tf.float32)))
            with tf.control_dependencies(added_noises):
                return self._apply_noise(model)
        # Add noise.
        else:
            for i in range(len(self.noise)):
                self.noise[i] = torch.normal(
                    0.0, self.stddev, size=self.noise[i].size)
            return self._apply_noise(model)

    #@override(Exploration)
    #def on_episode_end(self, environment: BaseEnv, episode: Episode,
    #                   model: ModelV2):
    #    if hasattr(model, "pi_distance"):
    #        episode.custom_metrics["policy_distance"] = model.pi_distance

    @override(Exploration)
    def postprocess_trajectory(self, policy, model, batch, tf_sess=None):
        # Adjust our stddev depending on the action (pi)-distance.
        # Also see [1] for details.
        inputs, dist_class, _ = policy.compute_distribution_inputs(
            model, batch, explore=False)
        # Categorical case (e.g. DQN).
        if dist_class is Categorical:
            action_dist = softmax(inputs)
        else:  # TODO(sven): Other action-dist cases.
            raise NotImplementedError

        inputs, dist_class, _ = policy.compute_distribution_inputs(
            model, batch, explore=False)
        # Categorical case (e.g. DQN).
        if dist_class is Categorical:
            noisy_action_dist = softmax(inputs)
            pi_distance = np.mean(entropy(action_dist.T, noisy_action_dist.T))
            current_epsilon = self.sub_exploration.get_info()["cur_epsilon"]
            if tf_sess is not None:
                current_epsilon = tf_sess.run(current_epsilon)
            delta = -np.log(
                1 - current_epsilon + current_epsilon / self.action_space.n)
            new_stddev = self.stddev * (1.01 if pi_distance < delta else 1 / 1.01)

        # Set new stddev to calculated value.
        if tf_sess is not None:
            tf_sess.run(tf.assign(self.stddev, new_stddev))
        else:
            self.stddev = new_stddev

        return batch

    def _tf_forward_op(self, explore, model, input_dict, states, seq_lens):

        # Exploration: Normal behavior.
        def true_fn():
            return model(input_dict, states, seq_lens)

        # No exploration: Revert noise, forward-pass, then re-apply noise.
        def false_fn():
            remove_op = self._remove_noise(model)
            with tf.control_dependencies([remove_op]):
                out = model(input_dict, states, seq_lens)
                with tf.control_dependencies([out]):
                    re_apply_op = self._apply_noise(model)
                    with tf.control_dependencies([re_apply_op]):
                        return out

        return tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=true_fn,
            false_fn=false_fn
        )

    def _apply_noise(self, model):
        # Build the apply-noise-op.
        if self.framework == "tf":
            add_noise_ops = list()
            for var, noise in zip(self.model_variables, self.noise):
                add_noise_ops.append(tf.assign_add(var, noise))
            return tf.group(*add_noise_ops)
        # Add noise.
        else:
            for i in range(len(self.noisy_weights)):
                # Add noise to weights in-place.
                torch.add_(self.model_variables[i], self.noise[i])

    def _remove_noise(self, model):
        """
        Removes the current action noise from the model parameters.

        Args:
            model (ModelV2): The Model object.
        """
        # Generate remove-noise op.
        if self.framework == "tf":
            remove_noise_ops = list()
            for var, noise in zip(self.model_variables, self.noise):
                remove_noise_ops.append(tf.assign_add(var, -noise))
            return tf.group(*tuple(remove_noise_ops))
        # Remove noise.
        else:
            for var, noise in zip(self.model_variables, self.noise):
                # Remove noise from weights in-place.
                torch.add_(var, -noise)
