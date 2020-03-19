from gym.spaces import Discrete
import numpy as np
from scipy.stats import entropy

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.utils.framework import get_variable
from ray.rllib.utils.from_config import from_config
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
                 policy_config,
                 *,
                 framework,
                 initial_stddev=1.0,
                 random_timesteps=10000,
                 sub_exploration=None,
                 **kwargs):
        """Initializes a ParameterNoise Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            policy_config (dict): The Policy's config dict.
            framework (str): One of None, "tf", "torch".
            initial_stddev (float): The initial stddev to use for the noise.
            random_timesteps (int): The number of timesteps to act completely
                randomly (see [1]).
            sub_exploration (Optional[dict]): Optional sub-exploration config.
                None for auto-detection/setup.
        """
        assert framework is not None
        super().__init__(action_space, framework=framework, **kwargs)

        self.stddev = get_variable(
            initial_stddev, framework=self.framework, tf_name="stddev")
        # Our noise to be added to the weights. Each item in `self.noise`
        # corresponds to one Model variable and holding the Gaussian noise to
        # be added to that variable (weight).
        self.noise = None
        self.add_noise_op = None
        self.remove_noise_op = None

        # The weight variables of the Model where noise should be applied to.
        # This excludes any variable, whose name contains "LayerNorm" (those
        # are BatchNormalization layers, which should not be perturbed).
        self.model_variables = None

        # Auto-detection of underlying exploration functionality.
        if sub_exploration is None:
            # For discrete action spaces, use an underlying EpsilonGreedy with
            # a special schedule.
            if isinstance(self.action_space, Discrete):
                sub_exploration = {
                    "type": "EpsilonGreedy",
                    "epsilon_schedule": {
                        "type": "PiecewiseSchedule",
                        "endpoints": [(0, 1.0), (random_timesteps, 0.01)],
                        "outside_value": 0.01
                    }
                }
            # TODO(sven): Implement for any action space.
            else:
                raise NotImplementedError

        self.sub_exploration = from_config(
            Exploration,
            sub_exploration,
            framework=self.framework,
            action_space=self.action_space,
            **kwargs)
        # Store the default setting for `explore`.
        self.default_explore = policy_config["explore"]

    @override(Exploration)
    def before_forward_pass(self,
                            model,
                            obs_batch,
                            *,
                            state_batches=None,
                            seq_lens=None,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            timestep=None,
                            explore=True):
        # Initialize all noise_vars.
        if self.noise is None:
            self.model_variables = [
                v for v in model.variables() if "LayerNorm" not in v.name
            ]
            self.noise = []
            for var in self.model_variables:
                self.noise.append(
                    get_variable(
                        np.zeros(var.shape, dtype=np.float32),
                        framework=self.framework,
                        tf_name=var.name.split(":")[0] + "_noisy"))

            self.add_noise_op = self._tf_add_noise_op(model)
            self.remove_noise_op = self._remove_noise(model)

        if self.framework == "tf" and not tf.executing_eagerly():
            return self._tf_before_forward_op(explore, model)
        # `explore` matches `self.default_explore` TODO(sven) ???:
        #  Revert noise, forward-pass, then re-apply noise.
        else:
            if not explore:
                self._remove_noise(model)

    @override(Exploration)
    def after_forward_pass(self,
                           *,
                           distribution_inputs,
                           action_dist_class,
                           model,
                           timestep=None,
                           explore=True):
        if self.framework == "tf" and not tf.executing_eagerly():
            return self._tf_after_forward_op(explore, model)
        else:
            # No exploration: Revert noise, forward-pass, then re-apply noise.
            if not explore:
                self._apply_noise(model)

    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs,
                               action_dist_class,
                               model,
                               timestep,
                               explore=True):
        return self.sub_exploration.get_exploration_action(
            distribution_inputs,
            action_dist_class,
            model,
            timestep,
            explore=explore)

    @override(Exploration)
    def on_episode_start(self,
                         policy,
                         model,
                         *,
                         environment=None,
                         episode=None,
                         tf_sess=None):
        # If True, add noise here and leave it (for the episode) unless a
        # compute_action call has `explore=False` (then we remove the noise,
        # act and re-add it).
        # If False, this logic is inversed.
        if self.default_explore is False:
            return

        # Build/execute the add-noise-op for tf.
        if self.framework == "tf":
            if tf.executing_eagerly():
                self._tf_add_noise_op(model)
            else:
                tf_sess.run(self.add_noise_op)

        # Add noise.
        else:
            for i in range(len(self.noise)):
                self.noise[i] = torch.normal(
                    0.0, self.stddev, size=self.noise[i].size)
            return self._apply_noise(model)

    @override(Exploration)
    def on_episode_end(self,
                       policy,
                       model,
                       *,
                       environment=None,
                       episode=None,
                       tf_sess=None):
        # If config["explore"]=False: No noise has been added at beginning of
        # episode -> no reason to remove it here.
        if self.default_explore is False:
            return

        # Build/execute the remove-noise-op for tf.
        if self.framework == "tf" and not tf.executing_eagerly():
            tf_sess.run(self.remove_noise_op)
        # Removes noise.
        else:
            return self._remove_noise(model)

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        # Adjust the stddev depending on the action (pi)-distance.
        # Also see [1] for details.
        inputs, dist_class, _ = policy.compute_distribution_inputs(
            obs_batch=sample_batch[SampleBatch.CUR_OBS],
            # TODO(sven): What about state-ins and seq-lens?
            prev_action_batch=sample_batch.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=sample_batch.get(SampleBatch.PREV_REWARDS),
            explore=False)
        # Categorical case (e.g. DQN).
        if dist_class is Categorical:
            action_dist = softmax(inputs)
        else:  # TODO(sven): Other action-dist cases.
            raise NotImplementedError

        inputs, dist_class, _ = policy.compute_distribution_inputs(
            obs_batch=sample_batch[SampleBatch.CUR_OBS],
            # TODO(sven): What about state-ins and seq-lens?
            prev_action_batch=sample_batch.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=sample_batch.get(SampleBatch.PREV_REWARDS),
            explore=True)

        new_stddev = 0.0
        # Categorical case (e.g. DQN).
        if dist_class is Categorical:
            noisy_action_dist = softmax(inputs)
            pi_distance = np.mean(entropy(action_dist.T, noisy_action_dist.T))
            current_epsilon = self.sub_exploration.get_info()["cur_epsilon"]
            if tf_sess is not None:
                current_epsilon = tf_sess.run(current_epsilon)
            delta = -np.log(1 - current_epsilon +
                            current_epsilon / self.action_space.n)
            if pi_distance > delta:
                new_stddev = self.stddev * 1.01
            else:
                new_stddev = self.stddev / 1.01

        # Set new stddev to calculated value.
        if tf_sess is not None:
            tf_sess.run(tf.assign(self.stddev, new_stddev))
        else:
            self.stddev = new_stddev

        return sample_batch

    def _tf_before_forward_op(self, explore, model):
        # explore: Do nothing if noise is in place, otherwise, add noise.
        def true_fn():
            return tf.no_op() if self.default_explore else \
                self._apply_noise(model)

        # No exploration: Remove noise (for upcoming forward-pass) if noise is
        # in place, otherwise, do nothing.
        def false_fn():
            return self._remove_noise(model) if self.default_explore else \
                tf.no_op()

        return tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=true_fn,
            false_fn=false_fn)

    def _tf_after_forward_op(self, explore, model):
        # explore: Do nothing if noise is in place, otherwise, remove noise.
        def true_fn():
            return tf.no_op() if self.default_explore else \
                self._remove_noise(model)

        # No exploration: Re-apply previously removed noise or do nothing
        # (if noise was never applied).
        def false_fn():
            return self._apply_noise(model) if self.default_explore else \
                tf.no_op()

        return tf.cond(
            tf.constant(explore) if isinstance(explore, bool) else explore,
            true_fn=true_fn,
            false_fn=false_fn)

    def _tf_add_noise_op(self, model):
        added_noises = []
        for noise in self.noise:
            added_noises.append(
                tf.assign(
                    noise,
                    tf.random_normal(
                        shape=noise.shape,
                        stddev=self.stddev,
                        dtype=tf.float32)))
        with tf.control_dependencies(added_noises):
            return self._apply_noise(model)

    def _apply_noise(self, model):
        """
        Adds the current action noise to the model parameters.

        Args:
            model (ModelV2): The Model object.
        """
        # Build the apply-noise-op.
        if self.framework == "tf":
            add_noise_ops = list()
            for var, noise in zip(self.model_variables, self.noise):
                add_noise_ops.append(tf.assign_add(var, noise))
            return tf.group(*add_noise_ops)
        # Add noise.
        else:
            for i in range(len(self.noise)):
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

    @override(Exploration)
    def get_info(self):
        return {"cur_stddev": self.stddev}
