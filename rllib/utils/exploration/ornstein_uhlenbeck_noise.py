import numpy as np

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.gaussian_noise import GaussianNoise
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable

tf = try_import_tf()
torch, _ = try_import_torch()


class OrnsteinUhlenbeckNoise(GaussianNoise):
    """An exploration that adds Ornstein-Uhlenbeck noise to continuous actions.

    If explore=True, returns sampled actions plus a noise term X,
    which changes according to this formula:
    Xt+1 = -theta*Xt + sigma*N[0,stddev], where theta, sigma and stddev are
    constants. Also, some completely random period is possible at the
    beginning.
    If explore=False, returns the deterministic action.
    """

    def __init__(self,
                 action_space,
                 *,
                 framework: str,
                 ou_theta=0.15,
                 ou_sigma=0.2,
                 ou_base_scale=0.1,
                 random_timesteps=1000,
                 initial_scale=1.0,
                 final_scale=0.02,
                 scale_timesteps=10000,
                 scale_schedule=None,
                 **kwargs):
        """Initializes an Ornstein-Uhlenbeck Exploration object.

        Args:
            action_space (Space): The gym action space used by the environment.
            ou_theta (float): The theta parameter of the Ornstein-Uhlenbeck
                process.
            ou_sigma (float): The sigma parameter of the Ornstein-Uhlenbeck
                process.
            ou_base_scale (float): A fixed scaling factor, by which all OU-
                noise is multiplied. NOTE: This is on top of the parent
                GaussianNoise's scaling.
            random_timesteps (int): The number of timesteps for which to act
                completely randomly. Only after this number of timesteps, the
                `self.scale` annealing process will start (see below).
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
        super().__init__(
            action_space,
            framework=framework,
            random_timesteps=random_timesteps,
            initial_scale=initial_scale,
            final_scale=final_scale,
            scale_timesteps=scale_timesteps,
            scale_schedule=scale_schedule,
            stddev=1.0,  # Force `self.stddev` to 1.0.
            **kwargs)
        self.ou_theta = ou_theta
        self.ou_sigma = ou_sigma
        self.ou_base_scale = ou_base_scale

        # The current OU-state value (gets updated each time, an eploration
        # action is computed).
        self.ou_state = get_variable(
            np.array(self.action_space.low.size * [.0], dtype=np.float32),
            framework=self.framework,
            tf_name="ou_state",
            torch_tensor=True,
            device=self.device)

    @override(GaussianNoise)
    def _get_tf_exploration_action_op(self, action_dist, explore, timestep):
        ts = timestep if timestep is not None else self.last_timestep
        scale = self.scale_schedule(ts)

        # The deterministic actions (if explore=False).
        deterministic_actions = action_dist.deterministic_sample()

        # Apply base-scaled and time-annealed scaled OU-noise to
        # deterministic actions.
        gaussian_sample = tf.random_normal(
            shape=[self.action_space.low.size], stddev=self.stddev)
        ou_new = self.ou_theta * -self.ou_state + \
            self.ou_sigma * gaussian_sample
        ou_state_new = tf.assign_add(self.ou_state, ou_new)
        high_m_low = self.action_space.high - self.action_space.low
        high_m_low = tf.where(
            tf.math.is_inf(high_m_low), tf.ones_like(high_m_low), high_m_low)
        noise = scale * self.ou_base_scale * ou_state_new * high_m_low
        stochastic_actions = tf.clip_by_value(
            deterministic_actions + noise,
            self.action_space.low * tf.ones_like(deterministic_actions),
            self.action_space.high * tf.ones_like(deterministic_actions))

        # Stochastic actions could either be: random OR action + noise.
        random_actions, _ = \
            self.random_exploration.get_tf_exploration_action_op(
                action_dist, explore)
        exploration_actions = tf.cond(
            pred=ts <= self.random_timesteps,
            true_fn=lambda: random_actions,
            false_fn=lambda: stochastic_actions)

        # Chose by `explore` (main exploration switch).
        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=lambda: exploration_actions,
            false_fn=lambda: deterministic_actions)
        # Logp=always zero.
        batch_size = tf.shape(deterministic_actions)[0]
        logp = tf.zeros(shape=(batch_size, ), dtype=tf.float32)

        # Increment `last_timestep` by 1 (or set to `timestep`).
        assign_op = \
            tf.assign_add(self.last_timestep, 1) if timestep is None else \
            tf.assign(self.last_timestep, timestep)
        with tf.control_dependencies([assign_op, ou_state_new]):
            return action, logp

    @override(GaussianNoise)
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
                        action_dist, explore=True)
            # Apply base-scaled and time-annealed scaled OU-noise to
            # deterministic actions.
            else:
                det_actions = action_dist.deterministic_sample()
                scale = self.scale_schedule(self.last_timestep)
                gaussian_sample = scale * torch.normal(
                    mean=torch.zeros(self.ou_state.size()), std=1.0) \
                    .to(self.device)
                ou_new = self.ou_theta * -self.ou_state + \
                    self.ou_sigma * gaussian_sample
                self.ou_state += ou_new
                high_m_low = torch.from_numpy(
                    self.action_space.high - self.action_space.low). \
                    to(self.device)
                high_m_low = torch.where(
                    torch.isinf(high_m_low),
                    torch.ones_like(high_m_low).to(self.device), high_m_low)
                noise = scale * self.ou_base_scale * self.ou_state * high_m_low
                action = torch.clamp(det_actions + noise,
                                     self.action_space.low[0],
                                     self.action_space.high[0])

        # No exploration -> Return deterministic actions.
        else:
            action = action_dist.deterministic_sample()

        # Logp=always zero.
        logp = torch.zeros(
            (action.size()[0], ), dtype=torch.float32, device=self.device)

        return action, logp
