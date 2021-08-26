import gym

from ray.rllib.utils.annotations import override


class ActionTransform(gym.ActionWrapper):
    def __init__(self, env, low, high):
        super().__init__(env)
        self._low = low
        self._high = high
        self.action_space = type(env.action_space)(self._low, self._high,
                                                   env.action_space.shape,
                                                   env.action_space.dtype)

    def action(self, action):
        return (action - self._low) / (self._high - self._low) * (
            self.env.action_space.high -
            self.env.action_space.low) + self.env.action_space.low


def transform_action_space(env_name_or_creator):
    """Wrapper for gym.Envs to have their action space transformed.

    Args:
        env_name_or_creator (Union[str, Callable[]]: String specifier or
            env_maker function.

    Returns:
        Type[TransformedActionSpaceEnv]: New TransformedActionSpaceEnv class
            to be used as env. The constructor takes a config dict with `_low`
            and `_high` keys specifying the new action range
            (default -1.0 to 1.0). The reset of the config dict will be
            passed on to the underlying env's constructor.

    Examples:
         >>> # By gym string:
         >>> pendulum_300_to_500_cls = transform_action_space("Pendulum-v0")
         >>> # Create a transformed pendulum env.
         >>> pendulum_300_to_500 = pendulum_300_to_500_cls({"_low": -15.0})
         >>> pendulum_300_to_500.action_space
         ... gym.spaces.Box(-15.0, 1.0, (1, ), "float32")
    """

    class TransformedActionSpaceEnv(gym.Env):
        """PendulumEnv w/ an action space of range 300.0 to 500.0."""

        def __init__(self, config):
            self._low = config.pop("low", -1.0)
            self._high = config.pop("high", 1.0)
            if isinstance(env_name_or_creator, str):
                self.env = gym.make(env_name_or_creator)
            else:
                self.env = env_name_or_creator(config)
            self.env = ActionTransform(self.env, self._low, self._high)
            self.observation_space = self.env.observation_space
            self.action_space = self.env.action_space

        @override(gym.Env)
        def reset(self):
            return self.env.reset()

        @override(gym.Env)
        def step(self, actions):
            return self.env.step(actions)

        @override(gym.Env)
        def render(self, mode=None):
            return self.env.render(mode)

    return TransformedActionSpaceEnv


TransformedActionPendulum = transform_action_space("Pendulum-v0")
