import gymnasium as gym
from typing import Type


class ActionTransform(gym.ActionWrapper):
    def __init__(self, env, low, high):
        super().__init__(env)
        self._low = low
        self._high = high
        self.action_space = type(env.action_space)(
            self._low, self._high, env.action_space.shape, env.action_space.dtype
        )

    def action(self, action):
        return (action - self._low) / (self._high - self._low) * (
            self.env.action_space.high - self.env.action_space.low
        ) + self.env.action_space.low


def transform_action_space(env_name_or_creator) -> Type[gym.Env]:
    """Wrapper for gym.Envs to have their action space transformed.

    Args:
        env_name_or_creator (Union[str, Callable[]]: String specifier or
            env_maker function.

    Returns:
        New transformed_action_space_env function that returns an environment
        wrapped by the ActionTransform wrapper. The constructor takes a
        config dict with `_low` and `_high` keys specifying the new action
        range (default -1.0 to 1.0). The reset of the config dict will be
        passed on to the underlying/wrapped env's constructor.

    Examples:
         >>> # By gym string:
         >>> pendulum_300_to_500_cls = transform_action_space("Pendulum-v1")
         >>> # Create a transformed pendulum env.
         >>> pendulum_300_to_500 = pendulum_300_to_500_cls({"_low": -15.0})
         >>> pendulum_300_to_500.action_space
         ... gym.spaces.Box(-15.0, 1.0, (1, ), "float32")
    """

    def transformed_action_space_env(config):
        if isinstance(env_name_or_creator, str):
            inner_env = gym.make(env_name_or_creator)
        else:
            inner_env = env_name_or_creator(config)
        _low = config.pop("low", -1.0)
        _high = config.pop("high", 1.0)
        env = ActionTransform(inner_env, _low, _high)
        return env

    return transformed_action_space_env


TransformedActionPendulum = transform_action_space("Pendulum-v1")
