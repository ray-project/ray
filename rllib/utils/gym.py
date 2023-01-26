import gymnasium as gym
from typing import Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def check_old_gym_env(
    env: Optional[gym.Env] = None, *, step_results=None, reset_results=None
):
    # Check `reset()` results.
    if reset_results is not None:
        if (
            # Result is NOT a tuple?
            not isinstance(reset_results, tuple)
            # Result is a tuple of len!=2?
            or len(reset_results) != 2
            # The second item is a NOT dict (infos)?
            or not isinstance(reset_results[1], dict)
            # Result is a tuple of len=2 and the second item is a dict (infos) and
            # our env does NOT have obs space 2-Tuple with the second space being a
            # dict?
            or (
                env
                and isinstance(env.observation_space, gym.spaces.Tuple)
                and len(env.observation_space.spaces) >= 2
                and isinstance(env.observation_space.spaces[1], gym.spaces.Dict)
            )
        ):
            raise ValueError
    # Check `step()` results.
    elif step_results is not None:
        if len(step_results) == 4:
            return ValueError
        elif len(step_results) == 5:
            return False
        else:
            raise ValueError(
                "The number of values returned from `gym.Env.step([action])` must be "
                "5 (new gym.Env API including `truncated` flags)! Make sure your "
                "`step()` method returns: [obs], [reward], [terminated], "
                "[truncated], and [infos]!"
            )

    else:
        raise AttributeError(
            "Either `step_results` or `reset_results` most be provided to "
            "`check_old_gym_env()`!"
        )
    return False


@DeveloperAPI
def convert_old_gym_space_to_gymnasium_space(space) -> gym.Space:
    """Converts an old gym (NOT gymnasium) Space into a gymnasium.Space.

    Args:
        space: The gym.Space to convert to gymnasium.Space.

    Returns:
         The converted gymnasium.space object.
    """
    from ray.rllib.utils.serialization import gym_space_from_dict, gym_space_to_dict

    return gym_space_from_dict(gym_space_to_dict(space))


@DeveloperAPI
def try_import_gymnasium_and_gym():
    try:
        import gymnasium as gym
    except (ImportError, ModuleNotFoundError):
        raise ImportError(
            "The `gymnasium` package seems to be not installed! As of Ray 2.2, it is "
            "required for RLlib. Try running `pip install gymnasium` from the "
            "command line to fix this problem."
        )

    old_gym = None
    try:
        import gym as old_gym
    except (ImportError, ModuleNotFoundError):
        pass

    return gym, old_gym
