import gym
from typing import Optional


def check_old_gym_env(env: Optional[gym.Env] = None, *, step_results=None, reset_results=None):
    # Check `reset()` results.
    if reset_results is not None:
        if (
            # Result is a tuple?
            not isinstance(reset_results, tuple)
            # Result is a tuple of len=2?
            or len(reset_results) != 2
            # Result is a tuple of len=2 and the second item is a dict (infos)?
            or (len(reset_results) == 2 and not isinstance(reset_results[1], dict))
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
            return True
    # Check `step()` results.
    elif step_results is not None:
        if len(step_results) == 4:
            return True
        elif len(step_results) == 5:
            return False
        else:
            raise ValueError(
                "The number of values returned from `gym.Env.step([action])` must be "
                "either 4 (old gym.Env API) or 5 (new gym.Env API including "
                "`truncated` flags)! Make sure your `step()` method returns: [obs], "
                "[reward], [done], ([truncated])?, and [infos]!"
            )

    else:
        raise AttributeError(
            "Either `step_results` or `reset_results` most be provided to "
            "`check_old_gym_env()`!"
        )
    return False
